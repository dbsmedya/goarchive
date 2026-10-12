package lock

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
)

// sessionWaitTimeout is the wait_timeout, in seconds, of every lock session: ten keep-alive
// intervals. A live process resets the session's idle timer every 30 s, so it never reaches
// it; after a lost host or network the server frees the locks within it (spec §4, probe L4).
// It is not a config option; a variable only so a test can shorten it (T7).
var sessionWaitTimeout = 300

// ErrLost reports that a set's locks are no longer all held by its session.
var ErrLost = errors.New("lock: the locks are no longer held")

// HeldError refuses an acquisition: another session holds Name. Holder is that session's
// connection id, or 0 when it released the lock between the refusal and the lookup.
type HeldError struct {
	Name   string
	Holder int64
}

func (e *HeldError) Error() string {
	if e.Holder == 0 {
		return fmt.Sprintf("lock: %s was held by another session a moment ago", e.Name)
	}
	return fmt.Sprintf("lock: %s is held by connection %d", e.Name, e.Holder)
}

// Set is a set of user-level locks held on one dedicated session for the length of a run
// (spec §4). The session runs nothing but lock statements. A Set is acquired once and never
// reused: user-level locks are recursive, so a reused session would get 1 for a lock it
// already holds (probe L5).
type Set struct {
	db    *sql.DB
	names []string // sorted, so the refusal is deterministic

	mu     sync.Mutex // held across every statement on conn and its scan (F1 M2)
	conn   *sql.Conn
	connID int64 // kept after a loss: the final-status check compares holders with it
	held   bool
	used   bool // Acquire was called

	keepAliveCancel context.CancelFunc
	keepAliveDone   chan struct{}
}

// NewSet returns an unacquired set of names on db's server.
func NewSet(db *sql.DB, names ...string) *Set {
	sorted := slices.Clone(names)
	slices.Sort(sorted)
	return &Set{db: db, names: slices.Compact(sorted)}
}

// Names returns the set's lock names, sorted.
func (s *Set) Names() []string { return slices.Clone(s.names) }

// ConnectionID is the connection id of the session that acquired the set, or 0 before an
// acquisition. It stays set after the locks are lost or released.
func (s *Set) ConnectionID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connID
}

// Acquire takes a dedicated session, sets its wait_timeout and takes every lock with
// timeout 0: it never queues. When another session holds a name it releases what it took,
// discards the session and returns *HeldError.
func (s *Set) Acquire(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.used {
		return errors.New("lock: a lock set is acquired once; build a new one")
	}
	s.used = true
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("lock: dedicated session: %w", err)
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION wait_timeout = %d", sessionWaitTimeout)); err != nil {
		discard(conn)
		return fmt.Errorf("lock: set the lock session's wait_timeout: %w", err)
	}
	var id int64
	if err := conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&id); err != nil {
		discard(conn)
		return fmt.Errorf("lock: read the lock session's connection id: %w", err)
	}
	for _, name := range s.names {
		var got sql.NullInt64
		if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", name).Scan(&got); err != nil {
			discard(conn)
			return fmt.Errorf("lock: GET_LOCK(%s): %w", name, err)
		}
		switch {
		case !got.Valid:
			discard(conn)
			return fmt.Errorf("lock: GET_LOCK(%s) returned NULL", name)
		case got.Int64 == 1:
			continue
		}
		var holder sql.NullInt64
		_ = conn.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?)", name).Scan(&holder)
		_, _ = conn.ExecContext(ctx, "DO RELEASE_ALL_LOCKS()")
		discard(conn)
		return &HeldError{Name: name, Holder: holder.Int64}
	}
	s.conn, s.connID, s.held = conn, id, true
	return nil
}

// checkSQL is true only when the session holds every name. <=> is required: IS_USED_LOCK
// returns NULL for a free lock, and NULL = CONNECTION_ID() is NULL, not false (probe L5).
func checkSQL(n int) string {
	return "SELECT " + strings.TrimSuffix(strings.Repeat("(IS_USED_LOCK(?) <=> CONNECTION_ID()) AND ", n), " AND ")
}

// Check verifies that the set's session still holds every lock, on that session. A failed
// check, or a failed statement, marks the set lost and discards the session; it returns an
// error wrapping ErrLost. A cancelled ctx returns ctx's error and changes nothing.
func (s *Set) Check(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.held || s.conn == nil {
		return ErrLost
	}
	args := make([]any, len(s.names))
	for i, n := range s.names {
		args[i] = n
	}
	var ok sql.NullInt64
	if err := s.conn.QueryRowContext(ctx, checkSQL(len(s.names)), args...).Scan(&ok); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.lostLocked()
		return fmt.Errorf("%w: %v", ErrLost, err)
	}
	if !ok.Valid || ok.Int64 != 1 {
		s.lostLocked()
		return ErrLost
	}
	return nil
}

func (s *Set) lostLocked() {
	s.held = false
	if s.conn != nil {
		discard(s.conn)
		s.conn = nil
	}
}

// StartKeepAlive runs Check every interval until StopKeepAlive or ctx ends. The first
// failure is sent on the returned channel, and the keep-alive stops.
func (s *Set) StartKeepAlive(ctx context.Context, interval time.Duration) <-chan error {
	lost := make(chan error, 1)
	s.mu.Lock()
	if s.keepAliveCancel != nil {
		s.mu.Unlock()
		return lost
	}
	kctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	s.keepAliveCancel, s.keepAliveDone = cancel, done
	s.mu.Unlock()
	go func() {
		defer close(done)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-kctx.Done():
				return
			case <-t.C:
				if err := s.Check(kctx); err != nil {
					if kctx.Err() != nil {
						return
					}
					lost <- err
					return
				}
			}
		}
	}()
	return lost
}

// StopKeepAlive stops the keep-alive and waits for it.
func (s *Set) StopKeepAlive() {
	s.mu.Lock()
	cancel, done := s.keepAliveCancel, s.keepAliveDone
	s.keepAliveCancel, s.keepAliveDone = nil, nil
	s.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
}

// Release stops the keep-alive, releases every lock and always discards the session, so a
// session with the short wait_timeout never returns to the pool (spec §4 step 5). A failed
// RELEASE_ALL_LOCKS is reported; the discard frees the locks anyway (issue #79).
func (s *Set) Release(ctx context.Context) error {
	s.StopKeepAlive()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		s.held = false
		return nil
	}
	_, err := s.conn.ExecContext(ctx, "DO RELEASE_ALL_LOCKS()")
	discard(s.conn)
	s.conn, s.held = nil, false
	if err != nil {
		return fmt.Errorf("lock: RELEASE_ALL_LOCKS: %w (the session was discarded, which frees them)", err)
	}
	return nil
}

// Holder returns the connection id holding name on db's server, or 0 when it is free.
func Holder(ctx context.Context, db *sql.DB, name string) (int64, error) {
	var id sql.NullInt64
	if err := db.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?)", name).Scan(&id); err != nil {
		return 0, fmt.Errorf("lock: IS_USED_LOCK(%s): %w", name, err)
	}
	return id.Int64, nil
}

// discard closes conn's MySQL session instead of returning it to the pool; MySQL frees
// every user-level lock when a session ends.
func discard(conn *sql.Conn) {
	_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	_ = conn.Close()
}
