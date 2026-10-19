package lock

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// releaseTimeout bounds a Wait lock's release, which the caller's cancellation cannot end.
const releaseTimeout = 5 * time.Second

// Wait takes the user-level lock name on a session of its own from db, waiting up to wait,
// in whole seconds, for another session to release it (TRK-1). It returns release, which
// frees the lock and discards the session; or *HeldError, naming the holder, when the wait
// runs out. Every failure discards the session too, so no pooled session can hold the lock.
// Unlike a Set's locks, which never queue, this one waits: the session's max_execution_time
// is cleared first, because the wait is a SELECT and a server's statement timer would end it
// with error 3024.
func Wait(ctx context.Context, db *sql.DB, name string, wait time.Duration) (release func(), err error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("lock: dedicated session: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "SET SESSION max_execution_time = 0"); err != nil {
		discard(conn)
		return nil, fmt.Errorf("lock: clear the lock session's max_execution_time: %w", err)
	}
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", name, int64(wait/time.Second)).Scan(&got); err != nil {
		discard(conn)
		return nil, fmt.Errorf("lock: GET_LOCK(%s): %w", name, err)
	}
	switch {
	case !got.Valid:
		discard(conn)
		return nil, fmt.Errorf("lock: GET_LOCK(%s) returned NULL", name)
	case got.Int64 != 1:
		var holder sql.NullInt64
		_ = conn.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?)", name).Scan(&holder)
		discard(conn)
		return nil, &HeldError{Name: name, Holder: holder.Int64}
	}
	return func() {
		// A second signal may have ended ctx; the release still runs, and the discard frees
		// the lock whatever it returns.
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
		defer cancel()
		_, _ = conn.ExecContext(rctx, "DO RELEASE_LOCK(?)", name)
		discard(conn)
	}, nil
}
