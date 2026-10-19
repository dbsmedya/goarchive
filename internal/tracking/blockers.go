package tracking

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/dbsmedya/dbsgomysql/pkg/sqlutil"
	"github.com/go-sql-driver/mysql"
)

// Blocker is a job whose unfinished work, or running status, overlaps an incoming run's
// tables on the same source data (unfinished-work guard spec §3).
type Blocker struct {
	ID               int64 // its registry id: the remedy names the row by it, since a name may hold ' or \
	Name             string
	Status           JobStatus
	Registered       bool // false: its registration did not complete, so it has no log yet
	Beat             bool // false: the job has never beaten
	HeartbeatAge     time.Duration
	Pending          bool     // its log holds copied or deleting roots
	Shared           []string // the overlapping tables, folded and sorted; nil when Unreadable
	Unreadable       error    // its config_fingerprint could not be read (spec §4)
	SourceServerUUID string   // as stored
}

// SameData reports whether a stored source_server_uuid is on the incoming run's source
// data (spec §3a). The engine builds it from the source's facts.
type SameData func(storedUUID string) bool

// LogUnreadableError refuses a run while a registered job's log, which the guard must read,
// cannot be read (spec §5). Missing is true for error 1146: a DBA dropped the table.
type LogUnreadableError struct {
	Job, Log string
	Missing  bool
	Err      error
}

func (e *LogUnreadableError) Error() string {
	if e.Missing {
		return fmt.Sprintf("tracking: job %q is registered, but its log %s is missing, so its unfinished work cannot be ruled out. "+
			"Rerun %q, which recreates its log empty, or finish retiring it (docs/README_JOBS_SCHEMA.md, \"Retiring a job completely\")",
			e.Job, e.Log, e.Job)
	}
	return fmt.Sprintf("tracking: read the log %s of job %q: %v", e.Log, e.Job, e.Err)
}

func (e *LogUnreadableError) Unwrap() error { return e.Err }

const mysqlNoSuchTable = 1146

// Blockers returns the jobs other than except, recorded on sourceDatabase (case-folded) and
// on the same source data by same, that share a table with tables and are marked running or
// hold copied or deleting roots in their logs, whatever their status (spec §3). A job whose
// fingerprint cannot be read is returned, with Unreadable set, when it is running or holds
// such roots: its overlap cannot be ruled out (spec §4). The registry is read once; the log
// only of a registered job that overlaps or cannot be read. Any read error is returned, never
// a partial list. The caller must hold the source table locks of every table in tables from
// before this call to the end of its run, as the engine does: no overlapping run can then
// change a row between this read and the caller's start (spec §6).
func (s *Store) Blockers(ctx context.Context, sourceDatabase string, same SameData, tables []string, except string) ([]Blocker, error) {
	want := map[string]bool{}
	for _, t := range tables {
		want[strings.ToLower(t)] = true
	}
	type candidate struct {
		b  Blocker
		id int64
	}
	var candidates []candidate
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(
		"SELECT id, job_name, root_table, job_status, source_server_uuid, root_key_columns, config_fingerprint, "+
			"TIMESTAMPDIFF(SECOND, last_heartbeat_at, UTC_TIMESTAMP()) FROM %s "+
			"WHERE LOWER(source_database) = LOWER(?) AND job_name <> ? ORDER BY job_name", s.jobTable),
		sourceDatabase, except)
	if err != nil {
		return nil, fmt.Errorf("tracking: read the jobs on source database %q: %w", sourceDatabase, err)
	}
	for rows.Next() {
		var (
			c           candidate
			root        string
			status      int64
			keyColumns  string
			fingerprint string
			seconds     sql.NullInt64
		)
		if err := rows.Scan(&c.id, &c.b.Name, &root, &status, &c.b.SourceServerUUID, &keyColumns, &fingerprint, &seconds); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("tracking: read the jobs on source database %q: %w", sourceDatabase, err)
		}
		if !same(c.b.SourceServerUUID) {
			continue
		}
		c.b.ID = c.id
		c.b.Status = JobStatus(status)
		c.b.Beat = seconds.Valid
		c.b.HeartbeatAge = time.Duration(seconds.Int64) * time.Second
		c.b.Registered = keyColumns != unregistered
		set, err := storedTables(root, fingerprint)
		if err != nil {
			c.b.Unreadable = err
		} else {
			for _, t := range set {
				if want[t] {
					c.b.Shared = append(c.b.Shared, t)
				}
			}
			if len(c.b.Shared) == 0 {
				continue
			}
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("tracking: read the jobs on source database %q: %w", sourceDatabase, err)
	}
	_ = rows.Close()

	var out []Blocker
	for _, c := range candidates {
		if c.b.Registered {
			pending, err := s.pending(ctx, c.id)
			if err != nil {
				_, qualified := s.logTable(c.id)
				var me *mysql.MySQLError
				return nil, &LogUnreadableError{Job: c.b.Name, Log: qualified,
					Missing: errors.As(err, &me) && me.Number == mysqlNoSuchTable, Err: err}
			}
			c.b.Pending = pending
		}
		if c.b.Status == JobRunning || c.b.Pending {
			out = append(out, c.b)
		}
	}
	return out, nil
}

// pending reports whether job id's log holds a copied or deleting root (idx_ga_status).
func (s *Store) pending(ctx context.Context, id int64) (bool, error) {
	_, qualified := s.logTable(id)
	var one int
	err := s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT 1 FROM %s WHERE ga_status IN (?, ?) LIMIT 1", qualified),
		int8(StatusCopied), int8(StatusDeleting)).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// storedTables is a stored job's table set (spec §4): its root and both ends of every
// relation, folded as the lock names fold (lock.hashed), de-duplicated and sorted. The
// fingerprint is parsed strictly, and every name must be a simple identifier; the edges need
// not form a tree, since the union does not depend on shape.
func storedTables(root, fingerprint string) ([]string, error) {
	f, err := parseFingerprint(fingerprint)
	if err != nil {
		return nil, err
	}
	names := []string{root}
	for _, e := range f.Relations {
		names = append(names, e.Parent, e.Table)
	}
	set := make([]string, 0, len(names))
	for _, n := range names {
		if !sqlutil.IsSimpleIdentifier(n) {
			return nil, fmt.Errorf("tracking: config_fingerprint names the table %q, which is not a simple identifier", n)
		}
		set = append(set, strings.ToLower(n))
	}
	slices.Sort(set)
	return slices.Compact(set), nil
}
