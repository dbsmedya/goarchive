package tracking

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
)

// maxJobName is the width of archiver_job.job_name, VARCHAR(255).
const maxJobName = 255

// JobInput is what a run knows about its job before it touches the log.
type JobInput struct {
	Name    string
	Type    jobspec.Type
	RootKey key.Def // the root table's key as MySQL reports it now (preflight's Result.Keys)
	Binding Binding
}

// Job is one registered job with its typed log.
type Job struct {
	ID   int64
	Name string
	Type jobspec.Type

	store     *Store
	log       key.Def // k1…kN
	logTable  string  // quoted, qualified
	maxTuples int     // key-addressed statements on the log (spec §4.3)
	insertMax int     // rows per INSERT
}

// Attach registers the job, or finds it, and runs the startup bookkeeping checks in the
// order spec §6.4 requires, before anything is recovered or run:
//
//  1. the job's type and root table are fixed for its life (2.x issue #14's schema-free
//     half), and its name may not differ from the stored one only in letter case;
//  2. the log-key check (§6.2): root_key_columns against in.RootKey's names, then the
//     log's k1…kN facts against in.RootKey's columns. A job whose registration did not
//     complete has no names yet, and completes it instead (register);
//  3. the binding check (§6.2, §6.4): with pending work, a binding that differs in a
//     compared part is refused with *BindingError; with none, it is overwritten.
//
// These are bookkeeping integrity, not preflight policy: no flag skips them. The caller
// holds the job's advisory lock.
func (s *Store) Attach(ctx context.Context, in JobInput) (*Job, error) {
	if err := in.validate(); err != nil {
		return nil, err
	}
	row, err := s.readJob(ctx, in.Name)
	if errors.Is(err, sql.ErrNoRows) {
		return s.create(ctx, in)
	}
	if err != nil {
		return nil, err
	}
	switch {
	case row.name != in.Name:
		return nil, fmt.Errorf("tracking: job %q differs only in letter case from the existing job %q in schema %q; "+
			"job names are unique without regard to case (R37). Use the existing name or another one", in.Name, row.name, s.schema)
	case row.jobType != string(in.Type):
		return nil, fmt.Errorf("tracking: job %q exists with job_type %s, and this config says %s; a job's type is fixed. "+
			"Use a new job name", in.Name, row.jobType, in.Type)
	case row.rootTable != in.RootKey.Table:
		return nil, fmt.Errorf("tracking: job %q exists for root table %q, and this config says %q: its log must never be "+
			"replayed against another table. Retire the job (docs/README_JOBS_SCHEMA.md, \"Retiring a job completely\") "+
			"or use a new job name", in.Name, row.rootTable, in.RootKey.Table)
	}
	j := s.newJob(row.id, in)
	if row.keyColumns == unregistered {
		if err := s.register(ctx, j, in.RootKey); err != nil {
			return nil, err
		}
	} else if err := s.checkLog(ctx, j, row, in.RootKey); err != nil {
		return nil, err
	}
	if err := s.bind(ctx, j, row, in.Binding); err != nil {
		return nil, err
	}
	return j, nil
}

func (in JobInput) validate() error {
	switch {
	case in.Name == "":
		return fmt.Errorf("tracking: a job needs a name")
	case strings.IndexFunc(in.Name, func(r rune) bool { return r <= ' ' || r > '~' }) >= 0:
		// job_name is an ascii column; jobspec already refuses such a name (R37).
		return fmt.Errorf("tracking: job name %q must be printable ASCII without spaces", in.Name)
	case len(in.Name) > maxJobName:
		// job_name is VARCHAR(255); jobspec refuses a longer name at load (M4-F4).
		return fmt.Errorf("tracking: job name %q is longer than %d characters", in.Name, maxJobName)
	case in.Type != jobspec.Archive && in.Type != jobspec.Purge && in.Type != jobspec.CopyOnly:
		return fmt.Errorf("tracking: job %q has job_type %q", in.Name, in.Type)
	case len(in.RootKey.Columns) == 0 || in.RootKey.Table == "":
		return fmt.Errorf("tracking: job %q has no root key", in.Name)
	case in.Binding.SourceServerUUID == "" || in.Binding.SourceDatabase == "" || in.Binding.DestinationDatabase == "":
		return fmt.Errorf("tracking: job %q has an incomplete binding %+v", in.Name, in.Binding)
	}
	return nil
}

type jobRow struct {
	id                                   int64
	name, rootTable, jobType, keyColumns string
	binding                              Binding
	fingerprint                          string
}

func (s *Store) readJob(ctx context.Context, name string) (jobRow, error) {
	var r jobRow
	err := s.db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT id, job_name, root_table, job_type, source_server_uuid, source_database, destination_database, "+
			"config_fingerprint, root_key_columns FROM %s WHERE job_name = ?", s.jobTable), name).
		Scan(&r.id, &r.name, &r.rootTable, &r.jobType, &r.binding.SourceServerUUID, &r.binding.SourceDatabase,
			&r.binding.DestinationDatabase, &r.fingerprint, &r.keyColumns)
	if errors.Is(err, sql.ErrNoRows) {
		return r, err
	}
	if err != nil {
		return r, fmt.Errorf("tracking: read job %q: %w", name, err)
	}
	return r, nil
}

func (s *Store) newJob(id int64, in JobInput) *Job {
	name, qualified := s.logTable(id)
	ld := logDef(name, in.RootKey)
	// A membership statement also binds one status (Logged) or two (Transition), and every
	// statement shares the 65,535-placeholder limit.
	maxTuples := min(key.MaxTuples(ld.Columns, s.budget), (65535-2)/len(ld.Columns))
	// An INSERT is not a range lookup, but it shares the 65,535-placeholder limit.
	insertMax := min(maxTuples, 65535/(len(ld.Columns)+1))
	return &Job{ID: id, Name: in.Name, Type: in.Type, store: s, log: ld, logTable: qualified,
		maxTuples: maxTuples, insertMax: insertMax}
}

// checkLog is the log-key check of a registered job.
func (s *Store) checkLog(ctx context.Context, j *Job, row jobRow, root key.Def) error {
	stored, err := decodeKeyColumns(row.keyColumns)
	if err != nil {
		return err
	}
	if err := checkKeyNames(j.Name, stored, root); err != nil {
		return err
	}
	// A registered job's log exists unless a DBA dropped it; it is then recreated empty.
	if err := s.createLog(ctx, j, root); err != nil {
		return err
	}
	spec, err := s.logSpec(ctx, j)
	if err != nil {
		return err
	}
	if err := checkLogColumns(j.Name, spec, root); err != nil {
		return err
	}
	if reason := checkLogTable(spec, len(root.Columns)); reason != "" {
		return fmt.Errorf("tracking: job %q: its log %s is not the table GoArchive created: %s. Restore it, or retire the job "+
			"(docs/README_JOBS_SCHEMA.md, \"Retiring a job completely\")", j.Name, j.logTable, reason)
	}
	return nil
}

func (s *Store) logSpec(ctx context.Context, j *Job) (validations.TableSpec, error) {
	name, _ := s.logTable(j.ID)
	spec, err := s.facts.TableSpec(ctx, name)
	if err != nil {
		return spec, fmt.Errorf("tracking: read the log %s.%s: %w", s.schema, name, err)
	}
	return spec, nil
}

// unregistered is root_key_columns of a job whose registration has not completed. The
// row comes first, because the log is named by its id, and the key's names are written
// only once the log is known to be the job's own. GoArchive holds no DELETE on the
// tracking schema (docs/README_PERMISSIONS.md), so a failed or interrupted registration
// cannot remove its row: this marker keeps every later run from taking it as registered.
const unregistered = ""

// create registers a new job: its row, marked unregistered, then its log (register).
func (s *Store) create(ctx context.Context, in JobInput) (*Job, error) {
	res, err := s.db.ExecContext(ctx, fmt.Sprintf(
		"INSERT INTO %s (job_name, root_table, job_type, job_status, source_server_uuid, source_database, "+
			"destination_database, config_fingerprint, root_key_columns) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", s.jobTable),
		in.Name, in.RootKey.Table, string(in.Type), JobIdle, in.Binding.SourceServerUUID, in.Binding.SourceDatabase,
		in.Binding.DestinationDatabase, in.Binding.Fingerprint.Text(), unregistered)
	if err != nil {
		return nil, fmt.Errorf("tracking: register job %q: %w", in.Name, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("tracking: read the id of job %q: %w", in.Name, err)
	}
	j := s.newJob(id, in)
	if err := s.register(ctx, j, in.RootKey); err != nil {
		return nil, err
	}
	return j, nil
}

// register completes the registration of job j: it creates the job's log, or finds the
// empty one of exactly the log's shape (checkLogTable, checkLogColumns) that an
// interrupted registration left, and only
// then writes the root key's names. A log that holds rows, or has another shape, was left
// by a dropped or truncated registry and is never adopted, on this run or any later one.
func (s *Store) register(ctx context.Context, j *Job, root key.Def) error {
	if err := s.createLog(ctx, j, root); err != nil {
		return err
	}
	spec, err := s.logSpec(ctx, j)
	if err != nil {
		return err
	}
	var lk *LogKeyError
	if err := checkLogColumns(j.Name, spec, root); errors.As(err, &lk) {
		return leftoverLog(j, lk.Reason)
	} else if err != nil {
		return err
	}
	if reason := checkLogTable(spec, len(root.Columns)); reason != "" {
		return leftoverLog(j, reason)
	}
	var one int
	switch err := s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT 1 FROM %s LIMIT 1", j.logTable)).Scan(&one); {
	case err == nil:
		return leftoverLog(j, "it holds rows")
	case !errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("tracking: read the log of job %q: %w", j.Name, err)
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET root_key_columns = ? WHERE id = ?", s.jobTable),
		encodeKeyColumns(root.Names()), j.ID); err != nil {
		return fmt.Errorf("tracking: register job %q: %w", j.Name, err)
	}
	return nil
}

func leftoverLog(j *Job, reason string) error {
	return fmt.Errorf("tracking: job %q cannot be registered: a table %s already exists under its log's name, and %s. "+
		"A dropped or truncated registry left it, and GoArchive never adopts it: drop it after confirming no job uses it, "+
		"or restore archiver_job. The job stays unregistered until then", j.Name, j.logTable, reason)
}

func (s *Store) createLog(ctx context.Context, j *Job, root key.Def) error {
	ddl, err := createLogSQL(j.logTable, root)
	if err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("tracking: create the log of job %q (%s): %w", j.Name, j.logTable, err)
	}
	return nil
}

// bind is the binding check: refused while pending work depends on a changed part,
// otherwise overwritten (spec §6.2, §6.4, R25).
func (s *Store) bind(ctx context.Context, j *Job, row jobRow, current Binding) error {
	fp, err := parseFingerprint(row.fingerprint)
	if err != nil {
		return err
	}
	stored := row.binding
	stored.Fingerprint = fp
	if sameBinding(stored, current) {
		return nil
	}
	if changed := changedParts(j.Type, stored, current); len(changed) > 0 {
		statuses := pendingStatuses(j.Type)
		n, err := j.count(ctx, statuses)
		if err != nil {
			return err
		}
		if n > 0 {
			return &BindingError{Job: j.Name, Type: j.Type, Pending: n, Status: statuses, Changed: changed}
		}
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(
		"UPDATE %s SET source_server_uuid = ?, source_database = ?, destination_database = ?, config_fingerprint = ? WHERE id = ?",
		s.jobTable), current.SourceServerUUID, current.SourceDatabase, current.DestinationDatabase,
		current.Fingerprint.Text(), j.ID); err != nil {
		return fmt.Errorf("tracking: rebind job %q: %w", j.Name, err)
	}
	return nil
}

// SetStatus writes archiver_job.job_status.
func (j *Job) SetStatus(ctx context.Context, st JobStatus) error {
	if !st.valid() {
		return fmt.Errorf("tracking: job status %d", st)
	}
	if _, err := j.store.db.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET job_status = ? WHERE id = ?", j.store.jobTable), st, j.ID); err != nil {
		return fmt.Errorf("tracking: set the status of job %q: %w", j.Name, err)
	}
	return nil
}

// Heartbeat stamps the job with the server's UTC wall clock. last_heartbeat_at is a
// DATETIME, which carries no zone, so it is written and read with UTC_TIMESTAMP() and
// never NOW() (2.x issue #16).
func (j *Job) Heartbeat(ctx context.Context) error {
	if _, err := j.store.db.ExecContext(ctx, fmt.Sprintf(
		"UPDATE %s SET last_heartbeat_at = UTC_TIMESTAMP() WHERE id = ?", j.store.jobTable), j.ID); err != nil {
		return fmt.Errorf("tracking: heartbeat of job %q: %w", j.Name, err)
	}
	return nil
}

// HeartbeatAge returns how long ago job name last beat. ok is false when the job does not
// exist or has never beaten. The stale-lock takeover (--force) reads it before Attach.
func (s *Store) HeartbeatAge(ctx context.Context, name string) (age time.Duration, ok bool, err error) {
	var seconds sql.NullInt64
	err = s.db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT TIMESTAMPDIFF(SECOND, last_heartbeat_at, UTC_TIMESTAMP()) FROM %s WHERE job_name = ?", s.jobTable), name).Scan(&seconds)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("tracking: read the heartbeat of job %q: %w", name, err)
	case !seconds.Valid:
		return 0, false, nil
	}
	return time.Duration(seconds.Int64) * time.Second, true, nil
}

// RunningJob is a job that archiver_job marks running.
type RunningJob struct {
	Name         string
	HeartbeatAge time.Duration
	Beat         bool // false: the job has never beaten
}

// Running returns the jobs other than except that archiver_job marks running on rootTable,
// with their heartbeat ages, for the engine's same-root guard (kept from 2.x, spec §9).
// Two startups on one root table cannot both pass the guard only if the caller holds the
// root table's advisory lock from before this call until its own job is marked running, as
// the engine does.
func (s *Store) Running(ctx context.Context, rootTable, except string) ([]RunningJob, error) {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(
		"SELECT job_name, TIMESTAMPDIFF(SECOND, last_heartbeat_at, UTC_TIMESTAMP()) FROM %s "+
			"WHERE root_table = ? AND job_status = ? AND job_name <> ? ORDER BY job_name", s.jobTable),
		rootTable, JobRunning, except)
	if err != nil {
		return nil, fmt.Errorf("tracking: read the jobs running on %q: %w", rootTable, err)
	}
	defer func() { _ = rows.Close() }()
	var out []RunningJob
	for rows.Next() {
		var r RunningJob
		var seconds sql.NullInt64
		if err := rows.Scan(&r.Name, &seconds); err != nil {
			return nil, fmt.Errorf("tracking: read the jobs running on %q: %w", rootTable, err)
		}
		r.Beat = seconds.Valid
		r.HeartbeatAge = time.Duration(seconds.Int64) * time.Second
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("tracking: read the jobs running on %q: %w", rootTable, err)
	}
	return out, nil
}
