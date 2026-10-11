// Package tracking is the 3.0 job registry and per-job log (spec §6): the tracking
// schema in the destination job_schema, labelled 3.0; archiver_job with the job binding
// and the root key's column names; the typed log archiver_job_log_<id>, one column per
// root key column; the log-key check and the binding check that run before recovery; and
// the log status writes, made inside the caller's transaction.
//
// It replaces the 2.x ResumeManager. internal/engine is its one caller.
package tracking

import "fmt"

// SchemaLabel is the one tracking-schema label this binary recognizes (spec §6.1). Every
// other label, 2.x included, is refused; there is no migration (R37).
const SchemaLabel = "3.0"

// Status is a root's state in the job's log, stored as TINYINT ga_status (spec §6.3).
// 2.x's pending (0) and failed (3) do not exist in 3.0, and their numbers are not reused.
type Status int8

const (
	// StatusCopied: archive, written in the destination transaction with the copied rows
	// and their verification. The source rows still exist.
	StatusCopied Status = 1
	// StatusCompleted: archive after the source deletes; copy-only in the destination
	// transaction; purge after the deletes.
	StatusCompleted Status = 2
	// StatusDeleting: purge, in its own transaction after the lag wait and before any
	// delete.
	StatusDeleting Status = 4
)

func (s Status) String() string {
	switch s {
	case StatusCopied:
		return "copied"
	case StatusCompleted:
		return "completed"
	case StatusDeleting:
		return "deleting"
	default:
		return fmt.Sprintf("Status(%d)", int8(s))
	}
}

func (s Status) valid() bool {
	return s == StatusCopied || s == StatusCompleted || s == StatusDeleting
}

// JobStatus is archiver_job.job_status. 2.x's never-written Paused (2) is gone (spec §6.2).
type JobStatus int8

const (
	JobIdle    JobStatus = 0
	JobRunning JobStatus = 1
	JobFailed  JobStatus = 3
)

func (s JobStatus) valid() bool { return s == JobIdle || s == JobRunning || s == JobFailed }

// SchemaError refuses a tracking schema this binary does not recognize. It is never
// retried and never repaired: the DBA follows the documented procedure (R37).
type SchemaError struct {
	Schema string
	Found  string // what the schema says about itself
	Remedy string // empty: the 2.x procedure
}

// remedy2x is R37's procedure for a schema written by GoArchive 2.x or earlier.
const remedy2x = "GoArchive 3.0 does not run against GoArchive 2.x tracking tables and never migrates them. " +
	"Finish every 2.x job with the 2.x binary, then drop the 2.x tracking tables " +
	"(archiver_job, goarchive_meta and every archiver_job_log_<id>) before the first 3.0 run. " +
	"See \"Moving from 2.2\" in docs/README_UPGRADING_3_0.md"

func (e *SchemaError) Error() string {
	remedy := e.Remedy
	if remedy == "" {
		remedy = remedy2x
	}
	return fmt.Sprintf("tracking tables in schema %q %s; this GoArchive requires tracking-schema label %q.\n%s",
		e.Schema, e.Found, SchemaLabel, remedy)
}
