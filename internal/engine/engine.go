// Package engine is GoArchive 3.0's run engine (spec §7): one run loop for the three job
// types, which differ only in the phases of a batch.
//
//	archive:   fetch → discover → [destination tx: copy → verify → log copied] → delete → log completed
//	copy-only: fetch → (resume: drop logged roots) → discover → [destination tx: copy → verify → log completed]
//	purge:     fetch → discover → log deleting → delete → log completed
//
// Startup runs once for every type, in the order spec §7.1 and §6.4 require: preflight,
// the tracking schema, the advisory locks, the job's registry row with its log-key and
// binding checks, then recovery. Keys are tuples throughout (internal/key), and every
// key-addressed statement is checked against the keys it was given (spec §7.1a).
//
// The engine takes open connection pools; the commands (M6) load the configuration, open
// the pools with internal/database and handle signals.
package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/database"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
)

// LagGate is the replication lag gate (internal/replication's *Gate). It applies to archive
// and purge: the engine waits on it before every source delete chunk, in the batch loop
// and in recovery (spec §6.4, §7.1).
type LagGate interface {
	WaitForLag(ctx context.Context) error
}

// Config is one run of one job.
type Config struct {
	// Name and Spec are the job, as jobspec.Document.Job returned it for the subcommand.
	Name string
	Spec *jobspec.Spec

	// Source and Destination are the pools internal/database opened, every session pinned
	// to UTC (spec §4.5). Each pool's DATABASE() is the connection's database.
	Source, Destination *sql.DB
	// JobSchema is destination.job_schema, which holds the tracking tables (spec §6.1).
	JobSchema string

	// Processing is the job's effective processing block (Spec.EffectiveProcessing).
	Processing config.ProcessingConfig
	// Verification is the job's effective verification, after any --verification.
	Verification jobspec.Verification
	// DisableForeignKeyChecks is safety.disable_foreign_key_checks: the destination copy
	// runs with FOREIGN_KEY_CHECKS=0.
	DisableForeignKeyChecks bool

	// ForceTriggers and SkipPolicy are preflight's (--force-triggers,
	// --skip-validate-preflight). Neither skips the log-key or binding check (spec §6.4).
	ForceTriggers, SkipPolicy bool

	// Lag is the replication lag gate; nil means none.
	Lag LagGate
	// Stop closes on the first signal: the run stops at the next batch boundary.
	Stop <-chan struct{}
	// Logger defaults to logger.NewDefault().
	Logger *logger.Logger
	// Progress is --progress's interval; zero disables it (R50). Out receives the progress
	// lines and defaults to os.Stdout.
	Progress time.Duration
	Out      io.Writer

	// Test seams, nil in production. fault may rewrite a key-addressed statement before it
	// runs (spec §10, key-set tests); at is called at each crash point.
	fault func(*statement)
	at    func(point)
	// preflighted may change preflight's result, for the statement-cap tests.
	preflighted func(*preflight.Result)
}

// point is a place in a batch where a crash leaves distinct durable state (spec §10).
type point string

const (
	atCommitted point = "committed" // the destination transaction committed
	atLogged    point = "logged"    // purge: deleting committed, before any delete
	atDeleted   point = "deleted"   // one source delete chunk committed
	atMarked    point = "marked"    // startup: the job is marked running, before its first heartbeat
	atRunning   point = "running"   // startup: the job is marked running and has beaten once
)

// Report is what a run did.
type Report struct {
	Batches   int   // batches processed, including copy-only batches whose roots were all skipped
	Roots     int64 // roots archived, purged or copied
	Skipped   int64 // copy-only resume: roots this job had already copied
	Recovered int64 // logged roots that recovery finished
	Copied    map[string]int64
	Deleted   map[string]int64
	// Conversions are the conversion warnings accepted under verification: none, by table
	// and code. They are counted only for committed batches.
	Conversions map[string]map[uint16]uint64
	// Stopped reports that a graceful stop ended the run before the where range was done.
	Stopped bool
}

func (c *Config) validate() error {
	switch {
	case c.Spec == nil:
		return errors.New("engine: no job")
	case !c.Spec.Type.Valid():
		return fmt.Errorf("engine: job %q has job_type %q", c.Name, c.Spec.Type)
	case c.Source == nil || c.Destination == nil:
		return fmt.Errorf("engine: job %q needs a source and a destination pool", c.Name)
	case c.JobSchema == "":
		return fmt.Errorf("engine: job %q has no job_schema", c.Name)
	case c.Processing.BatchSize < 1 || c.Processing.BatchDeleteSize < 1:
		return fmt.Errorf("engine: job %q needs batch_size and batch_delete_size of at least 1", c.Name)
	case c.Spec.Type != jobspec.Purge && !c.Verification.Valid():
		return fmt.Errorf("engine: job %q has verification %q", c.Name, c.Verification)
	}
	if n := c.Destination.Stats().MaxOpenConnections; n > 0 && n < minDestinationConnections {
		return fmt.Errorf("engine: the destination pool allows %d connections, and a run needs at least %d "+
			"(destination.max_connections): startup holds the root-table and job locks on two of them while it "+
			"reads the job registry on a third", n, minDestinationConnections)
	}
	return nil
}

// minDestinationConnections is the smallest destination pool a run can use: the job lock
// holds one connection for the whole run, the root-table lock a second during startup, and
// the registry, the log and the copy need a third (M5-F6). A smaller pool would wait forever.
const minDestinationConnections = 3

// Run runs one job: startup, recovery, then batches until the where range is exhausted or a
// graceful stop is requested. It returns what it did, even when it fails part way.
func Run(ctx context.Context, cfg Config) (*Report, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.Logger == nil {
		cfg.Logger = logger.NewDefault()
	}
	src, err := database.ReadServerIdentity(ctx, cfg.Source)
	if err != nil {
		return nil, fmt.Errorf("engine: source identity: %w", err)
	}
	dst, err := database.ReadServerIdentity(ctx, cfg.Destination)
	if err != nil {
		return nil, fmt.Errorf("engine: destination identity: %w", err)
	}
	for _, p := range []struct {
		name string
		db   *sql.DB
	}{{"source", cfg.Source}, {"destination", cfg.Destination}} {
		if err := assertUTC(ctx, p.db); err != nil {
			return nil, fmt.Errorf("engine: %s: %w", p.name, err)
		}
	}
	res, err := preflight.Run(ctx, preflight.Config{
		Source: cfg.Source, SourceDatabase: src.Schema,
		Destination: cfg.Destination, DestinationDatabase: dst.Schema, JobSchema: cfg.JobSchema,
		Spec: cfg.Spec, Verification: cfg.Verification,
		ForceTriggers: cfg.ForceTriggers, SkipPolicy: cfg.SkipPolicy, Logger: cfg.Logger,
	})
	if err != nil {
		return nil, err
	}
	if cfg.preflighted != nil {
		cfg.preflighted(res)
	}
	return start(ctx, cfg, tracking.Binding{
		SourceServerUUID: src.ServerUUID, SourceDatabase: src.Schema, DestinationDatabase: dst.Schema,
		Fingerprint: tracking.FingerprintOf(cfg.Spec),
	}, res)
}

// utcProbe returns 86400 only in a session whose time zone is UTC.
const utcProbe = "SELECT CAST(UNIX_TIMESTAMP('1970-01-02 00:00:00') AS SIGNED)"

// assertUTC refuses a pool whose sessions are not pinned to UTC: TIMESTAMP key text and
// its DATETIME(n) binding agree only there (spec §4.5, M5-4).
func assertUTC(ctx context.Context, db *sql.DB) error {
	var got int64
	if err := db.QueryRowContext(ctx, utcProbe).Scan(&got); err != nil {
		return fmt.Errorf("read the session time zone: %w", err)
	}
	if got != 86400 {
		return fmt.Errorf("the session time zone is not UTC (UNIX_TIMESTAMP('1970-01-02 00:00:00') = %d); every GoArchive session is pinned to '+00:00'", got)
	}
	return nil
}

// start runs everything after preflight: the tracking schema, the locks, the registry
// checks, recovery and the loop.
func start(ctx context.Context, cfg Config, binding tracking.Binding, res *preflight.Result) (*Report, error) {
	log := cfg.Logger
	root, ok := res.Keys[cfg.Spec.RootTable]
	if !ok {
		return nil, fmt.Errorf("engine: preflight returned no key for the root table %s", cfg.Spec.RootTable)
	}
	store, err := tracking.Open(ctx, cfg.Destination, cfg.JobSchema)
	if err != nil {
		return nil, err
	}
	jobLock, rootLock, err := lockJob(ctx, cfg, store)
	if err != nil {
		return nil, err
	}
	defer releaseLock(jobLock, log)
	// The root lock is held until the job is marked running (lockJob).
	rootHeld := true
	releaseRoot := func() {
		if rootHeld {
			releaseLock(rootLock, log)
			rootHeld = false
		}
	}
	defer releaseRoot()

	job, err := store.Attach(ctx, tracking.JobInput{Name: cfg.Name, Type: cfg.Spec.Type, RootKey: root, Binding: binding})
	if err != nil {
		return nil, err
	}
	r, err := newRun(ctx, cfg, res, job)
	if err != nil {
		return nil, err
	}
	if err := job.SetStatus(ctx, tracking.JobRunning); err != nil {
		return nil, err
	}
	r.reached(atMarked)
	if err := job.Heartbeat(ctx); err != nil {
		finalStatus(ctx, job, tracking.JobFailed, cfg.Name, log)
		return nil, err
	}
	r.reached(atRunning)
	releaseRoot()

	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stopBeat := keepAlive(runCtx, cancel, jobLock, job, log)

	err = r.execute(runCtx)
	stopBeat()
	if cause := context.Cause(runCtx); err != nil && cause != nil && !errors.Is(cause, context.Canceled) {
		err = fmt.Errorf("%w (%v)", cause, err)
	}

	final := tracking.JobIdle
	if err != nil {
		final = tracking.JobFailed
	}
	finalStatus(ctx, job, final, cfg.Name, log)
	return r.report, err
}

// finalStatus writes the job's status at the end of a run that marked it running, on a
// cleanup context: every way out of such a run writes it (M5-F1).
func finalStatus(ctx context.Context, job *tracking.Job, st tracking.JobStatus, name string, log *logger.Logger) {
	statusCtx, done := cleanupContext(ctx)
	defer done()
	if err := job.SetStatus(statusCtx, st); err != nil {
		log.Errorw("failed to write the final job status", "job", name, "status", st, "error", err)
	}
}
