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
	// --skip-dangerously-validate-preflight-allow-experimental). Neither skips the log-key or binding check (spec §6.4).
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
	// ProgressLog writes --progress as log entries, past the level filter, instead of lines on
	// Out: the log is JSON on the console (spec §3.2 C).
	ProgressLog bool

	began time.Time // Run's start, for --progress's elapsed (spec §3.5)

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

	// A commit's writes are done and it is not committed: the guarded commit's lock check
	// comes next (source-table-locks spec §4 step 4).
	atDeleteWritten point = "delete-written" // a source delete chunk
	atCopyWritten   point = "copy-written"   // the destination copy, its verification and log insert
	atLogWritten    point = "log-written"    // a log-only destination transaction

	// The run's own job-lock session has lost the lock, and its final status is about to be
	// written on a fresh session that takes the lock again first (Codex P1).
	atFinalWrite point = "final-write"
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
	// Verification is the method the run's copies were verified by; empty for purge.
	Verification jobspec.Verification
	// Verified holds, by table, the copied rows whose verification passed, for committed
	// batches. It is empty when nothing was compared (verification: none, or purge).
	Verified map[string]int64
	// Elapsed is the run's wall time, from Run's start until it returns.
	Elapsed time.Duration
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
	return nil
}

// checkPool refuses a pool too small for a run (M5-F6): a smaller one would wait forever.
func (c *Config) checkPool() error {
	if n := c.Source.Stats().MaxOpenConnections; n > 0 && n < minSourceConnections {
		return fmt.Errorf("engine: the source pool allows %d connections, and a run needs at least %d "+
			"(source.max_connections): the source table locks hold one for the whole run while the batch works on another",
			n, minSourceConnections)
	}
	if n := c.Destination.Stats().MaxOpenConnections; n > 0 && n < minDestinationConnections {
		return fmt.Errorf("engine: the destination pool allows %d connections, and a run needs at least %d "+
			"(destination.max_connections): the job lock holds one for the whole run while the registry, the copy "+
			"and the heartbeat use the others", n, minDestinationConnections)
	}
	return nil
}

// minSourceConnections and minDestinationConnections are the smallest pools a run can use.
// The source: the table locks' session and the batch's pinned session. The destination is
// kept at three (M5-F6, every gate since): the job lock's session, the copy's and the
// heartbeat's, which run at the same time; the retired root lock's slot is the heartbeat's.
const (
	minSourceConnections      = 2
	minDestinationConnections = 3
)

// Run runs one job: startup, recovery, then batches until the where range is exhausted or a
// graceful stop is requested. It returns what it did, even when it fails part way.
func Run(ctx context.Context, cfg Config) (rep *Report, err error) {
	began := time.Now()
	defer func() {
		if rep != nil {
			rep.Elapsed = time.Since(began)
		}
	}()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if err := cfg.checkPool(); err != nil {
		return nil, err
	}
	if cfg.Logger == nil {
		cfg.Logger = logger.NewDefault()
	}
	src, dst, res, err := prepare(ctx, cfg)
	if err != nil {
		return nil, err
	}
	// A run logs its statement caps; validate and dry-run print them instead (spec §3.7).
	for _, c := range res.Caps {
		cfg.Logger.Infof("Statement cap: %s: %d key tuples per statement", c, c.Tuples)
	}
	cfg.began = began
	return start(ctx, cfg, tracking.Binding{
		SourceServerUUID: src.ServerUUID, SourceDatabase: src.Schema, DestinationDatabase: dst.Schema,
		Fingerprint: tracking.FingerprintOf(cfg.Spec),
	}, res)
}

// prepare runs the startup checks that come before any lock, for Run and Explain: each
// pool's identity and its UTC pin, then preflight (spec §7.1).
func prepare(ctx context.Context, cfg Config) (src, dst database.ServerIdentity, res *preflight.Result, err error) {
	src, err = database.ReadServerIdentity(ctx, cfg.Source)
	if err != nil {
		return src, dst, nil, fmt.Errorf("engine: source identity: %w", err)
	}
	dst, err = database.ReadServerIdentity(ctx, cfg.Destination)
	if err != nil {
		return src, dst, nil, fmt.Errorf("engine: destination identity: %w", err)
	}
	for _, p := range []struct {
		name string
		db   *sql.DB
	}{{"source", cfg.Source}, {"destination", cfg.Destination}} {
		if err := assertUTC(ctx, p.db); err != nil {
			return src, dst, nil, fmt.Errorf("engine: %s: %w", p.name, err)
		}
	}
	res, err = preflight.Run(ctx, preflight.Config{
		Source: cfg.Source, SourceDatabase: src.Schema,
		Destination: cfg.Destination, DestinationDatabase: dst.Schema, JobSchema: cfg.JobSchema,
		Spec: cfg.Spec, Verification: cfg.Verification,
		ForceTriggers: cfg.ForceTriggers, SkipPolicy: cfg.SkipPolicy, Processing: cfg.Processing, Logger: cfg.Logger,
	})
	if err != nil {
		return src, dst, nil, err
	}
	if cfg.preflighted != nil {
		cfg.preflighted(res)
	}
	return src, dst, res, nil
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
	// runCtx is cancelled with the reason when a lock set is lost or the heartbeat fails,
	// from the moment the first lock is held (spec §4 step 3).
	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	held, err := lockJob(runCtx, cancel, cfg, store, binding)
	if err != nil {
		return nil, withCause(runCtx, err)
	}
	defer held.release(log)

	job, err := store.Attach(runCtx, tracking.JobInput{Name: cfg.Name, Type: cfg.Spec.Type, RootKey: root, Binding: binding})
	if err != nil {
		return nil, withCause(runCtx, err)
	}
	r, err := newRun(runCtx, cfg, res, job, binding.SourceDatabase)
	if err != nil {
		return nil, withCause(runCtx, err)
	}
	r.locks = held
	if err := job.SetStatus(runCtx, tracking.JobRunning); err != nil {
		return nil, withCause(runCtx, err)
	}
	r.reached(atMarked)
	if err := job.Heartbeat(runCtx); err != nil {
		finalStatus(ctx, cfg, job, tracking.JobFailed, held.job)
		return nil, withCause(runCtx, err)
	}
	r.reached(atRunning)

	stopBeat := heartbeat(runCtx, cancel, job, log)
	err = r.execute(runCtx)
	stopBeat()
	err = withCause(runCtx, err)

	final := tracking.JobIdle
	if err != nil {
		final = tracking.JobFailed
	}
	if taker := finalStatus(ctx, cfg, job, final, held.job); taker != 0 && err != nil {
		err = fmt.Errorf("%w; job %q is now run by destination connection %d, which owns its status", err, cfg.Name, taker)
	}
	return r.report, err
}
