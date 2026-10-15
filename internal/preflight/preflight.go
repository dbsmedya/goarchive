// Package preflight is GoArchive 3.0's preflight (spec §8): the checks that run before any
// row is read, chosen by the job's type, over facts from dbsgomysql.
//
// It reads every table's primary key from MySQL, which is the only source of key SQL (R31),
// and returns those definitions with the effective statement caps. The documented
// primary_key, foreign_key and references of the job are checked against the facts; the
// policy checks (permissions, triggers, foreign-key coverage, destination compatibility)
// are the 2.x checks carried over, with the key-shaped ones reworked for composite keys;
// the packet size is 3.0's (M7c).
//
// goarchive never queries information_schema; every schema fact comes from the library.
package preflight

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
)

// Error is a preflight check failure: the schema or the account is not fit for the job.
// A plain error from Run means preflight could not find out.
type Error struct {
	Check   string
	Message string
	Tables  []string
}

func (e *Error) Error() string {
	if len(e.Tables) > 0 {
		return fmt.Sprintf("%s: %s (tables: %v)", e.Check, e.Message, e.Tables)
	}
	return fmt.Sprintf("%s: %s", e.Check, e.Message)
}

// Profile is the subset of checks a job type needs (spec §8.1).
type Profile int

const (
	// ProfileFull is archive: every check.
	ProfileFull Profile = iota
	// ProfileSourceOnly is purge: no destination-table checks. The job schema permission
	// check stays, because tracking lives there.
	ProfileSourceOnly
	// ProfileNonDestructive is copy-only: no delete permission, delete trigger, cascade or
	// self-reference delete-rule checks, and no foreign-key visibility proof.
	ProfileNonDestructive
)

// ProfileFor returns the profile of a job type. validate and dry-run use it too, so a
// purge job is never asked for destination tables (R5).
func ProfileFor(t jobspec.Type) Profile {
	switch t {
	case jobspec.Purge:
		return ProfileSourceOnly
	case jobspec.CopyOnly:
		return ProfileNonDestructive
	default:
		return ProfileFull
	}
}

// Config is one preflight run.
type Config struct {
	Source         *sql.DB
	SourceDatabase string

	// Destination is the destination connection. Every job type needs it: the tracking
	// tables live in JobSchema on the destination server (spec §6.1), purge included.
	Destination         *sql.DB
	DestinationDatabase string
	JobSchema           string

	// Spec is a job returned by jobspec.Load: validated, with references filled in.
	Spec *jobspec.Spec
	// Verification is the job's effective verification (Spec.EffectiveVerification, after
	// any --verification). A charset difference on a non-key column is fatal unless it is
	// sha256, the only method that would detect a changed value.
	Verification jobspec.Verification
	// ForceTriggers accepts source DELETE triggers (--force-triggers).
	ForceTriggers bool
	// SkipPolicy skips every policy check (--skip-dangerously-validate-preflight-allow-experimental). The key
	// definitions are still read, because the SQL is built from them (spec §8.3).
	SkipPolicy bool
	// Processing is the job's effective processing block (Spec.EffectiveProcessing). The
	// engine sends at most BatchSize keys in a statement, BatchDeleteSize in the source's
	// deletes, so MAX_ALLOWED_PACKET_CHECK bounds each statement there (M7c-F4). Both
	// must be at least 1.
	Processing config.ProcessingConfig

	Logger *logger.Logger
}

// Result is what a passing preflight learned.
type Result struct {
	// Keys holds every table's primary key as the source reports it, by the job's table
	// name.
	Keys map[string]key.Def
	// Caps are the effective statement caps (spec §4.3), in copy order.
	Caps []Cap
}

// Run checks the job against both servers and returns the source key definitions.
func Run(ctx context.Context, cfg Config) (*Result, error) {
	if cfg.Source == nil || cfg.SourceDatabase == "" {
		return nil, errors.New("preflight: the source connection and database are required")
	}
	if cfg.Destination == nil || cfg.DestinationDatabase == "" || cfg.JobSchema == "" {
		return nil, errors.New("preflight: the destination connection, database and job schema are required")
	}
	return run(ctx, cfg, newMySQLServer(cfg.Source, cfg.SourceDatabase),
		newMySQLServer(cfg.Destination, cfg.DestinationDatabase))
}

// run is Run over any server pair, so tests can supply facts.
func run(ctx context.Context, cfg Config, src, dst server) (*Result, error) {
	if cfg.Spec == nil {
		return nil, errors.New("preflight: a job is required")
	}
	if cfg.Processing.BatchSize < 1 || cfg.Processing.BatchDeleteSize < 1 {
		return nil, fmt.Errorf("preflight: processing batch_size and batch_delete_size must be at least 1, got %d and %d",
			cfg.Processing.BatchSize, cfg.Processing.BatchDeleteSize)
	}
	if cfg.Logger == nil {
		cfg.Logger = logger.NewDefault()
	}
	f := newFacts(cfg, src, dst)
	for _, st := range stages(cfg) {
		if !st.enabled {
			continue
		}
		if err := st.run(ctx, f); err != nil {
			return nil, err
		}
	}
	return &Result{Keys: f.keys, Caps: f.caps}, nil
}

// stage is one ordered check. A failure stops the run, so a later check can never report
// ahead of an earlier one.
type stage struct {
	name    string
	enabled bool
	run     func(context.Context, *facts) error
}

// stages is the one registry of check order and applicability.
func stages(cfg Config) []stage {
	profile := ProfileFor(cfg.Spec.Type)
	policy := !cfg.SkipPolicy
	destination := policy && profile != ProfileSourceOnly
	deletes := policy && profile != ProfileNonDestructive

	return []stage{
		// Structure first: the earliest actionable failure.
		{"TablesExist", policy, checkTablesExist},
		{"Keys", true, readKeys},
		{"PrimaryKeyColumns", policy, checkPrimaryKeyColumns},
		{"StorageEngine", policy, checkStorageEngine},
		{"RelationKeys", policy, checkRelationKeys},
		{"ForeignKeyIndexes", policy, checkForeignKeyIndexes},

		// The tracking schema is on the destination server for every job type.
		{"JobSchemaPermissions", policy, checkJobSchemaPermissions},
		{"DestinationTablesExist", destination, checkDestinationTablesExist},
		{"DestinationStorageEngine", destination, checkDestinationStorageEngine},
		{"DestinationKeys", destination, checkDestinationKeys},
		{"DestinationSchemaCompatibility", destination, checkDestinationSchemaCompatibility},
		{"DestinationWritePermissions", destination, checkDestinationWritePermissions},
		// Visibility must be proven before the trigger check trusts an empty list (beta audit A2).
		{"DestinationInsertTriggerVisibility", destination, checkDestinationInsertTriggerVisibility},
		{"DestinationInsertTriggers", destination, checkDestinationInsertTriggers},
		// Under SkipPolicy the destination keys are still read, unjudged: the destination
		// statement caps are computed from them (spec §8.3).
		{"DestinationKeyRead", cfg.SkipPolicy && profile != ProfileSourceOnly, readDestinationKeys},

		// Visibility must be proven before coverage relies on the discovered set.
		{"ForeignKeyMetadataVisibility", deletes, checkForeignKeyMetadataVisibility},
		{"ForeignKeyCoverage", policy, checkForeignKeyCoverage},
		{"InternalForeignKeys", policy, checkInternalForeignKeys},
		{"SelfForeignKeys", deletes, checkSelfForeignKeys},

		{"SourceSelectPermissions", policy, checkSourceSelectPermissions},
		{"SourceDeletePermissions", deletes, checkSourceDeletePermissions},
		{"DeleteTriggerVisibility", deletes, checkDeleteTriggerVisibility},
		{"DeleteTriggers", deletes, func(ctx context.Context, f *facts) error {
			return checkDeleteTriggers(ctx, f, cfg.ForceTriggers)
		}},
		{"CascadeRules", deletes, warnCascadeRules},

		{"StatementCaps", true, reportStatementCaps},
		{"MaxAllowedPacket", policy, checkMaxAllowedPacket},
	}
}
