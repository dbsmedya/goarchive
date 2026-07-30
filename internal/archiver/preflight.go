// Package archiver provides preflight safety checks for GoArchive.
package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
)

// PreflightError represents a preflight check failure.
//
// GA-P4-F3: Preflight check error reporting
type PreflightError struct {
	Check   string
	Message string
	Tables  []string
}

func (e *PreflightError) Error() string {
	if len(e.Tables) > 0 {
		return fmt.Sprintf("%s: %s (tables: %v)", e.Check, e.Message, e.Tables)
	}
	return fmt.Sprintf("%s: %s", e.Check, e.Message)
}

// ForeignKeyResult holds foreign key constraint information.
//
// GA-P4-F3-T3: FK index check
// GA-P4-F3-T6: CASCADE rule warning
type ForeignKeyResult struct {
	TableSchema           string
	Table                 string
	ConstraintName        string
	Column                string
	ReferencedTableSchema string
	ReferencedTable       string
	ReferencedColumn      string
	OnDelete              string // CASCADE, SET NULL, RESTRICT, etc.
	OnUpdate              string
	Indexed               bool // Whether the FK column has an index
}

// PreflightProfile selects the subset of checks needed for a command.
type PreflightProfile int

const (
	PreflightProfileFull PreflightProfile = iota
	PreflightProfileSourceOnly
	PreflightProfileNonDestructive
)

// PreflightChecker performs safety checks before archiving.
//
// GA-P4-F3: Preflight Checks
type PreflightChecker struct {
	db                *sql.DB
	sourceDBName      string
	destinationDB     *sql.DB
	destinationDBName string
	jobSchemaName     string
	graph             *graph.Graph
	logger            *logger.Logger
	fkCache           []ForeignKeyResult
	fkCacheLoaded     bool
	verification      config.VerificationConfig
}

// NewPreflightChecker creates a new preflight checker.
func NewPreflightChecker(db *sql.DB, sourceDBName string, g *graph.Graph, log *logger.Logger) (*PreflightChecker, error) {
	if db == nil {
		return nil, fmt.Errorf("database is nil")
	}
	if sourceDBName == "" {
		return nil, fmt.Errorf("source database name is required")
	}
	if g == nil {
		return nil, fmt.Errorf("graph is nil")
	}
	if log == nil {
		log = logger.NewDefault()
	}

	return &PreflightChecker{
		db:                db,
		sourceDBName:      sourceDBName,
		destinationDB:     nil,
		destinationDBName: "",
		graph:             g,
		logger:            log,
	}, nil
}

// ConfigureDestination sets destination database context for destination-side preflight checks.
// jobSchema is the schema holding tracking tables (archiver_job + per-job logs);
// pass cfg.Destination.EffectiveJobSchema().
func (p *PreflightChecker) ConfigureDestination(db *sql.DB, destinationDBName, jobSchema string) error {
	if db == nil {
		return fmt.Errorf("destination database is nil")
	}
	if destinationDBName == "" {
		return fmt.Errorf("destination database name is required")
	}
	if jobSchema == "" {
		return fmt.Errorf("job schema is required")
	}
	p.destinationDB = db
	p.destinationDBName = destinationDBName
	p.jobSchemaName = jobSchema
	return nil
}

// RunAllChecks runs all preflight checks.
//
// GA-P4-F3-T7: Validate command implementation
func (p *PreflightChecker) RunWithProfile(ctx context.Context, profile PreflightProfile, forceTriggers bool, enforceFKVisibility bool) error {
	p.logger.Info("Running preflight checks...")

	// Facts are acquired per stage and memoized for this run only (spec §2). The run
	// is discarded when this function returns and is never stored on the checker.
	run := newPreflightRun(p)
	tables := run.graphTables()

	// GA-P4-F3-T2: Table existence check
	if err := p.ValidateTablesExist(ctx, run); err != nil {
		return err
	}

	// Validate configured PK columns exist and are explicitly defined.
	if err := p.ValidatePrimaryKeyColumns(ctx, run); err != nil {
		return err
	}

	// Reject composite primary keys: GoArchive identifies and DELETES rows by a
	// single PK column, so a multi-column PK would over-match (review P1-1).
	if err := p.ValidateSingleColumnPrimaryKey(ctx, run); err != nil {
		return err
	}

	if err := p.ValidateRootPKNumeric(ctx, run); err != nil {
		return err
	}

	// GA-P4-F3-T1: Storage engine check
	if err := p.ValidateStorageEngine(ctx, run); err != nil {
		return err
	}

	// INVISIBLE_COLUMN_CHECK: reject participating INVISIBLE columns. Rows are
	// copied with SELECT *, which omits invisible columns, so their values would
	// be silently dropped from the copy and the verification hash and then
	// deleted from the source (issue #23). Runs for every profile.
	if err := p.ValidateNoInvisibleColumns(ctx, run); err != nil {
		return err
	}

	// Tracking-schema privileges are needed by every command that writes
	// archiver_job / per-job logs (archive, purge, copy-only), independent of
	// the data-table destination checks below.
	if p.destinationDB != nil && p.jobSchemaName != "" {
		if err := p.ValidateJobSchemaPermissions(ctx, run); err != nil {
			return err
		}
	}

	// Destination checks ensure copy target is safe before archive execution.
	if profile != PreflightProfileSourceOnly && p.destinationDB != nil && p.destinationDBName != "" {
		if err := p.ValidateDestinationTablesExist(ctx, run); err != nil {
			return err
		}
		if err := p.ValidateDestinationSchemaCompatibility(ctx, tables); err != nil {
			return err
		}
		if err := p.ValidateDestinationWritePermissions(ctx, run); err != nil {
			return err
		}
		if err := p.ValidateDestinationInsertTriggers(ctx, run); err != nil {
			return err
		}
	}

	// GA-P4-F3-T3: FK index check
	if err := p.ValidateForeignKeyIndexes(ctx); err != nil {
		return err
	}

	// FK_COVERAGE_VISIBILITY_CHECK: coverage is only trustworthy if we can see
	// constraints in every schema. Fail closed before relying on it — except for
	// copy-only, which never deletes from source (no external cascade can fire).
	if enforceFKVisibility {
		if err := p.ValidateForeignKeyMetadataVisibility(ctx); err != nil {
			return err
		}
	}

	// FK_COVERAGE_CHECK: Validate all FK constraints are covered by relations
	// This MUST be checked before triggers - missing relations are a bigger problem
	if err := p.ValidateForeignKeyCoverage(ctx); err != nil {
		return err
	}

	// INTERNAL_FK_COVERAGE: Validate all internal FK relationships match graph edges
	if err := p.ValidateInternalFKCoverage(ctx); err != nil {
		return err
	}

	// SOURCE_SELECT_PERMISSION_CHECK (deviation D4, invariant I3): every command reads
	// source rows or estimates from them, so this runs for ALL profiles — deliberately
	// OUTSIDE the delete-capable block below. Its position is fixed by spec §3.2: after
	// INTERNAL_FK_COVERAGE and immediately before the conditional delete-permission slot.
	if err := p.ValidateSourceSelectPermissions(ctx, run); err != nil {
		return err
	}

	if profile == PreflightProfileFull || profile == PreflightProfileSourceOnly {
		if err := p.ValidateSourceDeletePermissions(ctx, run); err != nil {
			return err
		}

		// GA-P4-F3-T4 & T5: DELETE trigger detection (with force flag)
		if err := p.ValidateTriggers(ctx, run, forceTriggers); err != nil {
			return err
		}

		// GA-P4-F3-T6: CASCADE rule warning
		if err := p.WarnCascadeRules(ctx); err != nil {
			return err
		}
	}

	p.logger.Info("All preflight checks PASSED")
	return nil
}

// RunAllChecks runs the full preflight profile.
//
// GA-P4-F3-T7: Validate command implementation
func (p *PreflightChecker) RunAllChecks(ctx context.Context, forceTriggers bool) error {
	return p.RunWithProfile(ctx, PreflightProfileFull, forceTriggers, true)
}

// ValidateRootPKNumeric ensures the root table primary key is an integer type, and
// records its type and signedness on the graph.
//
// The write-back preserves a 1.8 stage side effect (spec §2 "What stays"); it is NOT
// what supplies the batch pipeline. Every production reader of the metadata
// (batch_pipeline.go) runs after loadRootPKMeta, which is called by all three
// orchestrators and unconditionally overwrites this value — so on the archive, purge and
// copy-only paths this write has no reader, and validate/dry-run never read it at all.
// It is kept because this phase is behaviour-preserving, not because anything consumes
// it. Removing it is a dead-write cleanup and belongs in phase 032.
//
// The error shape is deliberately a PLAIN string-prefixed error rather than a
// *PreflightError, preserving 1.8 behaviour (spec §2 "What stays"). Both prefixes are
// kept: ROOT_PK_TYPE_LOOKUP is unreachable through RunWithProfile (positions 1-3 have
// already proven the root table exists with a single-column PRIMARY KEY equal to the
// configured primary_key), but it remains as the defensive branch it always was.
//
// This now reads the source schema the checker was constructed with, via the run's
// memoized fact, instead of the connection's default schema (1.8 queried
// `WHERE TABLE_SCHEMA = DATABASE()`). Production opens the source pool with
// DBName = cfg.Database and passes that same value as sourceDBName, so the two are
// always equal there; the change removes an internal inconsistency rather than altering
// released behaviour, and carries no deviation-ledger entry.
func (p *PreflightChecker) ValidateRootPKNumeric(ctx context.Context, run *preflightRun) error {
	rootTable := p.graph.Root
	rootPKColumn := p.graph.GetPK(rootTable)

	pk, found, err := run.rootPKInfo(ctx)
	if err != nil {
		return fmt.Errorf("ROOT_PK_TYPE_LOOKUP: failed to look up data type for %s.%s: %w", rootTable, rootPKColumn, err)
	}
	if !found || pk.Kind != validations.PKSingle {
		return fmt.Errorf("ROOT_PK_TYPE_LOOKUP: failed to look up data type for %s.%s: no single-column primary key fact", rootTable, rootPKColumn)
	}

	// Written as a length check, NOT `for … range`. A range loop whose body returns on
	// every path trips staticcheck SA4004 ("the surrounding loop is unconditionally
	// terminated"), which is production-fatal here: .golangci.yml runs the standard set
	// at full strength and excludes SA rules only under `path: _test\.go`. Verified —
	// the loop form fails and this form passes.
	if findings := validations.CheckPKIntegerType([]validations.PKInfo{pk}); len(findings) > 0 {
		f := findings[0]
		if f.Check != validations.IDPKIntegerType {
			return unexpectedFindingError("root_pk_type", f)
		}
		return fmt.Errorf("ROOT_PK_TYPE_UNSUPPORTED: root table %q has primary key %q of type %q. GoArchive Community edition only supports integer root primary keys (TINYINT through BIGINT). See README 'Known Limits & Caution'", rootTable, rootPKColumn, pk.DataType)
	}

	// SetRootPKMeta moves here from inside the validator's query (spec §6 slice 4): the
	// fact is now acquired once by the run and written back explicitly at the call site.
	// See the doc comment: this preserves the 1.8 stage side effect and is not the write
	// the batch pipeline reads.
	if p.graph != nil {
		p.graph.SetRootPKMeta(strings.ToLower(pk.DataType), pk.Unsigned)
	}
	return nil
}

// ValidateSingleColumnPrimaryKey rejects participating tables whose PRIMARY KEY spans
// more than one column, has no PRIMARY KEY, or whose PRIMARY KEY is not the configured
// primary_key. GoArchive discovers, copies, verifies, and DELETES rows by a single PK
// column (WHERE pk IN (...)); any of those shapes lets that filter over-match and
// delete rows that were never part of the archived subgraph (review P1-1, review 003).
func (p *PreflightChecker) ValidateSingleColumnPrimaryKey(ctx context.Context, run *preflightRun) error {
	p.logger.Debug("Checking primary key shape (single-column PRIMARY KEY matching configured PK)...")

	facts, err := run.primaryKeys(ctx)
	if err != nil {
		return fmt.Errorf("COMPOSITE_PK_LOOKUP: failed to inspect primary keys: %w", err)
	}
	if err := judgePrimaryKeyShape(facts, run.expectedPKs()); err != nil {
		return err
	}

	p.logger.Debugf("Primary key shape check PASSED (%d tables)", len(run.graphTables()))
	return nil
}

// ValidateTablesExist checks that all tables in the graph exist in the source database.
//
// GA-P4-F3-T2: Table existence check
func (p *PreflightChecker) ValidateTablesExist(ctx context.Context, run *preflightRun) error {
	p.logger.Debug("Checking table existence...")

	found, err := run.sourceTables(ctx)
	if err != nil {
		return inspectionError("table_existence", err)
	}

	findings := validations.CheckTablesExist(run.graphTables(), found)
	for _, f := range findings {
		if f.Check != validations.IDTablesExist {
			return unexpectedFindingError("table_existence", f)
		}
	}
	if perr := findingsToPreflightError(
		"TABLE_EXISTENCE_CHECK",
		"Tables not found in source database",
		findings,
		validations.IDTablesExist,
	); perr != nil {
		return perr
	}

	// Explicit non-base-table policy (D5). CheckTablesExist proves name presence only,
	// so a view satisfies it; this is the earliest type-aware stage, and classifying
	// here reports the real problem instead of letting it surface downstream as an
	// unrelated structural error — a source view otherwise reached PRIMARY_KEY_CHECK,
	// which tells the operator to fix a primary key that a view cannot have.
	if nonBase := nonBaseTableNames(found); len(nonBase) > 0 {
		return &PreflightError{
			Check: "TABLE_EXISTENCE_CHECK",
			Message: "Only base tables can be archived. These source objects are not base tables; " +
				"remove them from the configuration, or archive the underlying tables instead",
			Tables: nonBase,
		}
	}

	p.logger.Debugf("Table existence check PASSED (%d tables)", len(run.graphTables()))
	return nil
}

// ValidateStorageEngine checks that all graph tables use the InnoDB storage engine.
//
// The table fact was already acquired by the existence check at position 1, so this
// stage issues no query of its own. Non-base objects are skipped by CheckStorageEngine
// because they have no engine; phase 013's non-base-table policy has already rejected
// them at that earlier stage, so nothing reaches here that needs skipping.
//
// GA-P4-F3-T1: Storage engine check
func (p *PreflightChecker) ValidateStorageEngine(ctx context.Context, run *preflightRun) error {
	p.logger.Debug("Checking storage engines...")

	found, err := run.sourceTables(ctx)
	if err != nil {
		return inspectionError("storage_engine", err)
	}

	findings := validations.CheckStorageEngine(found, requiredStorageEngine)

	var offenders []string
	for _, f := range findings {
		if f.Check != validations.IDStorageEngine {
			return unexpectedFindingError("storage_engine", f)
		}
		info, ok := f.Facts.(validations.TableInfo)
		if !ok {
			return unexpectedFactsError(
				"storage_engine",
				f,
				"validations.TableInfo",
			)
		}
		// Deviation D6: MySQL can report a BASE TABLE with NULL ENGINE (anomalous
		// metadata — a corrupted or unknown-engine table). The library scans that into
		// an empty Engine, which fails the comparison and lands here. 1.8 never got this
		// far: it scanned ENGINE into a bare string, so NULL aborted preflight with a
		// driver error carrying no check ID and no table name. Failing closed under
		// STORAGE_ENGINE_CHECK keeps the verdict and adds the attribution.
		engine := info.Engine
		if engine == "" {
			engine = "<unknown>"
		}
		offenders = append(offenders, info.Table+"("+engine+")")
	}

	if len(offenders) > 0 {
		return &PreflightError{
			Check:   "STORAGE_ENGINE_CHECK",
			Message: "Only InnoDB tables are supported. Use ALTER TABLE to convert",
			Tables:  offenders,
		}
	}

	p.logger.Debugf("Storage engine check PASSED (all tables are InnoDB)")
	return nil
}

// requiredStorageEngine is the only engine GoArchive supports. Non-transactional
// engines cannot provide the integrity a copy-verify-delete cycle depends on.
const requiredStorageEngine = "InnoDB"

// ValidateNoInvisibleColumns rejects any participating (graph) table that has an
// INVISIBLE column. GoArchive copies rows with SELECT *, which MySQL excludes
// invisible columns from, so their stored values would be silently dropped from both
// the destination INSERT and the SHA/count verification and then deleted from the
// source (issue #23). Until explicit-column support exists, an invisible column in a
// participating table is a fatal data-loss hazard.
//
// The library's query uses the same EXTRA LIKE '%INVISIBLE%' predicate 1.8 used, which
// catches both plain invisible columns (EXTRA = "INVISIBLE") and generated ones
// (EXTRA = "STORED GENERATED INVISIBLE"). What changes is the shape of the answer, not
// the question.
func (p *PreflightChecker) ValidateNoInvisibleColumns(ctx context.Context, run *preflightRun) error {
	p.logger.Debug("Checking for invisible columns...")

	facts, err := run.invisibleColumns(ctx)
	if err != nil {
		return inspectionError("invisible_columns", err)
	}

	findings := validations.CheckInvisibleColumns(facts)

	// The library reports one finding per TABLE with a Columns slice; goarchive has
	// always reported one entry per COLUMN, as "<table>.<column>". Fan out so the
	// operator-visible shape is unchanged.
	var offenders []string
	for _, f := range findings {
		if f.Check != validations.IDInvisibleColumns {
			return unexpectedFindingError("invisible_columns", f)
		}
		fact, ok := f.Facts.(validations.InvisibleColumns)
		if !ok {
			return unexpectedFactsError("invisible_columns", f, "validations.InvisibleColumns")
		}
		for _, col := range fact.Columns {
			offenders = append(offenders, fact.Table+"."+col)
		}
	}

	if len(offenders) > 0 {
		return &PreflightError{
			Check: "INVISIBLE_COLUMN_CHECK",
			Message: "This version of GoArchive does not support INVISIBLE columns. Rows are copied " +
				"with SELECT *, which omits invisible columns, so their values would be silently dropped " +
				"from the copy and the verification hash and then deleted from the source. Make these " +
				"columns visible (ALTER TABLE ... ALTER COLUMN ... SET VISIBLE) or remove these tables " +
				"from the archive until explicit-column support exists",
			Tables: offenders,
		}
	}

	p.logger.Debug("Invisible column check PASSED (no invisible columns)")
	return nil
}

// ValidateForeignKeyIndexes checks that all foreign key columns have indexes.
//
// GA-P4-F3-T3: FK index check
func (p *PreflightChecker) ValidateForeignKeyIndexes(ctx context.Context) error {
	p.logger.Debug("Checking foreign key indexes...")

	// Get all foreign keys from the database
	fks, err := p.getForeignKeys(ctx)
	if err != nil {
		return fmt.Errorf("failed to get foreign keys: %w", err)
	}

	// FK index checks apply only to child tables in the graph. getForeignKeys
	// only computes fk.Indexed for in-graph children (out-of-graph children keep
	// the zero value false), so flagging out-of-graph children here would be a
	// false positive — and would shadow FK_COVERAGE_CHECK, which is the real,
	// more actionable error for an out-of-graph table referencing the graph.
	graphTableSet := make(map[string]bool)
	for _, t := range p.graph.AllNodes() {
		graphTableSet[t] = true
	}

	var unindexedFKs []string
	for _, fk := range fks {
		if !p.inGraph(fk.TableSchema, fk.Table, graphTableSet) {
			continue
		}
		if !fk.Indexed {
			unindexedFKs = append(unindexedFKs, fmt.Sprintf("%s.%s", fk.Table, fk.Column))
		}
	}

	if len(unindexedFKs) > 0 {
		return &PreflightError{
			Check:   "FK_INDEX_CHECK",
			Message: "Foreign key columns without indexes (will cause slow deletes). Add indexes with: CREATE INDEX idx_fk ON table(column)",
			Tables:  unindexedFKs,
		}
	}

	p.logger.Debugf("FK index check PASSED (%d foreign keys verified)", len(fks))
	return nil
}

// inGraph reports whether (schema, table) is a node in the archive graph. Graph
// nodes always live in the source schema, so a same-named table in another
// schema is NOT in the graph.
func (p *PreflightChecker) inGraph(schema, table string, set map[string]bool) bool {
	return schema == p.sourceDBName && set[table]
}

// getForeignKeys retrieves all foreign key constraints for tables in the graph.
func (p *PreflightChecker) getForeignKeys(ctx context.Context) ([]ForeignKeyResult, error) {
	if p.fkCacheLoaded {
		return p.fkCache, nil
	}

	tables := p.graph.AllNodes()
	graphTableSet := make(map[string]bool, len(tables))
	for _, table := range tables {
		graphTableSet[table] = true
	}

	const query = `
		SELECT
			kcu.TABLE_SCHEMA,
			kcu.TABLE_NAME,
			kcu.CONSTRAINT_NAME,
			kcu.COLUMN_NAME,
			kcu.REFERENCED_TABLE_SCHEMA,
			kcu.REFERENCED_TABLE_NAME,
			kcu.REFERENCED_COLUMN_NAME,
			rc.DELETE_RULE,
			rc.UPDATE_RULE
		FROM information_schema.KEY_COLUMN_USAGE kcu
		JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
			ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
			AND kcu.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
		WHERE kcu.REFERENCED_TABLE_NAME IS NOT NULL
		AND (
			(kcu.TABLE_SCHEMA = ? AND kcu.TABLE_NAME IN (?))
			OR (kcu.REFERENCED_TABLE_SCHEMA = ? AND kcu.REFERENCED_TABLE_NAME IN (?))
		)`

	// Build placeholders
	placeholders := make([]string, len(tables))
	for i := range tables {
		placeholders[i] = "?"
	}
	// Arg order matches placeholder order: branch-1 schema+tables, branch-2 schema+tables.
	args := make([]interface{}, 0, 2*(len(tables)+1))
	args = append(args, p.sourceDBName)
	for _, table := range tables {
		args = append(args, table)
	}
	args = append(args, p.sourceDBName)
	for _, table := range tables {
		args = append(args, table)
	}

	fullQuery := strings.Replace(query, "(?)", "("+strings.Join(placeholders, ",")+")", 1)
	fullQuery = strings.Replace(fullQuery, "(?)", "("+strings.Join(placeholders, ",")+")", 1)

	rows, err := p.db.QueryContext(ctx, fullQuery, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			p.logger.Warnf("Failed to close rows: %v", err)
		}
	}()

	var results []ForeignKeyResult
	for rows.Next() {
		var fk ForeignKeyResult
		if err := rows.Scan(
			&fk.TableSchema, &fk.Table, &fk.ConstraintName, &fk.Column,
			&fk.ReferencedTableSchema, &fk.ReferencedTable, &fk.ReferencedColumn,
			&fk.OnDelete, &fk.OnUpdate,
		); err != nil {
			return nil, err
		}

		// FK index checks apply only to tables in the graph.
		if p.inGraph(fk.TableSchema, fk.Table, graphTableSet) {
			fk.Indexed, err = p.isColumnIndexed(ctx, fk.Table, fk.Column)
			if err != nil {
				return nil, fmt.Errorf("failed to check index for %s.%s: %w", fk.Table, fk.Column, err)
			}
		}

		results = append(results, fk)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	p.fkCache = results
	p.fkCacheLoaded = true
	return results, nil
}

// isColumnIndexed checks if a column has an index.
func (p *PreflightChecker) isColumnIndexed(ctx context.Context, table, column string) (bool, error) {
	const query = `
		SELECT COUNT(*)
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = ?
		AND TABLE_NAME = ?
		AND COLUMN_NAME = ?`

	var count int
	err := p.db.QueryRowContext(ctx, query, p.sourceDBName, table, column).Scan(&count)
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

// ValidateTriggers checks for DELETE triggers on source tables.
//
// GA-P4-F3-T4: DELETE trigger detection
// GA-P4-F3-T5: --force-triggers flag support
func (p *PreflightChecker) ValidateTriggers(ctx context.Context, run *preflightRun, forceTriggers bool) error {
	p.logger.Debug("Checking for DELETE triggers...")

	facts, err := run.sourceDeleteTriggers(ctx)
	if err != nil {
		return inspectionError("delete_triggers", err)
	}

	findings := validations.CheckTriggersPresent(facts, validations.TriggerDelete)
	tableList, err := triggerOffenders("delete_triggers", findings)
	if err != nil {
		return err
	}

	if len(tableList) == 0 {
		p.logger.Debug("DELETE trigger check PASSED (no triggers found)")
		return nil
	}

	const msg = "DELETE triggers detected"

	// GA-P4-F3-T5: Allow override with --force-triggers flag
	if forceTriggers {
		p.logger.Warnf("%s (proceeding due to --force-triggers): %v", msg, tableList)
		return nil
	}

	return &PreflightError{
		Check:   "DELETE_TRIGGER_CHECK",
		Message: msg + ". Use --force-triggers to override (not recommended, triggers will fire during delete)",
		Tables:  tableList,
	}
}

// WarnCascadeRules warns about ON DELETE CASCADE rules that may cause unexpected deletions.
//
// GA-P4-F3-T6: CASCADE rule warning
func (p *PreflightChecker) WarnCascadeRules(ctx context.Context) error {
	p.logger.Debug("Checking for CASCADE rules...")

	fks, err := p.getForeignKeys(ctx)
	if err != nil {
		return fmt.Errorf("failed to get foreign keys: %w", err)
	}

	var cascadeRules []string
	for _, fk := range fks {
		if fk.OnDelete == "CASCADE" {
			cascadeRules = append(cascadeRules, fmt.Sprintf("%s.%s.%s->%s.%s.%s",
				fk.TableSchema, fk.Table, fk.Column,
				fk.ReferencedTableSchema, fk.ReferencedTable, fk.ReferencedColumn))
		}
	}

	if len(cascadeRules) > 0 {
		// GA-P4-F3-T6: This is a WARNING, not an error
		p.logger.Warnf("ON DELETE CASCADE rules detected (%d): %v", len(cascadeRules), cascadeRules)
		p.logger.Warn("CASCADE rules may cause automatic deletion of related records. Verify this is intended behavior.")
	} else {
		p.logger.Debug("CASCADE rule check complete (no CASCADE rules found)")
	}

	return nil
}

// ValidateForeignKeyMetadataVisibility fails closed when the source account
// cannot be guaranteed to see foreign keys defined in OTHER schemas. MySQL only
// exposes a constraint in information_schema.KEY_COLUMN_USAGE to an account with
// some privilege on the constraint's (child) table, and a schema the account has
// no privilege on is entirely invisible (not even listed in SCHEMATA). So a
// global SELECT is the only privilege that guarantees FK_COVERAGE_CHECK saw every
// incoming cross-schema foreign key.
func (p *PreflightChecker) ValidateForeignKeyMetadataVisibility(ctx context.Context) error {
	p.logger.Debug("Checking FK metadata visibility...")
	grantees, err := p.currentGrantees(ctx, p.db)
	if err != nil {
		return err
	}
	ok, err := p.hasGlobalPrivilege(ctx, p.db, grantees, "SELECT")
	if err != nil {
		return err
	}
	if !ok {
		return &PreflightError{
			Check: "FK_COVERAGE_VISIBILITY_CHECK",
			Message: fmt.Sprintf("source account %s lacks a global SELECT privilege, so GoArchive cannot "+
				"verify there are no foreign keys in other schemas referencing the archive graph (an external "+
				"ON DELETE CASCADE/SET NULL would delete or mutate uncopied rows). Grant server-wide visibility "+
				"(GRANT SELECT ON *.* TO <user>) or run preflight as an account that has it.", grantees[0]),
		}
	}
	p.logger.Debug("FK metadata visibility check PASSED (global SELECT present)")
	return nil
}

// ValidateForeignKeyCoverage checks that all foreign key constraints referencing
// tables in the graph are covered by relations in the configuration.
//
// This prevents unsafe delete behavior when a table outside the graph has an FK
// constraint to a table inside the graph. Any uncovered FK is treated as fatal,
// regardless of ON DELETE rule (CASCADE/RESTRICT/NO ACTION/SET NULL).
//
// Precondition: coverage is only trustworthy when the connected account can see
// constraints in every schema. Production callers reach this via RunWithProfile,
// which runs ValidateForeignKeyMetadataVisibility (FK_COVERAGE_VISIBILITY_CHECK)
// first for delete-capable commands. Calling this directly (as some tests do)
// bypasses that guarantee.
func (p *PreflightChecker) ValidateForeignKeyCoverage(ctx context.Context) error {
	p.logger.Debug("Checking foreign key coverage...")

	// Get all tables in the graph
	graphTables := p.graph.AllNodes()
	graphTableSet := make(map[string]bool)
	for _, t := range graphTables {
		graphTableSet[t] = true
	}

	fks, err := p.getForeignKeys(ctx)
	if err != nil {
		return fmt.Errorf("failed to query FK coverage: %w", err)
	}

	type uncoveredFK struct {
		Table           string
		Constraint      string
		Column          string
		ReferencedTable string
		OnDelete        string
	}
	var uncovered []uncoveredFK
	for _, fk := range fks {
		if p.inGraph(fk.ReferencedTableSchema, fk.ReferencedTable, graphTableSet) &&
			!p.inGraph(fk.TableSchema, fk.Table, graphTableSet) {
			uncovered = append(uncovered, uncoveredFK{
				Table:           fk.TableSchema + "." + fk.Table,
				Constraint:      fk.ConstraintName,
				Column:          fk.Column,
				ReferencedTable: fk.ReferencedTable,
				OnDelete:        fk.OnDelete,
			})
		}
	}

	if len(uncovered) > 0 {
		// Group by referenced table for better readability
		byRefTable := make(map[string][]uncoveredFK)
		for _, fk := range uncovered {
			byRefTable[fk.ReferencedTable] = append(byRefTable[fk.ReferencedTable], fk)
		}

		var messages []string
		for refTable, fks := range byRefTable {
			var childTables []string
			for _, fk := range fks {
				childTables = append(childTables, fmt.Sprintf("%s (ON DELETE %s)", fk.Table, fk.OnDelete))
			}
			messages = append(messages, fmt.Sprintf("  - %s is referenced by: %v", refTable, childTables))
		}

		return &PreflightError{
			Check:   "FK_COVERAGE_CHECK",
			Message: fmt.Sprintf("Foreign key constraints not covered by relations (fatal for any ON DELETE rule):\n%s", strings.Join(messages, "\n")),
		}
	}

	p.logger.Debug("Foreign key coverage check complete (all FKs covered)")
	return nil
}

// ValidateInternalFKCoverage checks that all FK relationships between tables
// within the graph are properly represented as graph edges with matching columns.
//
// This prevents delete failures (Error 1451) caused by missing relation nesting
// in the configuration. For example, if the DB has item_shipments.item_id -> order_items.item_id
// but the config puts item_shipments as a sibling of order_items instead of a child.
func (p *PreflightChecker) ValidateInternalFKCoverage(ctx context.Context) error {
	p.logger.Debug("Checking internal FK coverage...")

	graphTables := p.graph.AllNodes()
	graphTableSet := make(map[string]bool, len(graphTables))
	for _, t := range graphTables {
		graphTableSet[t] = true
	}

	fks, err := p.getForeignKeys(ctx)
	if err != nil {
		return fmt.Errorf("failed to query foreign keys: %w", err)
	}

	var messages []string
	for _, fk := range fks {
		// Only check FKs where BOTH tables are in the graph
		if !p.inGraph(fk.TableSchema, fk.Table, graphTableSet) ||
			!p.inGraph(fk.ReferencedTableSchema, fk.ReferencedTable, graphTableSet) {
			continue
		}

		// Skip self-referencing FKs (e.g., category.parent_id -> category.id)
		if fk.Table == fk.ReferencedTable {
			continue
		}

		edgeMeta := p.graph.GetEdgeMeta(fk.ReferencedTable, fk.Table)

		if edgeMeta == nil {
			messages = append(messages, fmt.Sprintf(
				"  - %s.%s -> %s.%s (constraint: %s) [no graph edge]",
				fk.Table, fk.Column, fk.ReferencedTable, fk.ReferencedColumn, fk.ConstraintName,
			))
			continue
		}

		if edgeMeta.ForeignKey != fk.Column {
			messages = append(messages, fmt.Sprintf(
				"  - %s.%s -> %s.%s (constraint: %s) [FK column mismatch: config has '%s', DB has '%s']",
				fk.Table, fk.Column, fk.ReferencedTable, fk.ReferencedColumn, fk.ConstraintName,
				edgeMeta.ForeignKey, fk.Column,
			))
			continue
		}

		parentPK := p.graph.GetPK(fk.ReferencedTable)
		if parentPK != fk.ReferencedColumn {
			messages = append(messages, fmt.Sprintf(
				"  - %s.%s -> %s.%s (constraint: %s) [reference column mismatch: config PK is '%s', DB references '%s']",
				fk.Table, fk.Column, fk.ReferencedTable, fk.ReferencedColumn, fk.ConstraintName,
				parentPK, fk.ReferencedColumn,
			))
		}
	}

	if len(messages) > 0 {
		return &PreflightError{
			Check: "INTERNAL_FK_COVERAGE",
			Message: fmt.Sprintf(
				"Internal FK relationships not matching configuration:\n%s\n\nHint: Ensure child tables are nested under their parent in the relations configuration, with matching foreign_key and primary_key values.",
				strings.Join(messages, "\n"),
			),
		}
	}

	p.logger.Debug("Internal FK coverage check PASSED")
	return nil
}

// ColumnDefinition represents column metadata used for schema compatibility checks.
type ColumnDefinition struct {
	OrdinalPosition int
	ColumnName      string
	ColumnType      string
	IsNullable      string
	ColumnKey       string
	Extra           string
	CharacterSet    string // empty for non-string columns
	Collation       string // empty for non-string columns
}

// ValidateDestinationTablesExist checks that all graph tables exist in destination DB
// and are base tables.
func (p *PreflightChecker) ValidateDestinationTablesExist(ctx context.Context, run *preflightRun) error {
	if p.destinationDB == nil {
		return fmt.Errorf("destination database not configured; call ConfigureDestination first")
	}
	p.logger.Debug("Checking destination table existence...")

	found, err := run.destTables(ctx)
	if err != nil {
		return inspectionError("destination_table_existence", err)
	}

	findings := validations.CheckTablesExist(run.graphTables(), found)
	for _, f := range findings {
		if f.Check != validations.IDTablesExist {
			return unexpectedFindingError("destination_table_existence", f)
		}
	}
	if perr := findingsToPreflightError(
		"DEST_TABLE_EXISTENCE_CHECK",
		"Tables not found in destination database",
		findings,
		validations.IDTablesExist,
	); perr != nil {
		return perr
	}

	// Explicit non-base-table policy (D5), destination half. CheckTablesExist proves
	// name presence only; this is the earliest type-aware stage on this side. A
	// destination view otherwise reached DEST_SCHEMA_COMPATIBILITY_CHECK, which
	// reported a structural mismatch rather than the actual problem.
	if nonBase := nonBaseTableNames(found); len(nonBase) > 0 {
		return &PreflightError{
			Check: "DEST_TABLE_EXISTENCE_CHECK",
			Message: "Only base tables can receive archived rows. These destination objects are " +
				"not base tables; create real tables in the destination schema",
			Tables: nonBase,
		}
	}

	p.logger.Debugf("Destination table existence check PASSED (%d tables)", len(run.graphTables()))
	return nil
}

// columnIncompatibility reports why a destination column cannot receive copies
// of the source column, or "" when compatible. The destination may be more
// permissive than the source — secondary indexes dropped, auto_increment and
// default generation removed, NULLs allowed — because the copy inserts explicit
// values for every column and never relies on destination defaults or indexes.
// It must not be stricter: the primary key is required for INSERT IGNORE
// idempotency during crash recovery, and extra constraints (NOT NULL, unique
// indexes, destination generated columns) would reject or silently skip rows.
// Generated-column rule is destination-only: if the destination column is
// generated, MySQL rejects explicit inserts with Error 3105 even under INSERT
// IGNORE. A source-generated column writing into a plain destination column is
// fine — SELECT materialises the value and the destination accepts it.
// charsetStrict controls whether a charset mismatch is fatal (true) or only
// a warning (false, used when sha256 verification will catch any corruption).
func columnIncompatibility(s, d ColumnDefinition, charsetStrict bool) string {
	if s.ColumnName != d.ColumnName {
		return "column name mismatch"
	}
	if normalizeColumnType(s.ColumnType) != normalizeColumnType(d.ColumnType) {
		return "column type mismatch"
	}
	if s.IsNullable == "YES" && d.IsNullable == "NO" {
		return "destination is NOT NULL but source allows NULL"
	}
	if (s.ColumnKey == "PRI") != (d.ColumnKey == "PRI") {
		return "primary key mismatch (destination must keep the source primary key for idempotent resume)"
	}
	if d.ColumnKey == "UNI" && s.ColumnKey != "UNI" {
		return "destination has a unique index the source lacks (INSERT IGNORE would silently skip rows)"
	}
	if isGeneratedColumn(d.Extra) {
		return "destination column is generated (copy inserts explicit values for every column; MySQL rejects them with Error 3105 even under INSERT IGNORE)"
	}
	if charsetStrict && s.CharacterSet != d.CharacterSet {
		return fmt.Sprintf("character set mismatch (source=%s, destination=%s): copying can silently transliterate or truncate text and count verification cannot detect it; align charsets or use sha256 verification",
			s.CharacterSet, d.CharacterSet)
	}
	return ""
}

func isGeneratedColumn(extra string) bool {
	return strings.Contains(extra, "VIRTUAL GENERATED") || strings.Contains(extra, "STORED GENERATED")
}

// intDisplayWidthRe matches the deprecated integer display width — the
// parenthesized digit count following an integer type keyword, e.g. the "(20)"
// in "bigint(20)" or "int(10) unsigned". Anchored at the start so it never
// touches the genuinely-semantic precision of varchar(255), decimal(10,2), etc.
var intDisplayWidthRe = regexp.MustCompile(`^(tinyint|smallint|mediumint|int|integer|bigint)\(\d+\)`)

// normalizeColumnType strips the integer display width so columns differing only
// by it compare equal — "bigint(20)" and "bigint" are the same type. The width
// has always been cosmetic (it never affected storage or value range) and MySQL
// 8.0.17+ no longer reports it, so a schema dumped from an older server would
// otherwise false-fail against an identical 8.0.17+ destination. unsigned and
// zerofill are preserved because they do change the value range.
func normalizeColumnType(t string) string {
	return intDisplayWidthRe.ReplaceAllString(strings.TrimSpace(t), "$1")
}

// ValidateDestinationSchemaCompatibility ensures destination tables can receive
// copies of the source tables: identical column names, order, and types, with
// the same primary key. The destination is allowed to drop secondary indexes,
// auto_increment, and column defaults, and to relax NOT NULL — see
// columnIncompatibility for the exact rules.
func (p *PreflightChecker) ValidateDestinationSchemaCompatibility(ctx context.Context, tables []string) error {
	if p.destinationDB == nil {
		return fmt.Errorf("destination database not configured; call ConfigureDestination first")
	}
	p.logger.Debug("Checking destination schema compatibility...")

	var incompatible []string
	for _, table := range tables {
		sourceColumns, err := p.getTableColumns(ctx, p.db, p.sourceDBName, table)
		if err != nil {
			return fmt.Errorf("failed to read source schema for %s: %w", table, err)
		}
		destColumns, err := p.getTableColumns(ctx, p.destinationDB, p.destinationDBName, table)
		if err != nil {
			return fmt.Errorf("failed to read destination schema for %s: %w", table, err)
		}

		if len(sourceColumns) != len(destColumns) {
			incompatible = append(incompatible, fmt.Sprintf("%s(column count mismatch: source=%d destination=%d)", table, len(sourceColumns), len(destColumns)))
			continue
		}

		charsetStrict := p.charsetMismatchFatal()
		for i := range sourceColumns {
			s := sourceColumns[i]
			d := destColumns[i]
			if reason := columnIncompatibility(s, d, charsetStrict); reason != "" {
				incompatible = append(incompatible, fmt.Sprintf("%s(position %d: %s; source=%s %s nullable=%s key=%s extra=%s, destination=%s %s nullable=%s key=%s extra=%s)",
					table, s.OrdinalPosition, reason,
					s.ColumnName, s.ColumnType, s.IsNullable, s.ColumnKey, s.Extra,
					d.ColumnName, d.ColumnType, d.IsNullable, d.ColumnKey, d.Extra))
				break
			}
			// Emit advisory warnings for charset/collation differences that are
			// not fatal in this run (non-strict path: sha256 verification active).
			if s.CharacterSet != d.CharacterSet {
				p.logger.Warnf("Table %s column %s: charset differs (source=%s destination=%s); sha256 verification will fail before delete if data is altered",
					table, s.ColumnName, s.CharacterSet, d.CharacterSet)
			} else if s.Collation != d.Collation {
				p.logger.Warnf("Table %s column %s: collation differs (source=%s destination=%s); stored bytes are identical but comparisons/sorting may differ in the archive",
					table, s.ColumnName, s.Collation, d.Collation)
			}
		}
	}

	if len(incompatible) > 0 {
		return &PreflightError{
			Check:   "DEST_SCHEMA_COMPATIBILITY_CHECK",
			Message: "Source and destination schemas are incompatible",
			Tables:  incompatible,
		}
	}

	p.logger.Debug("Destination schema compatibility check PASSED")
	return nil
}

// formatGrantee converts CURRENT_USER() output (user@host) into the quoted
// GRANTEE format used by information_schema privilege tables ('user'@'host').
// Verified against MySQL 8.4: the GRANTEE column is built by plain
// concatenation of quotes around the raw name — embedded single quotes are
// NOT escaped (user o'brien appears as 'o'brien'@'%'), so none are added here.
func formatGrantee(currentUser string) string {
	quote := func(s string) string { return "'" + s + "'" }
	at := strings.LastIndex(currentUser, "@")
	if at < 0 {
		return quote(currentUser) + "@'%'"
	}
	return quote(currentUser[:at]) + "@" + quote(currentUser[at+1:])
}

// roleGrantees converts CURRENT_ROLE() output (`r1`@`%`,`r2`@`host` or NONE)
// into GRANTEE-format strings. Privilege grants held via active roles do not
// appear under the user's own GRANTEE in information_schema.
func roleGrantees(currentRole string) []string {
	currentRole = strings.TrimSpace(currentRole)
	if currentRole == "" || strings.EqualFold(currentRole, "NONE") {
		return nil
	}
	var grantees []string
	for _, role := range strings.Split(currentRole, ",") {
		role = strings.TrimSpace(role)
		if role == "" {
			continue
		}
		grantees = append(grantees, strings.ReplaceAll(role, "`", "'"))
	}
	return grantees
}

// currentGrantees returns the GRANTEE strings to match in privilege tables:
// the authenticated account plus any active roles. CURRENT_ROLE() exists on
// all supported MySQL versions (8.0+); an error there is real and fails
// preflight rather than being ignored — missing roles would false-fail the
// privilege checks anyway. Only directly activated roles are listed;
// privileges held via roles granted to roles (nested) are not detected and
// will conservatively fail the check.
func (p *PreflightChecker) currentGrantees(ctx context.Context, db *sql.DB) ([]string, error) {
	var user string
	if err := db.QueryRowContext(ctx, "SELECT CURRENT_USER()").Scan(&user); err != nil {
		return nil, fmt.Errorf("failed to resolve CURRENT_USER(): %w", err)
	}
	grantees := []string{formatGrantee(user)}
	var role sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT CURRENT_ROLE()").Scan(&role); err != nil {
		return nil, fmt.Errorf("failed to resolve CURRENT_ROLE(): %w", err)
	}
	if role.Valid {
		grantees = append(grantees, roleGrantees(role.String)...)
	}
	return grantees, nil
}

// ValidateDestinationWritePermissions checks that the connected destination account
// holds INSERT on every graph table.
//
// Deviation D1 (spec §4): only GrantPresent passes. Two 1.8 behaviours therefore change:
// a privilege held through a role now fails (the library reports role-dependent answers
// as GrantUnconfirmed, and the 1.8 role path was root-tested only), and a global
// INSERT ON *.* no longer short-circuits while @@global.partial_revokes is enabled —
// under partial revokes a global row proves nothing about a particular object until a
// direct schema or table grant proves it. Without partial revokes a global grant still
// evaluates GrantPresent, so the recommended recipe is unaffected.
func (p *PreflightChecker) ValidateDestinationWritePermissions(ctx context.Context, run *preflightRun) error {
	if p.destinationDB == nil {
		return fmt.Errorf("destination database not configured; call ConfigureDestination first")
	}
	p.logger.Debug("Checking destination write permissions...")

	grants, err := run.destGrants(ctx)
	if err != nil {
		return inspectionError("destination_write", err)
	}

	findings := validations.CheckTablePrivileges(
		grants, p.destinationDBName, run.graphTables(), validations.PrivilegeInsert)
	offenders, err := privilegeOffenders("destination_write", findings)
	if err != nil {
		return err
	}
	if len(offenders) > 0 {
		return &PreflightError{
			Check: "DEST_WRITE_PERMISSION_CHECK",
			Message: "Destination account lacks provable INSERT privilege on required tables. " +
				"GoArchive 2.0 requires the privilege to be provable for each object: grant it " +
				"directly to the account (GRANT INSERT ON <schema>.<table> TO <user>, or on " +
				"<schema>.*)",
			Tables: offenders,
		}
	}

	p.logger.Debug("Destination write permission check PASSED")
	return nil
}

// jobSchemaPrivileges are the privileges GoArchive needs on the tracking schema. CREATE
// is required at runtime because per-job log tables are created on the fly. DELETE and
// DROP are optional (DBA cleanup only) and intentionally not required.
var jobSchemaPrivileges = []validations.Privilege{
	validations.PrivilegeCreate,
	validations.PrivilegeSelect,
	validations.PrivilegeInsert,
	validations.PrivilegeUpdate,
}

// ValidateJobSchemaPermissions checks the destination account holds CREATE +
// SELECT/INSERT/UPDATE on the tracking schema.
//
// Deviation D1 (spec §4): the check passes only on GrantPresent. A privilege held
// through a role evaluates GrantUnconfirmed and therefore fails — the 1.8 role path was
// root-tested only and could not prove what it claimed. Under @@global.partial_revokes,
// a direct schema-level grant is what proves the object; a global grant alone does not.
func (p *PreflightChecker) ValidateJobSchemaPermissions(ctx context.Context, run *preflightRun) error {
	if p.destinationDB == nil {
		return fmt.Errorf("destination database not configured; call ConfigureDestination first")
	}

	grants, err := run.destGrants(ctx)
	if err != nil {
		return inspectionError("job_schema", err)
	}

	findings := validations.CheckSchemaPrivileges(grants, p.jobSchemaName, jobSchemaPrivileges)
	offenders, err := privilegeOffenders("job_schema", findings)
	if err != nil {
		return err
	}
	if len(offenders) == 0 {
		p.logger.Debug("Job schema permission check PASSED")
		return nil
	}

	missing := make([]string, 0, len(findings))
	needsCreate := false
	for _, f := range findings {
		fact, ok := f.Facts.(validations.PrivilegeFact)
		if !ok {
			return unexpectedFactsError("job_schema", f, "validations.PrivilegeFact")
		}
		missing = append(missing, fact.Privilege.String())
		if fact.Privilege == validations.PrivilegeCreate {
			needsCreate = true
		}
	}
	grant := fmt.Sprintf("GRANT %s ON `%s`.* TO <user>", strings.Join(missing, ", "), p.jobSchemaName)
	hint := grant
	if needsCreate {
		hint = fmt.Sprintf("CREATE DATABASE `%s`; %s", p.jobSchemaName, grant)
	}

	return &PreflightError{
		Check: "JOB_SCHEMA_PERMISSION_CHECK",
		Message: fmt.Sprintf(
			"destination account lacks provable %s on tracking schema %q (states: %s). "+
				"GoArchive 2.0 requires each privilege to be provable for the object: grant each "+
				"missing privilege directly to the account at schema scope (DBA must: %s)",
			strings.Join(missing, ", "), p.jobSchemaName, strings.Join(offenders, ", "), hint),
	}
}

// hasGlobalPrivilege reports whether any grantee holds priv at global scope
// (information_schema.USER_PRIVILEGES). GRANT ALL is expanded by MySQL into
// individual PRIVILEGE_TYPE rows, so matching the specific type covers it.
func (p *PreflightChecker) hasGlobalPrivilege(ctx context.Context, db *sql.DB, grantees []string, priv string) (bool, error) {
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(grantees)), ",")
	args := make([]interface{}, 0, len(grantees)+1)
	for _, g := range grantees {
		args = append(args, g)
	}
	args = append(args, priv)
	q := fmt.Sprintf(`SELECT COUNT(*) FROM information_schema.USER_PRIVILEGES
		WHERE GRANTEE IN (%s) AND PRIVILEGE_TYPE = ?`, placeholders)
	var count int
	if err := db.QueryRowContext(ctx, q, args...).Scan(&count); err != nil {
		return false, fmt.Errorf("failed to check global %s privilege: %w", priv, err)
	}
	return count > 0, nil
}

// ValidateSourceSelectPermissions checks the source account can SELECT from every graph
// table.
//
// Deviation D4 (spec §4), invariant I3 (spec §3.2). GoArchive 1.8 never validated source
// read permission: a PROCESS+DELETE-only account passed preflight and failed mid-run,
// after the job row and per-job log table had already been created. Every command reads
// source data or estimates from it, so this check applies to ALL FIVE commands — unlike
// SOURCE_DELETE_PERMISSION_CHECK, which stays profile-dependent.
//
// The rule is invariant I2's: only GrantPresent passes. The Grants value is the one
// already cached for this run (phase 021); no additional query is issued.
func (p *PreflightChecker) ValidateSourceSelectPermissions(ctx context.Context, run *preflightRun) error {
	p.logger.Debug("Checking source select permissions...")

	grants, err := run.sourceGrants(ctx)
	if err != nil {
		return inspectionError("source_select", err)
	}

	findings := validations.CheckTablePrivileges(
		grants, p.sourceDBName, run.graphTables(), validations.PrivilegeSelect)
	offenders, err := privilegeOffenders("source_select", findings)
	if err != nil {
		return err
	}
	if len(offenders) > 0 {
		return &PreflightError{
			Check: "SOURCE_SELECT_PERMISSION_CHECK",
			Message: "Source account lacks provable SELECT privilege on required tables. " +
				"Every GoArchive command reads source rows or estimates from them, so this is " +
				"checked before any work starts. Grant it directly to the account " +
				"(GRANT SELECT ON <schema>.* TO <user>)",
			Tables: offenders,
		}
	}

	p.logger.Debug("Source select permission check PASSED")
	return nil
}

// ValidateSourceDeletePermissions checks the source account can DELETE from all graph
// tables. Without it, archive fails only after copy has committed.
//
// Deviation D1 (spec §4): only GrantPresent passes. See
// ValidateDestinationWritePermissions for the two 1.8 behaviours this changes.
func (p *PreflightChecker) ValidateSourceDeletePermissions(ctx context.Context, run *preflightRun) error {
	p.logger.Debug("Checking source delete permissions...")

	grants, err := run.sourceGrants(ctx)
	if err != nil {
		return inspectionError("source_delete", err)
	}

	findings := validations.CheckTablePrivileges(
		grants, p.sourceDBName, run.graphTables(), validations.PrivilegeDelete)
	offenders, err := privilegeOffenders("source_delete", findings)
	if err != nil {
		return err
	}
	if len(offenders) > 0 {
		return &PreflightError{
			Check: "SOURCE_DELETE_PERMISSION_CHECK",
			Message: "Source account lacks provable DELETE privilege on required tables. " +
				"GoArchive 2.0 requires the privilege to be provable for each object: grant it " +
				"directly to the account (GRANT DELETE ON <schema>.<table> TO <user>, or on " +
				"<schema>.*)",
			Tables: offenders,
		}
	}

	p.logger.Debug("Source delete permission check PASSED")
	return nil
}

func (p *PreflightChecker) getTableColumns(ctx context.Context, db *sql.DB, dbName, table string) ([]ColumnDefinition, error) {
	const query = `
		SELECT
			ORDINAL_POSITION,
			COLUMN_NAME,
			COLUMN_TYPE,
			IS_NULLABLE,
			COLUMN_KEY,
			EXTRA,
			COALESCE(CHARACTER_SET_NAME, ''),
			COALESCE(COLLATION_NAME, '')
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ?
		AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION`

	rows, err := db.QueryContext(ctx, query, dbName, table)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			p.logger.Warnf("Failed to close rows: %v", err)
		}
	}()

	var columns []ColumnDefinition
	for rows.Next() {
		var col ColumnDefinition
		if err := rows.Scan(&col.OrdinalPosition, &col.ColumnName, &col.ColumnType, &col.IsNullable, &col.ColumnKey, &col.Extra, &col.CharacterSet, &col.Collation); err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return columns, nil
}

// ValidatePrimaryKeyColumns checks that each table has an explicitly configured PK and
// that the configured PK column exists in the source table with the EXACT same name,
// including letter case. See judgePrimaryKeyColumns for the full rule and its rationale.
func (p *PreflightChecker) ValidatePrimaryKeyColumns(ctx context.Context, run *preflightRun) error {
	p.logger.Debug("Checking primary key column definitions...")

	facts, err := run.sourceColumns(ctx)
	if err != nil {
		return inspectionError("pk_columns", err)
	}
	if err := judgePrimaryKeyColumns(columnNamesByTable(facts), run.expectedPKs(), run.graphTables()); err != nil {
		return err
	}

	p.logger.Debugf("Primary key column check PASSED (%d tables)", len(run.graphTables()))
	return nil
}

// ValidateDestinationInsertTriggers checks for INSERT triggers on destination tables.
//
// --force-triggers deliberately does NOT apply here, and this signature is the
// guarantee: the flag exists so an operator can accept source DELETE triggers firing
// during the delete phase, which is a knowable, bounded risk. A destination INSERT
// trigger silently mutates copied rows before verification reads them back, so there is
// no safe override. Do not add a force parameter.
func (p *PreflightChecker) ValidateDestinationInsertTriggers(ctx context.Context, run *preflightRun) error {
	if p.destinationDB == nil {
		return fmt.Errorf("destination database not configured; call ConfigureDestination first")
	}
	p.logger.Debug("Checking destination INSERT triggers...")

	facts, err := run.destInsertTriggers(ctx)
	if err != nil {
		return inspectionError("destination_insert_triggers", err)
	}

	findings := validations.CheckTriggersPresent(facts, validations.TriggerInsert)
	tableList, err := triggerOffenders("destination_insert_triggers", findings)
	if err != nil {
		return err
	}

	if len(tableList) == 0 {
		p.logger.Debug("Destination INSERT trigger check PASSED (no triggers found)")
		return nil
	}

	return &PreflightError{
		Check:   "DEST_INSERT_TRIGGER_CHECK",
		Message: "Destination INSERT triggers detected. Disable triggers before running archive copy operations",
		Tables:  tableList,
	}
}

// SetVerification tells the checker which verification the job will run.
// Charset mismatches are fatal under count verification (which cannot detect
// transcoded text) but only warnings under sha256 (which fails closed at
// verify time, before any delete). Defaults to the zero value, whose
// EffectiveMethod is "count" — the strict path.
func (p *PreflightChecker) SetVerification(v config.VerificationConfig) {
	p.verification = v
}

// charsetMismatchFatal reports whether a charset difference must fail preflight.
func (p *PreflightChecker) charsetMismatchFatal() bool {
	return p.verification.SkipVerification || p.verification.EffectiveMethod() != "sha256"
}
