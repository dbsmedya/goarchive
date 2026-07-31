// Package archiver provides preflight safety checks for GoArchive.
package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
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
		if err := p.ValidateDestinationSchemaCompatibility(ctx, run); err != nil {
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
	if err := p.ValidateForeignKeyIndexes(ctx, run); err != nil {
		return err
	}

	// FK_COVERAGE_VISIBILITY_CHECK: coverage is only trustworthy if we can see
	// constraints in every schema. Fail closed before relying on it — except for
	// copy-only, which never deletes from source (no external cascade can fire).
	if enforceFKVisibility {
		if err := p.ValidateForeignKeyMetadataVisibility(ctx, run); err != nil {
			return err
		}
	}

	// FK_COVERAGE_CHECK: Validate all FK constraints are covered by relations
	// This MUST be checked before triggers - missing relations are a bigger problem
	if err := p.ValidateForeignKeyCoverage(ctx, run); err != nil {
		return err
	}

	// INTERNAL_FK_COVERAGE: Validate all internal FK relationships match graph edges
	if err := p.ValidateInternalFKCoverage(ctx, run); err != nil {
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
		if err := p.WarnCascadeRules(ctx, run); err != nil {
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

// ValidateRootPKNumeric ensures the root table primary key is an integer type.
//
// The graph metadata every production reader consumes is supplied by loadRootPKMeta,
// which is called by all three orchestrators on every path that reads it; this stage
// does not write it.
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

// ValidateForeignKeyIndexes checks that every in-graph child table's foreign key has the
// supporting leftmost-prefix index MySQL guarantees for registered InnoDB constraints.
//
// A finding here means the facts did not come from a conforming InnoDB source, so in
// practice this check cannot fire against a real server — MySQL creates the index with
// the constraint and refuses to drop it, and the authoritative INNODB_FOREIGN path marks
// registered constraints indexed by construction. It is kept because the guarantee is
// exactly what the delete phase's performance depends on, and because losing the check
// would silently remove the assertion. See the unit tests for its behaviour on synthetic
// facts, and Ambiguity 1 for why no end-to-end fixture exists.
//
// GA-P4-F3-T3: FK index check
func (p *PreflightChecker) ValidateForeignKeyIndexes(ctx context.Context, run *preflightRun) error {
	p.logger.Debug("Checking foreign key indexes...")

	result, err := run.fkOutgoing(ctx)
	if err != nil {
		return inspectionError("fk_index", err)
	}

	unindexed, err := unindexedFKColumns("fk_index", validations.CheckFKIndexed(result.Keys))
	if err != nil {
		return err
	}
	if len(unindexed) > 0 {
		return &PreflightError{
			Check:   "FK_INDEX_CHECK",
			Message: "Foreign key columns without indexes (will cause slow deletes). Add indexes with: CREATE INDEX idx_fk ON table(column)",
			Tables:  unindexed,
		}
	}

	p.logger.Debugf("FK index check PASSED (%d foreign keys verified)", len(result.Keys))
	return nil
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

// WarnCascadeRules warns about ON DELETE CASCADE rules that may cause unexpected
// deletions. It is a WARNING, never a failure — cascade is legitimate design, and the
// operator is the one who knows whether it is intended here.
//
// The edge set is the union of the incoming and outgoing fetches, deduplicated by the
// child-side constraint triple: a constraint with both endpoints in the graph appears in
// both results (spec §3.5). Both facts were already acquired by earlier stages, so this
// stage issues no query.
//
// GA-P4-F3-T6: CASCADE rule warning
func (p *PreflightChecker) WarnCascadeRules(ctx context.Context, run *preflightRun) error {
	p.logger.Debug("Checking for CASCADE rules...")

	incoming, err := run.fkIncoming(ctx)
	if err != nil {
		return inspectionError("cascade", err)
	}
	outgoing, err := run.fkOutgoing(ctx)
	if err != nil {
		return inspectionError("cascade", err)
	}

	cascadeRules, err := dedupeCascadeEdges("cascade",
		validations.CheckCascadeRules(incoming.Keys),
		validations.CheckCascadeRules(outgoing.Keys))
	if err != nil {
		return err
	}

	// Both fetches are ordered by Graph.AllNodes(), which is a map range, so the union's
	// order varies run to run. Sort so an operator comparing two runs of the same schema
	// sees the same warning. Phases 025 and 026 fixed the identical defect.
	sort.Strings(cascadeRules)

	if len(cascadeRules) > 0 {
		// GA-P4-F3-T6: This is a WARNING, not an error
		p.logger.Warnf("ON DELETE CASCADE rules detected (%d): %v", len(cascadeRules), cascadeRules)
		p.logger.Warn("CASCADE rules may cause automatic deletion of related records. Verify this is intended behavior.")
	} else {
		p.logger.Debug("CASCADE rule check complete (no CASCADE rules found)")
	}

	return nil
}

// ValidateForeignKeyMetadataVisibility fails closed when foreign-key discovery cannot be
// proven complete.
//
// Deviation D2 (spec §4), invariant I1 (spec §3.1). The proof is the success of the
// library's PROCESS-gated InnoDB registry query, reported as VisibilityComplete on the
// same ForeignKeyResult that supplies the coverage facts — so the facts and the proof
// that they are complete can never disagree.
//
// This judges CheckFKClosure's VISIBILITY-flavoured finding rather than reading
// result.Visibility directly. Both would agree today, and the direct read is shorter — but
// the typed finding is the contract the design fixes (spec §3.1), and going through
// partitionClosureFindings is what makes an unrecognised future payload abort instead of
// being silently ignored. CheckFKClosure is pure computation over the already-memoized
// fact, so calling it here AND in coverage costs no extra I/O.
//
// This replaces 1.8's global-SELECT reasoning, which was a latent false-pass: while
// @@global.partial_revokes is enabled, a global SELECT row does not guarantee every
// schema is readable, so an incoming cross-schema foreign key could be invisible to an
// account that 1.8 accepted.
//
// I1 requires EFFECTIVE process privilege, not direct provenance: statement success is
// the proof, so a role-held PROCESS is exactly as safe. GoArchive therefore does not
// inspect PROCESS through Grants at all.
func (p *PreflightChecker) ValidateForeignKeyMetadataVisibility(ctx context.Context, run *preflightRun) error {
	p.logger.Debug("Checking FK metadata visibility...")

	result, err := run.fkIncoming(ctx)
	if err != nil {
		return inspectionError("fk_metadata_visibility", err)
	}

	_, visibility, err := partitionClosureFindings(
		validations.CheckFKClosure(result, p.sourceDBName, run.graphTables()))
	if err != nil {
		return err
	}
	if len(visibility) == 0 {
		p.logger.Debug("FK metadata visibility check PASSED (InnoDB metadata registry readable)")
		return nil
	}

	return &PreflightError{
		Check: "FK_COVERAGE_VISIBILITY_CHECK",
		Message: fmt.Sprintf(
			"foreign-key metadata completeness is not established (state: %s), so GoArchive cannot "+
				"verify there are no foreign keys in other schemas referencing the archive graph "+
				"(an external ON DELETE CASCADE/SET NULL would delete or mutate uncopied rows). "+
				"GoArchive 2.0 proves completeness by reading the InnoDB metadata registry, which "+
				"requires the PROCESS privilege: GRANT PROCESS ON *.* TO <user>. A role-held "+
				"PROCESS is equally acceptable — the proof is that the read succeeded",
			visibility[0]),
	}
}

// ValidateForeignKeyCoverage checks that no table outside the graph holds a foreign key
// into a graph table. Any uncovered FK is fatal regardless of ON DELETE rule
// (CASCADE/RESTRICT/NO ACTION/SET NULL) — a cross-schema child cannot be represented in
// the graph, because identifiers forbid schema.table.
//
// This judges only the external-edge flavour of CheckFKClosure's output. The visibility
// flavour is judged by ValidateForeignKeyMetadataVisibility, which runs first and which
// copy-only skips — see partitionClosureFindings.
func (p *PreflightChecker) ValidateForeignKeyCoverage(ctx context.Context, run *preflightRun) error {
	p.logger.Debug("Checking foreign key coverage...")

	result, err := run.fkIncoming(ctx)
	if err != nil {
		return inspectionError("fk_coverage", err)
	}

	external, _, err := partitionClosureFindings(
		validations.CheckFKClosure(result, p.sourceDBName, run.graphTables()))
	if err != nil {
		return err
	}
	if len(external) == 0 {
		p.logger.Debug("Foreign key coverage check complete (all FKs covered)")
		return nil
	}

	// Group consecutive findings by ParentTable into "  - <parent> is referenced by:
	// […]" lines. CheckFKClosure returns findings in requested-parent order, and the
	// requested order is run.graphTables() -> Graph.AllNodes(), which ranges over a
	// map — so findings for one parent are still consecutive, but the order of the
	// groups varies run to run. Sorting the assembled lines is what makes the message
	// deterministic; without it, a multi-parent message would vary exactly as in 1.8.
	var lines []string
	var current string
	var children []string
	flush := func() {
		if current != "" {
			lines = append(lines, fmt.Sprintf("  - %s is referenced by: %v", current, children))
		}
	}
	for _, fk := range external {
		if fk.ParentTable != current {
			flush()
			current = fk.ParentTable
			children = nil
		}
		children = append(children, fmt.Sprintf("%s.%s (ON DELETE %s)", fk.ChildSchema, fk.ChildTable, fk.OnDelete))
	}
	flush()
	sort.Strings(lines)

	return &PreflightError{
		Check: "FK_COVERAGE_CHECK",
		Message: fmt.Sprintf(
			"Foreign key constraints not covered by relations (fatal for any ON DELETE rule):\n%s",
			strings.Join(lines, "\n")),
	}
}

// ValidateInternalFKCoverage checks that all FK relationships between tables within the
// graph are properly represented as graph edges with matching columns.
//
// This prevents delete failures (Error 1451) caused by missing relation nesting in the
// configuration — for example a DB constraint item_shipments.item_id -> order_items.item_id
// while the config puts item_shipments as a SIBLING of order_items rather than a child.
//
// This is a goarchive-only check: it reconciles library facts against goarchive's own
// graph model, which the library has no view of.
func (p *PreflightChecker) ValidateInternalFKCoverage(ctx context.Context, run *preflightRun) error {
	p.logger.Debug("Checking internal FK coverage...")

	result, err := run.fkWithin(ctx)
	if err != nil {
		return inspectionError("internal_fk_coverage", err)
	}

	messages := reconcileInternalFKs(result.Keys, p.graph)
	if len(messages) > 0 {
		// Fact order follows the request order, which is Graph.AllNodes() — a map range.
		// Sort so an operator comparing two runs of the same failing config sees the same
		// report. Phase 025 fixed the identical defect in ValidateForeignKeyCoverage.
		sort.Strings(messages)
		return &PreflightError{
			Check: "INTERNAL_FK_COVERAGE",
			Message: fmt.Sprintf(
				"Internal FK relationships not matching configuration:\n%s\n\nHint: Ensure child tables are nested under their parent in the relations configuration, with matching foreign_key and primary_key values.",
				strings.Join(messages, "\n")),
		}
	}

	p.logger.Debug("Internal FK coverage check PASSED")
	return nil
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

// ValidateDestinationSchemaCompatibility ensures destination tables can receive copies of
// the source tables. The destination is allowed to be LOOSER than the source — dropped
// secondary indexes, auto_increment, defaults, ON UPDATE, relaxed NOT NULL — but never
// STRICTER. See evaluateSchemaCompatibility for the full disposition table.
//
// Fatality aggregates ACROSS tables: one error reports every incompatible table rather
// than aborting on the first.
func (p *PreflightChecker) ValidateDestinationSchemaCompatibility(ctx context.Context, run *preflightRun) error {
	if p.destinationDB == nil {
		return fmt.Errorf("destination database not configured; call ConfigureDestination first")
	}
	p.logger.Debug("Checking destination schema compatibility...")

	pairs, err := run.tableSpecs(ctx)
	if err != nil {
		return inspectionError("destination_schema_compatibility", err)
	}

	charsetStrict := p.charsetMismatchFatal()
	var incompatible []string
	var warnings []string
	for _, pair := range pairs {
		verdict, err := evaluateSchemaCompatibility(pair, charsetStrict)
		if err != nil {
			return err
		}
		if verdict.Fatal != "" {
			incompatible = append(incompatible, verdict.Fatal)
			continue
		}
		for _, w := range verdict.Warnings {
			warnings = append(warnings, "Table "+pair.Table+" "+w)
		}
	}

	// Spec §3.3, declared evaluation order: fatal outcomes are determined for ALL tables
	// first, then advisory warnings are emitted only for tables whose own verdict was
	// non-fatal. A fatal on one table does not suppress an independent warning on another.
	//
	// The per-table half of that contract is the `continue` in the loop above, NOT this
	// loop: evaluateSchemaCompatibility records the first fatal and keeps appending
	// warnings from later diffs (preflight_schema_policy.go:94-102), so a fatal table's
	// verdict really does carry advisories. Dropping the `continue` would leak them.
	//
	// This changes only WHICH advisory lines appear on runs that already fail fatally.
	// No pass/fail outcome changes, so it is a permissible output improvement under spec
	// §1.1 and deliberately NOT a ledger deviation (§4, ledger scope).
	for _, w := range warnings {
		p.logger.Warn(w)
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
