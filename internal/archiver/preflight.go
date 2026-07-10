// Package archiver provides preflight safety checks for GoArchive.
package archiver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"

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

// StorageEngineResult holds storage engine check results.
//
// GA-P4-F3-T1: Storage engine check
type StorageEngineResult struct {
	Table  string
	Engine string
}

// TriggerCheckResult holds trigger detection results.
//
// GA-P4-F3-T4: DELETE trigger detection
type TriggerCheckResult struct {
	Table   string
	Trigger string
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
func (p *PreflightChecker) RunWithProfile(ctx context.Context, profile PreflightProfile, forceTriggers bool) error {
	p.logger.Info("Running preflight checks...")

	// Get all tables from graph
	tables := p.graph.AllNodes()

	// GA-P4-F3-T2: Table existence check
	if err := p.ValidateTablesExist(ctx, tables); err != nil {
		return err
	}

	// Validate configured PK columns exist and are explicitly defined.
	if err := p.ValidatePrimaryKeyColumns(ctx, tables); err != nil {
		return err
	}

	// Reject composite primary keys: GoArchive identifies and DELETES rows by a
	// single PK column, so a multi-column PK would over-match (review P1-1).
	if err := p.ValidateSingleColumnPrimaryKey(ctx, tables); err != nil {
		return err
	}

	rootTable := p.graph.Root
	if err := p.ValidateRootPKNumeric(ctx, rootTable, p.graph.GetPK(rootTable)); err != nil {
		return err
	}

	// GA-P4-F3-T1: Storage engine check
	if err := p.ValidateStorageEngine(ctx, tables); err != nil {
		return err
	}

	// Tracking-schema privileges are needed by every command that writes
	// archiver_job / per-job logs (archive, purge, copy-only), independent of
	// the data-table destination checks below.
	if p.destinationDB != nil && p.jobSchemaName != "" {
		if err := p.ValidateJobSchemaPermissions(ctx); err != nil {
			return err
		}
	}

	// Destination checks ensure copy target is safe before archive execution.
	if profile != PreflightProfileSourceOnly && p.destinationDB != nil && p.destinationDBName != "" {
		if err := p.ValidateDestinationTablesExist(ctx, tables); err != nil {
			return err
		}
		if err := p.ValidateDestinationSchemaCompatibility(ctx, tables); err != nil {
			return err
		}
		if err := p.ValidateDestinationWritePermissions(ctx, tables); err != nil {
			return err
		}
		if err := p.ValidateDestinationInsertTriggers(ctx, tables); err != nil {
			return err
		}
	}

	// GA-P4-F3-T3: FK index check
	if err := p.ValidateForeignKeyIndexes(ctx); err != nil {
		return err
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

	if profile == PreflightProfileFull || profile == PreflightProfileSourceOnly {
		if err := p.ValidateSourceDeletePermissions(ctx, tables); err != nil {
			return err
		}

		// GA-P4-F3-T4 & T5: DELETE trigger detection (with force flag)
		if err := p.ValidateTriggers(ctx, tables, forceTriggers); err != nil {
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
	return p.RunWithProfile(ctx, PreflightProfileFull, forceTriggers)
}

// ValidateRootPKNumeric ensures the root table primary key is an integer type.
func (p *PreflightChecker) ValidateRootPKNumeric(ctx context.Context, rootTable, rootPKColumn string) error {
	const query = `
		SELECT DATA_TYPE, COLUMN_TYPE
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = ?
		  AND COLUMN_NAME = ?
	`
	var dataType, columnType string
	err := p.db.QueryRowContext(ctx, query, rootTable, rootPKColumn).Scan(&dataType, &columnType)
	if err != nil {
		return fmt.Errorf("ROOT_PK_TYPE_LOOKUP: failed to look up data type for %s.%s: %w", rootTable, rootPKColumn, err)
	}
	if !isIntegerRootPKType(dataType) {
		return fmt.Errorf("ROOT_PK_TYPE_UNSUPPORTED: root table %q has primary key %q of type %q. GoArchive Community edition only supports integer root primary keys (TINYINT through BIGINT). See README 'Known Limits & Caution'", rootTable, rootPKColumn, dataType)
	}
	if p.graph != nil {
		p.graph.SetRootPKMeta(strings.ToLower(dataType), strings.Contains(strings.ToLower(columnType), "unsigned"))
	}
	return nil
}

func isIntegerRootPKType(dataType string) bool {
	switch strings.ToLower(dataType) {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint":
		return true
	default:
		return false
	}
}

// ValidateSingleColumnPrimaryKey rejects participating tables whose PRIMARY KEY
// spans more than one column. GoArchive discovers, copies, verifies, and DELETES
// rows by a single PK column (WHERE pk IN (...)); against a composite primary key
// that filter matches every row sharing the single column value, so a delete
// could remove rows that were never part of the archived subgraph — silent data
// loss (review P1-1). The destination PK is required to match the source by the
// schema-compatibility check, so inspecting the source is sufficient.
func (p *PreflightChecker) ValidateSingleColumnPrimaryKey(ctx context.Context, tables []string) error {
	p.logger.Debug("Checking primary key shape (single-column PRIMARY KEY matching configured PK)...")

	var compositeIssues []string
	var pkDefIssues []string
	for _, table := range tables {
		pkCols, err := p.primaryKeyColumns(ctx, table)
		if err != nil {
			return fmt.Errorf("COMPOSITE_PK_LOOKUP: failed to inspect primary key for %s: %w", table, err)
		}
		switch {
		case len(pkCols) > 1:
			// Composite PRIMARY KEY: delete-by-single-column would over-match.
			compositeIssues = append(compositeIssues, fmt.Sprintf("%s(%d-column PRIMARY KEY)", table, len(pkCols)))
		case len(pkCols) == 0:
			// No PRIMARY KEY at all: the configured primary_key is then almost
			// certainly a non-unique column, so delete-by-it would over-match (review 003).
			pkDefIssues = append(pkDefIssues, fmt.Sprintf("%s(no PRIMARY KEY)", table))
		case p.graph.HasPK(table) && pkCols[0] != p.graph.GetPK(table):
			// Configured primary_key is not the table's actual PRIMARY KEY column;
			// if it is non-unique, delete-by-it over-matches (review 003).
			pkDefIssues = append(pkDefIssues, fmt.Sprintf("%s(configured primary_key %q is not the PRIMARY KEY column %q)", table, p.graph.GetPK(table), pkCols[0]))
		}
	}

	// Composite PKs are the headline rejection (keeps the COMPOSITE_PK_CHECK
	// category and its E2E demo); report them first when present.
	if len(compositeIssues) > 0 {
		return &PreflightError{
			Check:   "COMPOSITE_PK_CHECK",
			Message: "Composite primary keys are not supported. GoArchive identifies and deletes rows by a single primary-key column; a multi-column PK would over-match and risk deleting rows outside the archived set. See README 'Known Limits & Caution'",
			Tables:  compositeIssues,
		}
	}
	if len(pkDefIssues) > 0 {
		return &PreflightError{
			Check:   "PRIMARY_KEY_CHECK",
			Message: "Every participating table must have a single-column PRIMARY KEY equal to the configured primary_key. GoArchive identifies and deletes rows by that column, so a missing or mismatched PRIMARY KEY can over-match and delete rows outside the archived set",
			Tables:  pkDefIssues,
		}
	}

	p.logger.Debugf("Primary key shape check PASSED (%d tables)", len(tables))
	return nil
}

// primaryKeyColumns returns the PRIMARY KEY column names of a source table, in
// key order. An empty slice means the table has no PRIMARY KEY.
func (p *PreflightChecker) primaryKeyColumns(ctx context.Context, table string) ([]string, error) {
	const query = `
		SELECT COLUMN_NAME
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = ?
		  AND TABLE_NAME = ?
		  AND INDEX_NAME = 'PRIMARY'
		ORDER BY SEQ_IN_INDEX`

	rows, err := p.db.QueryContext(ctx, query, p.sourceDBName, table)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			p.logger.Warnf("Failed to close rows: %v", cerr)
		}
	}()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return cols, nil
}

// ValidateTablesExist checks that all tables in the graph exist in the source database.
//
// GA-P4-F3-T2: Table existence check
func (p *PreflightChecker) ValidateTablesExist(ctx context.Context, tables []string) error {
	p.logger.Debug("Checking table existence...")

	const query = `
		SELECT TABLE_NAME
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ?`

	rows, err := p.db.QueryContext(ctx, query, p.sourceDBName)
	if err != nil {
		return fmt.Errorf("failed to query tables: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			p.logger.Warnf("Failed to close rows: %v", err)
		}
	}()

	existingTables := make(map[string]bool)
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return err
		}
		existingTables[tableName] = true
	}

	if err := rows.Err(); err != nil {
		return err
	}

	// Check for missing tables
	var missingTables []string
	for _, table := range tables {
		if !existingTables[table] {
			missingTables = append(missingTables, table)
		}
	}

	if len(missingTables) > 0 {
		return &PreflightError{
			Check:   "TABLE_EXISTENCE_CHECK",
			Message: "Tables not found in source database",
			Tables:  missingTables,
		}
	}

	p.logger.Debugf("Table existence check PASSED (%d tables)", len(tables))
	return nil
}

// ValidateStorageEngine checks that all tables use InnoDB storage engine.
//
// GA-P4-F3-T1: Storage engine check
func (p *PreflightChecker) ValidateStorageEngine(ctx context.Context, tables []string) error {
	p.logger.Debug("Checking storage engines...")
	if len(tables) == 0 {
		p.logger.Debug("Storage engine check skipped (no tables)")
		return nil
	}

	const query = `
		SELECT TABLE_NAME, ENGINE
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ?
		AND TABLE_NAME IN (?)`

	// Build placeholders for table list
	placeholders := make([]string, len(tables))
	args := make([]interface{}, len(tables)+1)
	args[0] = p.sourceDBName
	for i, table := range tables {
		placeholders[i] = "?"
		args[i+1] = table
	}

	fullQuery := strings.Replace(query, "(?)", "("+strings.Join(placeholders, ",")+")", 1)

	rows, err := p.db.QueryContext(ctx, fullQuery, args...)
	if err != nil {
		return fmt.Errorf("failed to query storage engines: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			p.logger.Warnf("Failed to close rows: %v", err)
		}
	}()

	var nonInnoDBTables []string
	for rows.Next() {
		var result StorageEngineResult
		if err := rows.Scan(&result.Table, &result.Engine); err != nil {
			return err
		}

		if result.Engine != "InnoDB" {
			nonInnoDBTables = append(nonInnoDBTables, fmt.Sprintf("%s(%s)", result.Table, result.Engine))
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	if len(nonInnoDBTables) > 0 {
		return &PreflightError{
			Check:   "STORAGE_ENGINE_CHECK",
			Message: "Only InnoDB tables are supported. Use ALTER TABLE to convert",
			Tables:  nonInnoDBTables,
		}
	}

	p.logger.Debugf("Storage engine check PASSED (all tables are InnoDB)")
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
func (p *PreflightChecker) ValidateTriggers(ctx context.Context, tables []string, forceTriggers bool) error {
	p.logger.Debug("Checking for DELETE triggers...")

	triggers, err := p.CheckDeleteTriggers(ctx, tables)
	if err != nil {
		return err
	}

	if len(triggers) == 0 {
		p.logger.Debug("DELETE trigger check PASSED (no triggers found)")
		return nil
	}

	// Collect unique table names
	tableMap := make(map[string]bool)
	var tableList []string
	for _, t := range triggers {
		if !tableMap[t.Table] {
			tableMap[t.Table] = true
			tableList = append(tableList, fmt.Sprintf("%s(%s)", t.Table, t.Trigger))
		}
	}

	msg := "DELETE triggers detected"

	// GA-P4-F3-T5: Allow override with --force-triggers flag
	if forceTriggers {
		p.logger.Warnf("%s (proceeding due to --force-triggers): %v", msg, tableList)
		return nil
	}

	return &PreflightError{
		Check:   "DELETE_TRIGGER_CHECK",
		Message: fmt.Sprintf("%s. Use --force-triggers to override (not recommended, triggers will fire during delete)", msg),
		Tables:  tableList,
	}
}

// CheckDeleteTriggers scans for DELETE triggers on the specified tables.
//
// GA-P4-F3-T4: DELETE trigger detection
func (p *PreflightChecker) CheckDeleteTriggers(ctx context.Context, tables []string) ([]TriggerCheckResult, error) {
	if len(tables) == 0 {
		return []TriggerCheckResult{}, nil
	}

	const query = `
		SELECT EVENT_OBJECT_TABLE, TRIGGER_NAME
		FROM information_schema.TRIGGERS
		WHERE EVENT_OBJECT_SCHEMA = ?
		AND EVENT_OBJECT_TABLE IN (?)
		AND EVENT_MANIPULATION = 'DELETE'`

	// Build placeholders for table list
	placeholders := make([]string, len(tables))
	args := make([]interface{}, len(tables)+1)
	args[0] = p.sourceDBName
	for i, table := range tables {
		placeholders[i] = "?"
		args[i+1] = table
	}

	fullQuery := strings.Replace(query, "(?)", "("+strings.Join(placeholders, ",")+")", 1)
	rows, err := p.db.QueryContext(ctx, fullQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query triggers: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			p.logger.Warnf("Failed to close rows: %v", err)
		}
	}()

	var results []TriggerCheckResult
	for rows.Next() {
		var r TriggerCheckResult
		if err := rows.Scan(&r.Table, &r.Trigger); err != nil {
			return nil, err
		}
		results = append(results, r)
	}

	return results, rows.Err()
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
			cascadeRules = append(cascadeRules, fmt.Sprintf("%s.%s->%s.%s",
				fk.Table, fk.Column, fk.ReferencedTable, fk.ReferencedColumn))
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

// ValidateDestinationTablesExist checks that all graph tables exist in destination DB.
func (p *PreflightChecker) ValidateDestinationTablesExist(ctx context.Context, tables []string) error {
	if p.destinationDB == nil {
		return fmt.Errorf("destination database not configured; call ConfigureDestination first")
	}
	p.logger.Debug("Checking destination table existence...")

	const query = `
		SELECT TABLE_NAME
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ?`

	rows, err := p.destinationDB.QueryContext(ctx, query, p.destinationDBName)
	if err != nil {
		return fmt.Errorf("failed to query destination tables: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			p.logger.Warnf("Failed to close rows: %v", err)
		}
	}()

	existingTables := make(map[string]bool)
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return err
		}
		existingTables[tableName] = true
	}
	if err := rows.Err(); err != nil {
		return err
	}

	var missingTables []string
	for _, table := range tables {
		if !existingTables[table] {
			missingTables = append(missingTables, table)
		}
	}

	if len(missingTables) > 0 {
		return &PreflightError{
			Check:   "DEST_TABLE_EXISTENCE_CHECK",
			Message: "Tables not found in destination database",
			Tables:  missingTables,
		}
	}

	p.logger.Debugf("Destination table existence check PASSED (%d tables)", len(tables))
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

// ValidateDestinationWritePermissions checks that the connected destination
// account (including active roles) holds INSERT at the global, schema, or
// per-table level for all graph tables.
func (p *PreflightChecker) ValidateDestinationWritePermissions(ctx context.Context, tables []string) error {
	if p.destinationDB == nil {
		return fmt.Errorf("destination database not configured; call ConfigureDestination first")
	}
	p.logger.Debug("Checking destination write permissions...")

	grantees, err := p.currentGrantees(ctx, p.destinationDB)
	if err != nil {
		return err
	}

	missing, err := p.tablesMissingPrivilege(ctx, p.destinationDB, grantees, p.destinationDBName, tables, "INSERT")
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		return &PreflightError{
			Check:   "DEST_WRITE_PERMISSION_CHECK",
			Message: fmt.Sprintf("Destination account %s lacks INSERT privilege on required tables", grantees[0]),
			Tables:  missing,
		}
	}

	p.logger.Debug("Destination write permission check PASSED")
	return nil
}

// schemaMissingPrivileges returns the subset of privs not held by any grantee
// at global (USER_PRIVILEGES) or schema (SCHEMA_PRIVILEGES) level for schema.
// No per-table fallback — the per-job tracking tables don't exist yet at
// preflight time.
func (p *PreflightChecker) schemaMissingPrivileges(ctx context.Context, db *sql.DB, grantees []string, schema string, privs []string) ([]string, error) {
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(grantees)), ",")
	var missing []string
	for _, priv := range privs {
		gArgs := make([]interface{}, 0, len(grantees)+1)
		for _, g := range grantees {
			gArgs = append(gArgs, g)
		}
		gArgs = append(gArgs, priv)

		var count int
		globalQuery := fmt.Sprintf(`SELECT COUNT(*) FROM information_schema.USER_PRIVILEGES
			WHERE GRANTEE IN (%s) AND PRIVILEGE_TYPE = ?`, placeholders)
		if err := db.QueryRowContext(ctx, globalQuery, gArgs...).Scan(&count); err != nil {
			return nil, fmt.Errorf("failed to check global %s: %w", priv, err)
		}
		if count > 0 {
			continue
		}
		sArgs := append(append([]interface{}{}, gArgs...), schema)
		schemaQuery := fmt.Sprintf(`SELECT COUNT(*) FROM information_schema.SCHEMA_PRIVILEGES
			WHERE GRANTEE IN (%s) AND PRIVILEGE_TYPE = ? AND TABLE_SCHEMA = ?`, placeholders)
		if err := db.QueryRowContext(ctx, schemaQuery, sArgs...).Scan(&count); err != nil {
			return nil, fmt.Errorf("failed to check schema %s: %w", priv, err)
		}
		if count == 0 {
			missing = append(missing, priv)
		}
	}
	return missing, nil
}

// ValidateJobSchemaPermissions checks the destination account holds CREATE +
// SELECT/INSERT/UPDATE on the tracking schema. CREATE is required at runtime
// because per-job log tables are created on the fly. DELETE/DROP are optional
// (DBA cleanup only) and intentionally not required.
func (p *PreflightChecker) ValidateJobSchemaPermissions(ctx context.Context) error {
	if p.destinationDB == nil {
		return fmt.Errorf("destination database not configured; call ConfigureDestination first")
	}
	grantees, err := p.currentGrantees(ctx, p.destinationDB)
	if err != nil {
		return err
	}
	missing, err := p.schemaMissingPrivileges(ctx, p.destinationDB, grantees, p.jobSchemaName,
		[]string{"CREATE", "SELECT", "INSERT", "UPDATE"})
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		grant := fmt.Sprintf("GRANT %s ON `%s`.* TO <user>", strings.Join(missing, ", "), p.jobSchemaName)
		hint := grant
		for _, p2 := range missing {
			if p2 == "CREATE" {
				hint = fmt.Sprintf("CREATE DATABASE `%s`; %s", p.jobSchemaName, grant)
				break
			}
		}
		return &PreflightError{
			Check:   "JOB_SCHEMA_PERMISSION_CHECK",
			Message: fmt.Sprintf("destination account %s lacks %s on tracking schema %q (DBA must: %s)", grantees[0], strings.Join(missing, ", "), p.jobSchemaName, hint),
		}
	}
	p.logger.Debug("Job schema permission check PASSED")
	return nil
}

// tablesMissingPrivilege returns the tables for which none of the grantees
// holds the privilege at global, schema, or table level. GRANT ALL is
// expanded into individual privilege rows by MySQL, so matching the specific
// privilege type covers it.
func (p *PreflightChecker) tablesMissingPrivilege(ctx context.Context, db *sql.DB, grantees []string, dbName string, tables []string, privilege string) ([]string, error) {
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(grantees)), ",")
	granteeArgs := make([]interface{}, 0, len(grantees)+1)
	for _, g := range grantees {
		granteeArgs = append(granteeArgs, g)
	}
	granteeArgs = append(granteeArgs, privilege)

	var count int
	globalQuery := fmt.Sprintf(`SELECT COUNT(*) FROM information_schema.USER_PRIVILEGES
		WHERE GRANTEE IN (%s) AND PRIVILEGE_TYPE = ?`, placeholders)
	if err := db.QueryRowContext(ctx, globalQuery, granteeArgs...).Scan(&count); err != nil {
		return nil, fmt.Errorf("failed to check global privileges: %w", err)
	}
	if count > 0 {
		return nil, nil
	}

	schemaArgs := make([]interface{}, 0, len(grantees)+2)
	schemaArgs = append(schemaArgs, granteeArgs...)
	schemaArgs = append(schemaArgs, dbName)
	schemaQuery := fmt.Sprintf(`SELECT COUNT(*) FROM information_schema.SCHEMA_PRIVILEGES
		WHERE GRANTEE IN (%s) AND PRIVILEGE_TYPE = ? AND TABLE_SCHEMA = ?`, placeholders)
	if err := db.QueryRowContext(ctx, schemaQuery, schemaArgs...).Scan(&count); err != nil {
		return nil, fmt.Errorf("failed to check schema privileges: %w", err)
	}
	if count > 0 {
		return nil, nil
	}

	tableQuery := fmt.Sprintf(`SELECT COUNT(*) FROM information_schema.TABLE_PRIVILEGES
		WHERE GRANTEE IN (%s) AND PRIVILEGE_TYPE = ? AND TABLE_SCHEMA = ? AND TABLE_NAME = ?`, placeholders)
	var missing []string
	for _, table := range tables {
		tableArgs := make([]interface{}, 0, len(grantees)+3)
		tableArgs = append(tableArgs, granteeArgs...)
		tableArgs = append(tableArgs, dbName, table)
		if err := db.QueryRowContext(ctx, tableQuery, tableArgs...).Scan(&count); err != nil {
			return nil, fmt.Errorf("failed to check table privileges for %s: %w", table, err)
		}
		if count == 0 {
			missing = append(missing, table)
		}
	}
	return missing, nil
}

// ValidateSourceDeletePermissions checks the source account can DELETE from
// all graph tables. Without it, archive fails only after copy has committed.
func (p *PreflightChecker) ValidateSourceDeletePermissions(ctx context.Context, tables []string) error {
	p.logger.Debug("Checking source delete permissions...")

	grantees, err := p.currentGrantees(ctx, p.db)
	if err != nil {
		return err
	}
	missing, err := p.tablesMissingPrivilege(ctx, p.db, grantees, p.sourceDBName, tables, "DELETE")
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		return &PreflightError{
			Check:   "SOURCE_DELETE_PERMISSION_CHECK",
			Message: fmt.Sprintf("Source account %s lacks DELETE privilege on required tables", grantees[0]),
			Tables:  missing,
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

// ValidatePrimaryKeyColumns checks that each table has an explicitly configured
// PK and that the configured PK column exists in the source table with the
// EXACT same name, including letter case.
//
// Column names are matched case-sensitively on purpose. MySQL's
// information_schema.COLUMNS collates COLUMN_NAME case-insensitively
// (utf8mb3_tolower_ci), so a plain `COLUMN_NAME = ?` lookup would treat
// "LOG_ID", "log_id", and "Log_Id" as equal and let a mis-cased primary_key
// slip through here — only to be caught later by PRIMARY_KEY_CHECK, whose
// data-loss-flavored "over-match and delete rows" wording is confusing for what
// is really just a letter-case typo. We therefore fetch the real column name
// (which carries its true case) and compare it in Go, reporting a mere casing
// difference with its own clear message (PK_COLUMN_CASE_CHECK).
func (p *PreflightChecker) ValidatePrimaryKeyColumns(ctx context.Context, tables []string) error {
	p.logger.Debug("Checking primary key column definitions...")

	// COLUMN_NAME = ? matches case-insensitively (see doc comment above); the
	// SELECT returns the column's actual stored name so we can compare case in Go.
	const query = `
		SELECT COLUMN_NAME
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ?
		AND TABLE_NAME = ?
		AND COLUMN_NAME = ?`

	var missing []string
	var caseIssues []string
	for _, table := range tables {
		if !p.graph.HasPK(table) {
			missing = append(missing, fmt.Sprintf("%s(primary key must be explicitly configured; implicit default to 'id' is not allowed)", table))
			continue
		}

		pkColumn := p.graph.GetPK(table)
		var actualColumn string
		err := p.db.QueryRowContext(ctx, query, p.sourceDBName, table, pkColumn).Scan(&actualColumn)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			// No column matches even case-insensitively: it genuinely does not exist.
			missing = append(missing, fmt.Sprintf("%s(%s)", table, pkColumn))
		case err != nil:
			return fmt.Errorf("failed to validate primary key column for %s.%s: %w", table, pkColumn, err)
		case actualColumn != pkColumn:
			// A column exists but its name differs only in letter case. Column
			// names are case-sensitive here; the configured value must match exactly.
			caseIssues = append(caseIssues, fmt.Sprintf("%s(configured primary_key %q but the column is named %q)", table, pkColumn, actualColumn))
		}
	}

	// Report the case mismatch first: it is the subtle, easy-to-miss failure and
	// its guidance (fix the casing) is different from a truly-missing column.
	if len(caseIssues) > 0 {
		return &PreflightError{
			Check:   "PK_COLUMN_CASE_CHECK",
			Message: "Configured primary_key does not match the database column name exactly. Column names are case-sensitive (e.g. log_id, LOG_ID and Log_Id are different) — set primary_key to the exact case used in the database schema",
			Tables:  caseIssues,
		}
	}
	if len(missing) > 0 {
		return &PreflightError{
			Check:   "PK_COLUMN_CHECK",
			Message: "Configured primary key columns not found in the source table",
			Tables:  missing,
		}
	}

	p.logger.Debugf("Primary key column check PASSED (%d tables)", len(tables))
	return nil
}

// ValidateDestinationInsertTriggers checks for INSERT triggers on destination tables.
func (p *PreflightChecker) ValidateDestinationInsertTriggers(ctx context.Context, tables []string) error {
	if p.destinationDB == nil {
		return fmt.Errorf("destination database not configured; call ConfigureDestination first")
	}
	p.logger.Debug("Checking destination INSERT triggers...")

	triggers, err := p.CheckInsertTriggers(ctx, tables)
	if err != nil {
		return err
	}

	if len(triggers) == 0 {
		p.logger.Debug("Destination INSERT trigger check PASSED (no triggers found)")
		return nil
	}

	tableMap := make(map[string]bool)
	var tableList []string
	for _, t := range triggers {
		if !tableMap[t.Table] {
			tableMap[t.Table] = true
			tableList = append(tableList, fmt.Sprintf("%s(%s)", t.Table, t.Trigger))
		}
	}

	return &PreflightError{
		Check:   "DEST_INSERT_TRIGGER_CHECK",
		Message: "Destination INSERT triggers detected. Disable triggers before running archive copy operations",
		Tables:  tableList,
	}
}

// CheckInsertTriggers scans for INSERT triggers on destination tables.
func (p *PreflightChecker) CheckInsertTriggers(ctx context.Context, tables []string) ([]TriggerCheckResult, error) {
	if p.destinationDB == nil {
		return nil, fmt.Errorf("destination database not configured; call ConfigureDestination first")
	}
	if len(tables) == 0 {
		return []TriggerCheckResult{}, nil
	}

	const query = `
		SELECT EVENT_OBJECT_TABLE, TRIGGER_NAME
		FROM information_schema.TRIGGERS
		WHERE EVENT_OBJECT_SCHEMA = ?
		AND EVENT_OBJECT_TABLE IN (?)
		AND EVENT_MANIPULATION = 'INSERT'`

	placeholders := make([]string, len(tables))
	args := make([]interface{}, len(tables)+1)
	args[0] = p.destinationDBName
	for i, table := range tables {
		placeholders[i] = "?"
		args[i+1] = table
	}

	fullQuery := strings.Replace(query, "(?)", "("+strings.Join(placeholders, ",")+")", 1)
	rows, err := p.destinationDB.QueryContext(ctx, fullQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query destination triggers: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			p.logger.Warnf("Failed to close rows: %v", err)
		}
	}()

	var results []TriggerCheckResult
	for rows.Next() {
		var r TriggerCheckResult
		if err := rows.Scan(&r.Table, &r.Trigger); err != nil {
			return nil, err
		}
		results = append(results, r)
	}

	return results, rows.Err()
}

// SetLogger sets a custom logger for the preflight checker.
func (p *PreflightChecker) SetLogger(log *logger.Logger) {
	p.logger = log
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
