// Package archiver provides preflight safety checks for GoArchive.
package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

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
	Table            string
	ConstraintName   string
	Column           string
	ReferencedTable  string
	ReferencedColumn string
	OnDelete         string // CASCADE, SET NULL, RESTRICT, etc.
	OnUpdate         string
	Indexed          bool // Whether the FK column has an index
}

// PreflightChecker performs safety checks before archiving.
//
// GA-P4-F3: Preflight Checks
type PreflightChecker struct {
	db                *sql.DB
	sourceDBName      string
	destinationDB     *sql.DB
	destinationDBName string
	graph             *graph.Graph
	logger            *logger.Logger
	fkCache           []ForeignKeyResult
	fkCacheLoaded     bool
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
func (p *PreflightChecker) ConfigureDestination(db *sql.DB, destinationDBName string) error {
	if db == nil {
		return fmt.Errorf("destination database is nil")
	}
	if destinationDBName == "" {
		return fmt.Errorf("destination database name is required")
	}
	p.destinationDB = db
	p.destinationDBName = destinationDBName
	return nil
}

// RunAllChecks runs all preflight checks.
//
// GA-P4-F3-T7: Validate command implementation
func (p *PreflightChecker) RunAllChecks(ctx context.Context, forceTriggers bool) error {
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

	// GA-P4-F3-T1: Storage engine check
	if err := p.ValidateStorageEngine(ctx, tables); err != nil {
		return err
	}

	// Destination checks ensure copy target is safe before archive execution.
	if p.destinationDB != nil && p.destinationDBName != "" {
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

	// GA-P4-F3-T4 & T5: DELETE trigger detection (with force flag)
	if err := p.ValidateTriggers(ctx, tables, forceTriggers); err != nil {
		return err
	}

	// GA-P4-F3-T6: CASCADE rule warning
	if err := p.WarnCascadeRules(ctx); err != nil {
		return err
	}

	p.logger.Info("All preflight checks PASSED")
	return nil
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

	var unindexedFKs []string
	for _, fk := range fks {
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
			kcu.TABLE_NAME,
			kcu.CONSTRAINT_NAME,
			kcu.COLUMN_NAME,
			kcu.REFERENCED_TABLE_NAME,
			kcu.REFERENCED_COLUMN_NAME,
			rc.DELETE_RULE,
			rc.UPDATE_RULE
		FROM information_schema.KEY_COLUMN_USAGE kcu
		JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
			ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
			AND kcu.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
		WHERE kcu.TABLE_SCHEMA = ?
		AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
		AND (kcu.TABLE_NAME IN (?) OR kcu.REFERENCED_TABLE_NAME IN (?))`

	// Build placeholders
	placeholders := make([]string, len(tables))
	args := make([]interface{}, 0, (len(tables)*2)+1)
	args = append(args, p.sourceDBName)
	for i := range tables {
		placeholders[i] = "?"
	}
	for _, table := range tables {
		args = append(args, table)
	}
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
		if err := rows.Scan(&fk.Table, &fk.ConstraintName, &fk.Column, &fk.ReferencedTable, &fk.ReferencedColumn, &fk.OnDelete, &fk.OnUpdate); err != nil {
			return nil, err
		}

		// FK index checks apply only to tables in the graph.
		if graphTableSet[fk.Table] {
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
		if graphTableSet[fk.ReferencedTable] && !graphTableSet[fk.Table] {
			uncovered = append(uncovered, uncoveredFK{
				Table:           fk.Table,
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
		if !graphTableSet[fk.Table] || !graphTableSet[fk.ReferencedTable] {
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

// ValidateDestinationSchemaCompatibility ensures source/destination table columns match exactly.
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

		for i := range sourceColumns {
			s := sourceColumns[i]
			d := destColumns[i]
			if s.ColumnName != d.ColumnName || s.ColumnType != d.ColumnType || s.IsNullable != d.IsNullable || s.ColumnKey != d.ColumnKey || s.Extra != d.Extra {
				incompatible = append(incompatible, fmt.Sprintf("%s(position %d: source=%s %s nullable=%s key=%s extra=%s, destination=%s %s nullable=%s key=%s extra=%s)",
					table, s.OrdinalPosition,
					s.ColumnName, s.ColumnType, s.IsNullable, s.ColumnKey, s.Extra,
					d.ColumnName, d.ColumnType, d.IsNullable, d.ColumnKey, d.Extra))
				break
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

// ValidateDestinationWritePermissions checks that destination grants INSERT privileges for all graph tables.
func (p *PreflightChecker) ValidateDestinationWritePermissions(ctx context.Context, tables []string) error {
	if p.destinationDB == nil {
		return fmt.Errorf("destination database not configured; call ConfigureDestination first")
	}
	p.logger.Debug("Checking destination write permissions...")

	const schemaPrivilegeQuery = `
		SELECT COUNT(*)
		FROM information_schema.SCHEMA_PRIVILEGES
		WHERE TABLE_SCHEMA = ?
		AND PRIVILEGE_TYPE IN ('INSERT', 'ALL PRIVILEGES')`

	var schemaPrivilegeCount int
	if err := p.destinationDB.QueryRowContext(ctx, schemaPrivilegeQuery, p.destinationDBName).Scan(&schemaPrivilegeCount); err != nil {
		return fmt.Errorf("failed to check destination schema privileges: %w", err)
	}
	if schemaPrivilegeCount > 0 {
		p.logger.Debug("Destination write permission check PASSED (schema-level privilege)")
		return nil
	}

	const tablePrivilegeQuery = `
		SELECT COUNT(*)
		FROM information_schema.TABLE_PRIVILEGES
		WHERE TABLE_SCHEMA = ?
		AND TABLE_NAME = ?
		AND PRIVILEGE_TYPE IN ('INSERT', 'ALL PRIVILEGES')`

	var missing []string
	for _, table := range tables {
		var count int
		if err := p.destinationDB.QueryRowContext(ctx, tablePrivilegeQuery, p.destinationDBName, table).Scan(&count); err != nil {
			return fmt.Errorf("failed to check destination table privileges for %s: %w", table, err)
		}
		if count == 0 {
			missing = append(missing, table)
		}
	}

	if len(missing) > 0 {
		return &PreflightError{
			Check:   "DEST_WRITE_PERMISSION_CHECK",
			Message: "Destination user lacks INSERT privilege on required tables",
			Tables:  missing,
		}
	}

	p.logger.Debug("Destination write permission check PASSED (table-level privileges)")
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
			EXTRA
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
		if err := rows.Scan(&col.OrdinalPosition, &col.ColumnName, &col.ColumnType, &col.IsNullable, &col.ColumnKey, &col.Extra); err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return columns, nil
}

// ValidatePrimaryKeyColumns checks that each table has an explicitly configured PK and the PK column exists.
func (p *PreflightChecker) ValidatePrimaryKeyColumns(ctx context.Context, tables []string) error {
	p.logger.Debug("Checking primary key column definitions...")

	const query = `
		SELECT COUNT(*)
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ?
		AND TABLE_NAME = ?
		AND COLUMN_NAME = ?`

	var issues []string
	for _, table := range tables {
		if !p.graph.HasPK(table) {
			issues = append(issues, fmt.Sprintf("%s(primary key must be explicitly configured; implicit default to 'id' is not allowed)", table))
			continue
		}

		pkColumn := p.graph.GetPK(table)
		var count int
		if err := p.db.QueryRowContext(ctx, query, p.sourceDBName, table, pkColumn).Scan(&count); err != nil {
			return fmt.Errorf("failed to validate primary key column for %s.%s: %w", table, pkColumn, err)
		}
		if count == 0 {
			issues = append(issues, fmt.Sprintf("%s(%s)", table, pkColumn))
		}
	}

	if len(issues) > 0 {
		return &PreflightError{
			Check:   "PK_COLUMN_CHECK",
			Message: "Configured primary key columns not found",
			Tables:  issues,
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
