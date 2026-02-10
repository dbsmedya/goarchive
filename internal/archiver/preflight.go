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
	Details map[string]string
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
	db           *sql.DB
	sourceDBName string
	graph        *graph.Graph
	logger       *logger.Logger
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
		db:           db,
		sourceDBName: sourceDBName,
		graph:        g,
		logger:       log,
	}, nil
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

	// GA-P4-F3-T1: Storage engine check
	if err := p.ValidateStorageEngine(ctx, tables); err != nil {
		return err
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
	tables := p.graph.AllNodes()

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
		AND kcu.TABLE_NAME IN (?)`

	// Build placeholders
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

		// Check if column has an index
		fk.Indexed, err = p.isColumnIndexed(ctx, fk.Table, fk.Column)
		if err != nil {
			return nil, fmt.Errorf("failed to check index for %s.%s: %w", fk.Table, fk.Column, err)
		}

		results = append(results, fk)
	}

	return results, rows.Err()
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
// This prevents delete failures when a table outside the graph has an FK
// constraint to a table inside the graph with ON DELETE RESTRICT.
func (p *PreflightChecker) ValidateForeignKeyCoverage(ctx context.Context) error {
	p.logger.Debug("Checking foreign key coverage...")

	// Get all tables in the graph
	graphTables := p.graph.AllNodes()
	graphTableSet := make(map[string]bool)
	for _, t := range graphTables {
		graphTableSet[t] = true
	}

	// Query all FK constraints that reference tables in our graph
	query := `
		SELECT 
			kcu.table_name,
			kcu.constraint_name,
			kcu.column_name,
			kcu.referenced_table_name,
			kcu.referenced_column_name,
			rc.delete_rule
		FROM information_schema.referential_constraints rc
		JOIN information_schema.key_column_usage kcu 
			ON rc.constraint_name = kcu.constraint_name
			AND rc.constraint_schema = kcu.constraint_schema
		WHERE rc.constraint_schema = ?
			AND kcu.referenced_table_name IN (` + p.placeholders(len(graphTables)) + `)
		ORDER BY kcu.referenced_table_name, kcu.table_name
	`

	args := make([]interface{}, 0, len(graphTables)+1)
	args = append(args, p.sourceDBName)
	for _, t := range graphTables {
		args = append(args, t)
	}

	rows, err := p.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to query FK coverage: %w", err)
	}
	defer func() { _ = rows.Close() }()

	type uncoveredFK struct {
		Table           string
		Constraint      string
		Column          string
		ReferencedTable string
		OnDelete        string
	}
	var uncovered []uncoveredFK

	for rows.Next() {
		var fk uncoveredFK
		if err := rows.Scan(&fk.Table, &fk.Constraint, &fk.Column, &fk.ReferencedTable, new(string), &fk.OnDelete); err != nil {
			return fmt.Errorf("failed to scan FK row: %w", err)
		}
		// If the referencing table is NOT in our graph, it's uncovered
		if !graphTableSet[fk.Table] {
			uncovered = append(uncovered, fk)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating FK rows: %w", err)
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
				childTables = append(childTables, fk.Table)
			}
			messages = append(messages, fmt.Sprintf("  - %s is referenced by: %v", refTable, childTables))
		}

		return &PreflightError{
			Check:   "FK_COVERAGE_CHECK",
			Message: fmt.Sprintf("Foreign key constraints not covered by relations:\n%s", strings.Join(messages, "\n")),
			Details: map[string]string{
				"uncovered_count": fmt.Sprintf("%d", len(uncovered)),
				"suggestion":      "Add these tables to your relations or use 'purge' mode to skip archiving",
			},
		}
	}

	p.logger.Debug("Foreign key coverage check complete (all FKs covered)")
	return nil
}

// placeholders generates SQL placeholders for IN clause.
func (p *PreflightChecker) placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.Repeat("?,", n-1) + "?"
}

// SetLogger sets a custom logger for the preflight checker.
func (p *PreflightChecker) SetLogger(log *logger.Logger) {
	p.logger = log
}
