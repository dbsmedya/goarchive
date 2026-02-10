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
	defer rows.Close()

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
	defer rows.Close()

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
	defer rows.Close()

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
	defer rows.Close()

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

// SetLogger sets a custom logger for the preflight checker.
func (p *PreflightChecker) SetLogger(log *logger.Logger) {
	p.logger = log
}
