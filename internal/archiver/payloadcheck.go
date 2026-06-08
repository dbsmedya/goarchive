package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/dbsmedya/goarchive/internal/sqlutil"
)

// maxPreparedPlaceholders is the MySQL prepared-statement parameter ceiling.
const maxPreparedPlaceholders = 65535

// checkPlaceholderLimit verifies that one batch_size INSERT for a table with
// columnCount columns stays under the prepared-statement placeholder ceiling.
func checkPlaceholderLimit(table string, columnCount, batchSize int) error {
	total := columnCount * batchSize
	if total >= maxPreparedPlaceholders {
		return fmt.Errorf(
			"table %q: a batch_size of %d × %d columns = %d placeholders exceeds the MySQL limit of %d; lower batch_size to at most %d",
			table, batchSize, columnCount, total, maxPreparedPlaceholders,
			(maxPreparedPlaceholders-1)/columnCount)
	}
	return nil
}

// PayloadValidator validates that batch_size copy chunks fit the destination's
// limits. Used by dry-run only.
type PayloadValidator struct {
	source    *sql.DB
	dest      *sql.DB
	graph     *graph.Graph
	jobCfg    *config.JobConfig
	safetyCfg config.SafetyConfig
	batchSize int
	logger    *logger.Logger
}

// NewPayloadValidator creates a new PayloadValidator.
func NewPayloadValidator(source, dest *sql.DB, g *graph.Graph, jobCfg *config.JobConfig, safetyCfg config.SafetyConfig, batchSize int, log *logger.Logger) *PayloadValidator {
	if log == nil {
		log = logger.NewDefault()
	}
	return &PayloadValidator{source, dest, g, jobCfg, safetyCfg, batchSize, log}
}

// Validate runs, per table in copy order: an exact placeholder check (always)
// and a measured rolled-back INSERT against the destination (when rows exist).
// It fails fast on the first table that exceeds a limit.
func (p *PayloadValidator) Validate(ctx context.Context) error {
	copyOrder, err := p.graph.CopyOrder()
	if err != nil {
		return fmt.Errorf("failed to get copy order: %w", err)
	}

	maxPacket, err := p.maxAllowedPacket(ctx)
	if err != nil {
		return fmt.Errorf("failed to read destination max_allowed_packet: %w", err)
	}
	p.logger.Infof("Destination max_allowed_packet = %d bytes", maxPacket)

	for _, table := range copyOrder {
		columns, err := p.tableColumns(ctx, table)
		if err != nil {
			return fmt.Errorf("table %q: %w", table, err)
		}
		// Exact check (valid even for empty tables).
		if err := checkPlaceholderLimit(table, len(columns), p.batchSize); err != nil {
			return err
		}

		sampled, rowBytes, err := p.measureSample(ctx, table, columns)
		if err != nil {
			return fmt.Errorf("table %q: %w", table, err)
		}
		if sampled == 0 {
			p.logger.Infof("table %q: no rows to sample — placeholder check passed; packet size not measured (no data)", table)
			continue
		}
		// Project to a full batch_size chunk when the sample was smaller.
		projected := rowBytes
		if sampled < p.batchSize {
			projected = rowBytes * p.batchSize / sampled
			p.logger.Infof("table %q: only %d rows available; projected full-chunk payload ≈ %d bytes", table, sampled, projected)
		}
		if int64(projected) >= maxPacket {
			return fmt.Errorf(
				"table %q: a batch_size of %d would build an INSERT of ≈%d bytes, exceeding destination max_allowed_packet=%d; lower batch_size",
				table, p.batchSize, projected, maxPacket)
		}
		if p.jobCfg.RootTable != table {
			p.logger.Infof("table %q: packet check is APPROXIMATE (child sample uses arbitrary rows, not discovery-resolved rows)", table)
		}
	}
	return nil
}

func (p *PayloadValidator) maxAllowedPacket(ctx context.Context) (int64, error) {
	var name, valStr string
	row := p.dest.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'max_allowed_packet'")
	if err := row.Scan(&name, &valStr); err != nil {
		return 0, err
	}
	val, err := strconv.ParseInt(valStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unexpected max_allowed_packet value %q: %w", valStr, err)
	}
	return val, nil
}

func (p *PayloadValidator) tableColumns(ctx context.Context, table string) ([]string, error) {
	rows, err := p.source.QueryContext(ctx,
		fmt.Sprintf("SELECT * FROM %s LIMIT 0", sqlutil.QuoteIdentifier(table)))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return rows.Columns()
}

// measureSample fetches up to batchSize rows, builds the real INSERT, executes
// it inside a destination transaction, then rolls back. Returns (#rows sampled,
// approximate INSERT byte size, error).
func (p *PayloadValidator) measureSample(ctx context.Context, table string, columns []string) (int, int, error) {
	pkColumn := p.graph.GetPK(table)
	var query string
	if p.jobCfg.RootTable == table && strings.TrimSpace(p.jobCfg.Where) != "" {
		// Order ASC to mirror the real copy fetch (batch.go uses ORDER BY pk ASC):
		// the sample becomes exactly the first batch the archive would process. ASC
		// is also the only safe direction here — archive WHERE clauses select OLD
		// rows, which in an append-only table have the LOWEST pk values. ORDER BY pk
		// DESC would start at the newest rows (all failing the WHERE) and scan
		// backward across the whole table before reaching qualifying rows, which on
		// a large production table looks like an indefinite hang.
		query = fmt.Sprintf("SELECT * FROM %s WHERE (%s) ORDER BY %s ASC LIMIT %d",
			sqlutil.QuoteIdentifier(table), p.jobCfg.Where,
			sqlutil.QuoteIdentifier(pkColumn), p.batchSize)
	} else {
		query = fmt.Sprintf("SELECT * FROM %s LIMIT %d",
			sqlutil.QuoteIdentifier(table), p.batchSize)
	}

	rows, err := p.source.QueryContext(ctx, query)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = rows.Close() }()

	values := make([]interface{}, 0, len(columns)*p.batchSize)
	count := 0
	for rows.Next() {
		rowVals := make([]interface{}, len(columns))
		ptrs := make([]interface{}, len(columns))
		for i := range rowVals {
			ptrs[i] = &rowVals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return 0, 0, err
		}
		values = append(values, rowVals...)
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, 0, err
	}
	if count == 0 {
		return 0, 0, nil
	}

	// Build the same INSERT IGNORE the real copy would send.
	insert := buildInsertIgnoreBatchQueryStandalone(table, columns, count)

	// Check out a dedicated connection so the session-scoped SET FOREIGN_KEY_CHECKS
	// = 0 below is contained to this conn and reset before it returns to the pool.
	// SET is NOT transactional in MySQL, so tx.Rollback() does not restore it; a
	// pooled connection would otherwise be handed back with FK enforcement silently
	// disabled. Mirrors CopyPhase's dedicated-conn + explicit-reset pattern.
	conn, err := p.dest.Conn(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get destination connection: %w", err)
	}
	fkReset := false
	defer func() {
		// Always re-enable FK checks before returning the connection to the pool,
		// even on error/rollback. Uses context.Background() so the reset still runs
		// if ctx was cancelled.
		if !fkReset {
			if _, resetErr := conn.ExecContext(context.Background(),
				"SET FOREIGN_KEY_CHECKS = 1"); resetErr != nil {
				p.logger.Errorf("Failed to reset FOREIGN_KEY_CHECKS on destination connection: %v", resetErr)
			}
		}
		if closeErr := conn.Close(); closeErr != nil {
			p.logger.Warnf("Failed to close destination connection: %v", closeErr)
		}
	}()

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = tx.Rollback() }() // always roll back — validation only

	// ALWAYS disable FK checks for the validation insert, regardless of
	// safety.disable_foreign_key_checks. Child samples are arbitrary rows whose
	// parents are generally absent in the destination, so FK enforcement would
	// false-fail an otherwise size-safe batch_size. This check validates PAYLOAD
	// limits (placeholder count, max_allowed_packet), not referential integrity.
	if _, err := tx.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		return 0, 0, err
	}
	if _, err := tx.ExecContext(ctx, insert, values...); err != nil {
		return 0, 0, fmt.Errorf("destination rejected sample INSERT (likely max_allowed_packet or a schema mismatch): %w", err)
	}

	// Re-enable FK checks on this connection before it returns to the pool, inside
	// the linear statement stream (belt-and-suspenders with the defer above). SET
	// survives the rollback, so the connection is restored regardless.
	if _, err := tx.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 1"); err != nil {
		return 0, 0, fmt.Errorf("failed to reset FOREIGN_KEY_CHECKS: %w", err)
	}
	fkReset = true

	// Approximate the byte size: query text + a coarse per-value estimate.
	approx := len(insert)
	for _, v := range values {
		approx += approxValueBytes(v)
	}
	return count, approx, nil
}

func approxValueBytes(v interface{}) int {
	switch t := v.(type) {
	case nil:
		return 4
	case []byte:
		return len(t) + 2
	case string:
		return len(t) + 2
	default:
		return 8
	}
}

// buildInsertIgnoreBatchQueryStandalone mirrors CopyPhase.buildInsertIgnoreBatchQuery
// without needing a CopyPhase instance.
func buildInsertIgnoreBatchQueryStandalone(table string, columns []string, rowCount int) string {
	quoted := make([]string, len(columns))
	for i, c := range columns {
		quoted[i] = sqlutil.QuoteIdentifier(c)
	}
	ph := make([]string, len(columns))
	for i := range ph {
		ph[i] = "?"
	}
	tuple := "(" + strings.Join(ph, ", ") + ")"
	tuples := make([]string, rowCount)
	for i := range tuples {
		tuples[i] = tuple
	}
	return fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES %s",
		sqlutil.QuoteIdentifier(table), strings.Join(quoted, ", "), strings.Join(tuples, ", "))
}
