// Package verifier provides data integrity verification for GoArchive.
package verifier

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/dbsmedya/goarchive/internal/sqlutil"
	"github.com/dbsmedya/goarchive/internal/types"
)

// VerificationMethod defines how to verify data integrity.
type VerificationMethod string

const (
	// MethodCount uses simple row count comparison (fast)
	MethodCount VerificationMethod = "count"
	// MethodSHA256 uses SHA256 hash of all rows (slower but more thorough)
	MethodSHA256 VerificationMethod = "sha256"
	// MethodSkip skips verification entirely
	MethodSkip VerificationMethod = "skip"
)

// VerifyResult holds verification results for a single table.
//
// GA-P4-F1-T1: Row count verification
// GA-P4-F1-T2: SHA256 hash verification
// GA-P4-F1-T6: Verification stats
type VerifyResult struct {
	Table        string
	Method       VerificationMethod
	SourceCount  int64
	DestCount    int64
	SourceHash   string
	DestHash     string
	Match        bool
	ErrorMessage string
}

// VerifyStats contains overall verification statistics.
//
// GA-P4-F1-T6: Verification stats
type VerifyStats struct {
	TablesVerified int
	TablesPassed   int
	TablesFailed   int
	TotalRows      int64
	Method         VerificationMethod
}

// Verifier handles data integrity verification between source and destination databases.
//
// GA-P4-F1: Verification Implementation
type Verifier struct {
	source      *sql.DB
	destination *sql.DB
	graph       *graph.Graph
	method      VerificationMethod
	chunkSize   int // For chunked SHA256 (GA-P4-F1-T3)
	logger      *logger.Logger
}

// NewVerifier creates a new verifier for data integrity checks.
//
// GA-P4-F1-T4: Verification method selection
// GA-P4-F1-T7: Skip verification option (method = MethodSkip)
func NewVerifier(source, destination *sql.DB, g *graph.Graph, method VerificationMethod, log *logger.Logger) (*Verifier, error) {
	if source == nil {
		return nil, fmt.Errorf("source database is nil")
	}
	if destination == nil {
		return nil, fmt.Errorf("destination database is nil")
	}
	if g == nil {
		return nil, fmt.Errorf("graph is nil")
	}
	if log == nil {
		log = logger.NewDefault()
	}

	// GA-P4-F1-T4: Default to count if method not specified
	if method == "" {
		method = MethodCount
	}

	return &Verifier{
		source:      source,
		destination: destination,
		graph:       g,
		method:      method,
		chunkSize:   1000, // Default chunk size for SHA256
		logger:      log,
	}, nil
}

// Verify verifies data integrity for all tables in the record set.
//
// GA-P4-F1-T4: Uses configured verification method
// GA-P4-F1-T5: Mismatch handling (returns detailed error)
// GA-P4-F1-T6: Returns verification statistics
// GA-P4-F1-T7: Skip verification if method = MethodSkip
func (v *Verifier) Verify(ctx context.Context, recordSet *types.RecordSet) (*VerifyStats, error) {
	// GA-P4-F1-T7: Skip verification if requested
	if v.method == MethodSkip {
		v.logger.Info("Verification SKIPPED (method=skip)")
		return &VerifyStats{
			Method: MethodSkip,
		}, nil
	}

	stats := &VerifyStats{
		Method: v.method,
	}

	// Get copy order to verify tables in same order
	copyOrder, err := v.graph.CopyOrder()
	if err != nil {
		return nil, fmt.Errorf("failed to get copy order: %w", err)
	}

	v.logger.Infof("Starting verification (method=%s) for %d tables", v.method, len(copyOrder))

	// Verify each table
	for _, table := range copyOrder {
		pks, exists := recordSet.Records[table]
		if !exists || len(pks) == 0 {
			// Table has no records to verify
			v.logger.Debugf("Skipping table %q (no records)", table)
			continue
		}

		// Check context cancellation
		if err := ctx.Err(); err != nil {
			return stats, fmt.Errorf("verification interrupted: %w", err)
		}

		// Verify table based on method
		var result *VerifyResult
		switch v.method {
		case MethodCount:
			result, err = v.verifyByCount(ctx, table, pks)
		case MethodSHA256:
			result, err = v.verifyBySHA256(ctx, table, pks)
		default:
			return stats, fmt.Errorf("unsupported verification method: %s", v.method)
		}

		if err != nil {
			return stats, fmt.Errorf("verification failed for table %s: %w", table, err)
		}

		stats.TablesVerified++
		stats.TotalRows += result.SourceCount

		if result.Match {
			stats.TablesPassed++
			v.logger.Debugf("Verification PASSED for table %q (%d rows)", table, result.SourceCount)
		} else {
			// GA-P4-F1-T5: Mismatch handling
			stats.TablesFailed++
			v.logger.Errorf("Verification FAILED for table %q: %s", table, result.ErrorMessage)
			return stats, fmt.Errorf("verification mismatch in table %s: %s", table, result.ErrorMessage)
		}
	}

	v.logger.Infof("Verification complete: %d tables verified, %d passed, %d failed, %d total rows",
		stats.TablesVerified, stats.TablesPassed, stats.TablesFailed, stats.TotalRows)

	if stats.TablesFailed > 0 {
		return stats, fmt.Errorf("verification failed: %d tables had mismatches", stats.TablesFailed)
	}

	return stats, nil
}

// verifyByCount compares row counts between source and destination.
//
// GA-P4-F1-T1: Row count verification
func (v *Verifier) verifyByCount(ctx context.Context, table string, pks []interface{}) (*VerifyResult, error) {
	if len(pks) == 0 {
		return &VerifyResult{
			Table:       table,
			Method:      MethodCount,
			SourceCount: 0,
			DestCount:   0,
			Match:       true,
		}, nil
	}

	// Build IN clause
	placeholders := make([]string, len(pks))
	args := make([]interface{}, len(pks))
	for i, pk := range pks {
		placeholders[i] = "?"
		args[i] = pk
	}

	// GA-P3-F3-T9: Get PK column from graph (supports configurable PKs for all tables)
	pkColumn := v.graph.GetPK(table)

	// Count source
	sourceQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s IN (%s)",
		sqlutil.QuoteIdentifier(table), sqlutil.QuoteIdentifier(pkColumn), strings.Join(placeholders, ","))
	var sourceCount int64
	if err := v.source.QueryRowContext(ctx, sourceQuery, args...).Scan(&sourceCount); err != nil {
		return nil, fmt.Errorf("failed to count source: %w", err)
	}

	// Count destination
	destQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s IN (%s)",
		sqlutil.QuoteIdentifier(table), sqlutil.QuoteIdentifier(pkColumn), strings.Join(placeholders, ","))
	var destCount int64
	if err := v.destination.QueryRowContext(ctx, destQuery, args...).Scan(&destCount); err != nil {
		return nil, fmt.Errorf("failed to count destination: %w", err)
	}

	result := &VerifyResult{
		Table:       table,
		Method:      MethodCount,
		SourceCount: sourceCount,
		DestCount:   destCount,
		Match:       sourceCount == destCount,
	}

	// GA-P4-F1-T5: Generate error message on mismatch
	if !result.Match {
		result.ErrorMessage = fmt.Sprintf("count mismatch: source=%d, dest=%d", sourceCount, destCount)
	}

	return result, nil
}

// verifyBySHA256 compares SHA256 hashes of all rows between source and destination.
//
// GA-P4-F1-T2: SHA256 hash verification
// GA-P4-F1-T3: Chunked SHA256 for large datasets
func (v *Verifier) verifyBySHA256(ctx context.Context, table string, pks []interface{}) (*VerifyResult, error) {
	if len(pks) == 0 {
		return &VerifyResult{
			Table:  table,
			Method: MethodSHA256,
			Match:  true,
		}, nil
	}

	// GA-P4-F1-T3: Chunk PKs to avoid memory issues
	sourceHash, sourceCount, err := v.computeTableHash(ctx, v.source, table, pks)
	if err != nil {
		return nil, fmt.Errorf("failed to compute source hash: %w", err)
	}

	destHash, destCount, err := v.computeTableHash(ctx, v.destination, table, pks)
	if err != nil {
		return nil, fmt.Errorf("failed to compute destination hash: %w", err)
	}

	result := &VerifyResult{
		Table:       table,
		Method:      MethodSHA256,
		SourceCount: sourceCount,
		DestCount:   destCount,
		SourceHash:  sourceHash,
		DestHash:    destHash,
		Match:       sourceHash == destHash && sourceCount == destCount,
	}

	// GA-P4-F1-T5: Generate error message on mismatch
	if !result.Match {
		if sourceCount != destCount {
			result.ErrorMessage = fmt.Sprintf("count mismatch: source=%d, dest=%d", sourceCount, destCount)
		} else {
			result.ErrorMessage = fmt.Sprintf("hash mismatch: source=%s, dest=%s", sourceHash[:16], destHash[:16])
		}
	}

	return result, nil
}

// computeTableHash computes a SHA256 hash of all rows in the specified table for the given PKs.
//
// GA-P4-F1-T2: SHA256 hash computation
// GA-P4-F1-T3: Chunked processing for large datasets
func (v *Verifier) computeTableHash(ctx context.Context, db *sql.DB, table string, pks []interface{}) (string, int64, error) {
	// GA-P3-F3-T9: Get PK column from graph (supports configurable PKs for all tables)
	pkColumn := v.graph.GetPK(table)

	// GA-P4-F1-T3: Process in chunks to avoid memory issues
	hasher := sha256.New()
	var totalRows int64

	for i := 0; i < len(pks); i += v.chunkSize {
		end := i + v.chunkSize
		if end > len(pks) {
			end = len(pks)
		}
		chunk := pks[i:end]

		// Build query
		placeholders := make([]string, len(chunk))
		args := make([]interface{}, len(chunk))
		for j, pk := range chunk {
			placeholders[j] = "?"
			args[j] = pk
		}

		// Fetch all rows ordered by PK for deterministic hashing
		query := fmt.Sprintf("SELECT * FROM %s WHERE %s IN (%s) ORDER BY %s",
			sqlutil.QuoteIdentifier(table), sqlutil.QuoteIdentifier(pkColumn), strings.Join(placeholders, ","), sqlutil.QuoteIdentifier(pkColumn))

		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			return "", 0, fmt.Errorf("query failed: %w", err)
		}

		// Get column names
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			return "", 0, fmt.Errorf("failed to get columns: %w", err)
		}

		// Hash each row
		for rows.Next() {
			// Check context cancellation
			if err := ctx.Err(); err != nil {
				rows.Close()
				return "", 0, fmt.Errorf("hash computation interrupted: %w", err)
			}

			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for j := range values {
				valuePtrs[j] = &values[j]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				rows.Close()
				return "", 0, fmt.Errorf("failed to scan row: %w", err)
			}

			// Hash row: column1=value1,column2=value2,...
			rowStr := v.serializeRow(columns, values)
			hasher.Write([]byte(rowStr))
			hasher.Write([]byte("\n"))
			totalRows++
		}

		if err := rows.Err(); err != nil {
			rows.Close()
			return "", 0, fmt.Errorf("error iterating rows: %w", err)
		}
		rows.Close()
	}

	hashBytes := hasher.Sum(nil)
	hashStr := hex.EncodeToString(hashBytes)

	return hashStr, totalRows, nil
}

// serializeRow converts a row to a deterministic string representation for hashing.
// Format: col1=val1,col2=val2,...
func (v *Verifier) serializeRow(columns []string, values []interface{}) string {
	var parts []string

	for i, col := range columns {
		val := values[i]
		var valStr string

		switch v := val.(type) {
		case nil:
			valStr = "NULL"
		case []byte:
			valStr = string(v)
		case int64:
			valStr = fmt.Sprintf("%d", v)
		case float64:
			valStr = fmt.Sprintf("%f", v)
		case bool:
			valStr = fmt.Sprintf("%t", v)
		case string:
			valStr = v
		default:
			valStr = fmt.Sprintf("%v", v)
		}

		parts = append(parts, fmt.Sprintf("%s=%s", col, valStr))
	}

	// Use null byte separator to avoid ambiguity with column values containing commas
	return strings.Join(parts, "\x00")
}

// SetChunkSize sets the chunk size for chunked SHA256 verification.
//
// GA-P4-F1-T3: Chunked SHA256 configuration
func (v *Verifier) SetChunkSize(size int) {
	if size > 0 {
		v.chunkSize = size
	}
}

// GetChunkSize returns the current chunk size.
func (v *Verifier) GetChunkSize() int {
	return v.chunkSize
}

// SetLogger sets a custom logger for the verifier.
func (v *Verifier) SetLogger(log *logger.Logger) {
	v.logger = log
}

// GetMethod returns the configured verification method.
func (v *Verifier) GetMethod() VerificationMethod {
	return v.method
}
