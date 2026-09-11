// Package verifier provides data integrity verification for GoArchive.
package verifier

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/dbsmedya/dbsgomysql/pkg/sqlutil"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
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

	// columnLists maps each graph table to its full source column list
	// (ordinal order, invisible columns included). The SAME list is used for
	// the source and destination hash reads, so both sides hash the same
	// column set regardless of visibility (issue #23).
	columnMetadata map[string]types.ColumnMetadata
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
	switch method {
	case MethodCount, MethodSHA256, MethodSkip:
	default:
		return nil, fmt.Errorf("unsupported verification method: %s", method)
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

	// GA-P3-F3-T9: Get PK column from graph (supports configurable PKs for all tables)
	pkColumn := v.graph.GetPK(table)

	meta := v.columnMetadata[table]
	if _, err := meta.ProjectColumn(pkColumn); err != nil {
		return nil, &types.TemporalReadContractError{Table: table, Operation: "verify", Detail: "primary-key metadata unavailable", Cause: err}
	}
	count := v.countByPKChunks
	if meta.Kind(pkColumn) != types.TemporalNone {
		count = v.readTemporalPKs
	}
	sourceCount, err := count(ctx, v.source, table, pkColumn, pks)
	if err != nil {
		return nil, fmt.Errorf("failed to count source: %w", err)
	}
	destCount, err := count(ctx, v.destination, table, pkColumn, pks)
	if err != nil {
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

func (v *Verifier) countByPKChunks(ctx context.Context, db *sql.DB, table, pkColumn string, pks []interface{}) (int64, error) {
	var total int64

	for i := 0; i < len(pks); i += v.chunkSize {
		end := i + v.chunkSize
		if end > len(pks) {
			end = len(pks)
		}
		chunk := pks[i:end]

		placeholders := make([]string, len(chunk))
		args := make([]interface{}, len(chunk))
		for j, pk := range chunk {
			placeholders[j] = "?"
			args[j] = pk
		}

		query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s IN (%s)",
			sqlutil.QuoteIdentifier(table), sqlutil.QuoteIdentifier(pkColumn), strings.Join(placeholders, ","))

		var count int64
		if err := db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
			return 0, err
		}
		total += count
	}

	return total, nil
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

// buildHashQuery builds the explicit-column verification fetch, preserving the
// historical query shape with * replaced by explicit names.
func buildHashQuery(table, pkColumn string, meta types.ColumnMetadata, pkCount int) (string, error) {
	projection, err := meta.Projection()
	if err != nil {
		return "", err
	}
	placeholders := make([]string, pkCount)
	for i := range placeholders {
		placeholders[i] = "?"
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s IN (%s) ORDER BY %s",
		projection, sqlutil.QuoteIdentifier(table),
		sqlutil.QuoteIdentifier(pkColumn), strings.Join(placeholders, ","),
		sqlutil.QuoteIdentifier(pkColumn)), nil
}

// computeTableHash computes a SHA256 hash of all rows in the specified table for the given PKs.
//
// GA-P4-F1-T2: SHA256 hash computation
// GA-P4-F1-T3: Chunked processing for large datasets
func (v *Verifier) computeTableHash(ctx context.Context, db *sql.DB, table string, pks []interface{}) (string, int64, error) {
	// GA-P3-F3-T9: Get PK column from graph (supports configurable PKs for all tables)
	pkColumn := v.graph.GetPK(table)

	meta := v.columnMetadata[table]
	columns := meta.Names
	temporalPK := meta.Kind(pkColumn) != types.TemporalNone
	if _, err := meta.ProjectColumn(pkColumn); err != nil {
		return "", 0, &types.TemporalReadContractError{Table: table, Operation: "verify", Detail: "primary-key metadata unavailable", Cause: err}
	}
	if meta.Kind(pkColumn) != types.TemporalNone {
		set, err := types.NewTemporalIdentitySet(meta.Kind(pkColumn), table, "verify", pks)
		if err != nil {
			return "", 0, err
		}
		pks = set.Keys()
	}

	// GA-P4-F1-T3: Process in chunks to avoid memory issues
	hasher := sha256.New()
	var totalRows int64

	// Allocate scan targets and the serializer once for the whole call; the
	// column set no longer varies per chunk. Scan overwrites values in place
	// on every row.
	serializer := newRowSerializer(columns)
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for j := range values {
		valuePtrs[j] = &values[j]
	}

	for i := 0; i < len(pks); i += v.chunkSize {
		end := i + v.chunkSize
		if end > len(pks) {
			end = len(pks)
		}
		chunk := pks[i:end]

		// Fetch all rows ordered by PK for deterministic hashing
		query, err := buildHashQuery(table, pkColumn, meta, len(chunk))
		if err != nil {
			return "", 0, err
		}
		var identitySet *types.TemporalIdentitySet
		if meta.Kind(pkColumn) != types.TemporalNone {
			identitySet, err = types.NewTemporalIdentitySet(meta.Kind(pkColumn), table, "verify", chunk)
			if err != nil {
				return "", 0, err
			}
		}

		if err := func() error {
			rows, err := db.QueryContext(ctx, query, chunk...)
			if err != nil {
				return fmt.Errorf("query failed: %w", err)
			}
			defer func() {
				if err := rows.Close(); err != nil {
					v.logger.Warnf("Failed to close rows: %v", err)
				}
			}()

			// Hash each row
			for rows.Next() {
				// Check context cancellation
				if err := ctx.Err(); err != nil {
					return fmt.Errorf("hash computation interrupted: %w", err)
				}

				if err := rows.Scan(valuePtrs...); err != nil {
					return fmt.Errorf("failed to scan row: %w", err)
				}

				for j, column := range columns {
					if meta.Kind(column) != types.TemporalNone && values[j] != nil {
						raw, err := types.TemporalText(values[j])
						if err != nil {
							return &types.TemporalReadContractError{Table: table, Operation: "verify", Detail: "temporal payload representation unavailable", Cause: err}
						}
						values[j] = raw
					}
					if identitySet != nil && strings.EqualFold(column, pkColumn) {
						if err := identitySet.Observe(values[j]); err != nil {
							return err
						}
					}
				}
				// Hash row: col1=val1\x00col2=val2...\n (sorted by column name)
				hasher.Write(serializer.appendRow(values))
				totalRows++
			}

			if err := rows.Err(); err != nil {
				return fmt.Errorf("error iterating rows: %w", err)
			}
			if identitySet != nil {
				if err := identitySet.Finish(); err != nil {
					return err
				}
			}
			return nil
		}(); err != nil {
			var temporalErr *types.TemporalReadContractError
			if errors.As(err, &temporalErr) {
				return "", 0, err
			}
			if temporalPK {
				return "", 0, &types.TemporalReadContractError{Table: table, Operation: "verify", Detail: "hash read failed", Cause: err}
			}
			return "", 0, fmt.Errorf("failed to hash table %s: %w", table, err)
		}
	}

	hashBytes := hasher.Sum(nil)
	hashStr := hex.EncodeToString(hashBytes)

	return hashStr, totalRows, nil
}

// rowSerializer serializes rows that share one column set into a reusable
// buffer, preserving the historical byte format the hasher consumes:
// pairs sorted by column name, "col=value" joined by \x00, one \n per row.
type rowSerializer struct {
	order []int    // column indices in name-sorted order, computed once
	names []string // column names in driver order
	buf   []byte   // reused across rows; valid until the next appendRow
}

func newRowSerializer(columns []string) *rowSerializer {
	order := make([]int, len(columns))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool {
		return columns[order[a]] < columns[order[b]]
	})
	return &rowSerializer{order: order, names: columns}
}

// appendRow serializes values (driver order) into the reused buffer and
// returns it for a direct hasher.Write. The slice is invalidated by the
// next appendRow call.
func (s *rowSerializer) appendRow(values []interface{}) []byte {
	s.buf = s.buf[:0]
	for n, idx := range s.order {
		if n > 0 {
			s.buf = append(s.buf, 0)
		}
		s.buf = append(s.buf, s.names[idx]...)
		s.buf = append(s.buf, '=')
		s.buf = appendValue(s.buf, values[idx])
	}
	s.buf = append(s.buf, '\n')
	return s.buf
}

// appendValue appends a driver value using the same formatting the legacy
// Sprintf-based serializer produced ("%d", "%.17g", "%t", hex, "%v").
func appendValue(buf []byte, val interface{}) []byte {
	switch v := val.(type) {
	case nil:
		return append(buf, "NULL"...)
	case []byte:
		buf = append(buf, '0', 'x')
		return hex.AppendEncode(buf, v)
	case int64:
		return strconv.AppendInt(buf, v, 10)
	case float64:
		return strconv.AppendFloat(buf, v, 'g', 17, 64)
	case bool:
		return strconv.AppendBool(buf, v)
	case string:
		return append(buf, v...)
	default:
		return fmt.Appendf(buf, "%v", v)
	}
}

// SetChunkSize sets the chunk size for chunked SHA256 verification.
//
// GA-P4-F1-T3: Chunked SHA256 configuration
func (v *Verifier) SetChunkSize(size int) {
	if size > 0 {
		v.chunkSize = size
	}
}

// SetColumnMetadata installs the per-table explicit column lists used for SHA256
// verification reads on both source and destination.
func (v *Verifier) SetColumnMetadata(metadata map[string]types.ColumnMetadata) {
	v.columnMetadata = make(map[string]types.ColumnMetadata, len(metadata))
	for table, m := range metadata {
		n := types.ColumnMetadata{Names: append([]string(nil), m.Names...)}
		if m.Temporal != nil {
			n.Temporal = make(map[string]types.TemporalKind, len(m.Temporal))
			for c, k := range m.Temporal {
				n.Temporal[c] = k
			}
		}
		v.columnMetadata[table] = n
	}
}

func (v *Verifier) readTemporalPKs(ctx context.Context, db *sql.DB, table, pk string, pks []interface{}) (int64, error) {
	meta := v.columnMetadata[table]
	all, err := types.NewTemporalIdentitySet(meta.Kind(pk), table, "verify-count", pks)
	if err != nil {
		return 0, err
	}
	pks = all.Keys()
	var total int64
	projection, err := meta.ProjectColumn(pk)
	if err != nil {
		return 0, err
	}
	for i := 0; i < len(pks); i += v.chunkSize {
		end := i + v.chunkSize
		if end > len(pks) {
			end = len(pks)
		}
		chunk := pks[i:end]
		identitySet, err := types.NewTemporalIdentitySet(meta.Kind(pk), table, "verify-count", chunk)
		if err != nil {
			return 0, err
		}
		query := fmt.Sprintf("SELECT %s FROM %s WHERE %s IN (%s)", projection, sqlutil.QuoteIdentifier(table), sqlutil.QuoteIdentifier(pk), strings.TrimSuffix(strings.Repeat("?,", len(chunk)), ","))
		err = func() error {
			rows, err := db.QueryContext(ctx, query, chunk...)
			if err != nil {
				return err
			}
			defer func() { _ = rows.Close() }()
			for rows.Next() {
				var value interface{}
				if err := rows.Scan(&value); err != nil {
					return err
				}
				if err := identitySet.Observe(value); err != nil {
					return err
				}
				total++
			}
			if err := rows.Err(); err != nil {
				return err
			}
			return identitySet.Finish()
		}()
		if err != nil {
			return 0, &types.TemporalReadContractError{Table: table, Operation: "verify-count", Detail: "identity read failed", Cause: err}
		}
	}
	return total, nil
}
