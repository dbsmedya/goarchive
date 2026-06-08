// Package archiver provides replication lag monitoring for GoArchive.
package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/logger"
)

// ReplicationStatus represents the current state of MySQL replication.
//
// GA-P3-F5-T1: SHOW SLAVE STATUS query
type ReplicationStatus struct {
	SecondsBehindMaster sql.NullInt64 // NULL if replica is stopped
	SlaveIORunning      string        // "Yes", "No", "Connecting"
	SlaveSQLRunning     string        // "Yes", "No"
	LastError           string        // Last replication error message
}

// LagMonitor monitors replication lag on a MySQL replica.
//
// It queries SHOW SLAVE STATUS (or SHOW REPLICA STATUS for MySQL 8.0.22+)
// to track replication lag and pause processing when lag exceeds threshold.
//
// GA-P3-F5: Replication Lag Monitor
type LagMonitor struct {
	db        *sql.DB
	enabled   bool
	threshold int           // Maximum acceptable lag in seconds
	interval  time.Duration // How often to check lag
	channel   string        // Optional named replication channel (empty = default)
	logger    *logger.Logger
}

// NewLagMonitor creates a new replication lag monitor.
//
// Parameters:
//   - replicaDB: Connection to the replica database (can be nil if disabled)
//   - cfg: Safety configuration with lag threshold and check interval
//   - log: Logger for lag warnings
//
// GA-P3-F5-T7: Monitor disabled mode
func NewLagMonitor(replicaDB *sql.DB, cfg config.SafetyConfig, log *logger.Logger) (*LagMonitor, error) {
	if log == nil {
		log = logger.NewDefault()
	}

	// GA-P3-F5-T7: If replicaDB is nil, monitoring is disabled
	if replicaDB == nil {
		log.Info("Replication lag monitoring is DISABLED (no replica connection)")
		return &LagMonitor{
			db:      nil,
			enabled: false,
			logger:  log,
		}, nil
	}

	// Default values if not configured
	threshold := cfg.LagThreshold
	if threshold <= 0 {
		threshold = 10 // Default: 10 seconds
	}

	interval := time.Duration(cfg.CheckInterval) * time.Second
	if interval <= 0 {
		interval = 5 * time.Second // Default: 5 seconds
	}

	log.Infof("Replication lag monitoring ENABLED (threshold: %ds, interval: %s)", threshold, interval)

	return &LagMonitor{
		db:        replicaDB,
		enabled:   true,
		threshold: threshold,
		interval:  interval,
		logger:    log,
	}, nil
}

// GetReplicationStatus queries the replica for current replication status.
//
// GA-P3-F5-T1: SHOW SLAVE STATUS query
// GA-P3-F5-T6: Replica error handling
func (lm *LagMonitor) GetReplicationStatus(ctx context.Context) (*ReplicationStatus, error) {
	// GA-P3-F5-T7: If disabled, return nil status
	if !lm.enabled {
		return nil, nil
	}

	// GA-P3-F5-T1: Try SHOW REPLICA STATUS (MySQL 8.0.22+)
	// Fall back to SHOW SLAVE STATUS for older versions. When a named channel is
	// configured, scope both forms to it with FOR CHANNEL '<name>'.
	channelClause := ""
	if lm.channel != "" {
		channelClause = " FOR CHANNEL " + quoteSQLString(lm.channel)
	}
	query := "SHOW REPLICA STATUS" + channelClause
	rows, err := lm.db.QueryContext(ctx, query)
	if err != nil {
		// Try legacy command
		query = "SHOW SLAVE STATUS" + channelClause
		rows, err = lm.db.QueryContext(ctx, query)
		if err != nil {
			// GA-P3-F5-T6: Replica error handling
			return nil, fmt.Errorf("failed to query replication status: %w", err)
		}
	}
	defer func() {
		if err := rows.Close(); err != nil {
			lm.logger.Warnf("Failed to close rows in lag monitor: %v", err)
		}
	}()

	if !rows.Next() {
		// GA-P3-F5-T6: No replication configured on this server
		return nil, fmt.Errorf("replication not configured on replica server")
	}

	// Get column names to dynamically locate fields
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Scan all columns into a map
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, fmt.Errorf("failed to scan replication status: %w", err)
	}

	// Build map of column name -> value
	result := make(map[string]interface{})
	for i, col := range columns {
		val := values[i]
		// Convert []byte to string
		if b, ok := val.([]byte); ok {
			val = string(b)
		}
		result[col] = val
	}

	// Extract key fields
	status := &ReplicationStatus{}

	// Seconds_Behind_Master (MySQL <8.4) or Seconds_Behind_Source (MySQL 8.4+,
	// which renamed the column and removed SHOW SLAVE STATUS). Can be NULL if the
	// replica is stopped.
	// The MySQL driver's text protocol returns numeric columns as []byte, which the
	// loop above normalizes to string. A small number of callers (and sqlmock) may
	// deliver int64 directly, so both are handled.
	sbm, ok := result["Seconds_Behind_Master"]
	if !ok || sbm == nil {
		sbm = result["Seconds_Behind_Source"]
	}
	if lag, valid := parseSecondsBehind(sbm); valid {
		status.SecondsBehindMaster = sql.NullInt64{Int64: lag, Valid: true}
	}

	// Slave_IO_Running (or Replica_IO_Running)
	if sio, ok := result["Slave_IO_Running"]; ok {
		status.SlaveIORunning = statusFieldToString(sio)
	} else if sio, ok := result["Replica_IO_Running"]; ok {
		status.SlaveIORunning = statusFieldToString(sio)
	}

	// Slave_SQL_Running (or Replica_SQL_Running)
	if ssql, ok := result["Slave_SQL_Running"]; ok {
		status.SlaveSQLRunning = statusFieldToString(ssql)
	} else if ssql, ok := result["Replica_SQL_Running"]; ok {
		status.SlaveSQLRunning = statusFieldToString(ssql)
	}

	// Last_Error
	if lastErr, ok := result["Last_Error"]; ok && lastErr != nil {
		status.LastError = statusFieldToString(lastErr)
	}

	return status, nil
}

// CheckLag checks if replication lag is within acceptable threshold.
//
// Returns:
//   - bool: true if lag is acceptable (or monitoring disabled), false if lag exceeds threshold
//   - int: current lag in seconds (0 if disabled)
//   - error: any error querying replication status
//
// GA-P3-F5-T2: Lag threshold check
// GA-P3-F5-T5: Lag warning logging
// GA-P3-F5-T6: Replica error handling
func (lm *LagMonitor) CheckLag(ctx context.Context) (bool, int, error) {
	// GA-P3-F5-T7: If disabled, always return OK
	if !lm.enabled {
		return true, 0, nil
	}

	status, err := lm.GetReplicationStatus(ctx)
	if err != nil {
		// GA-P3-F5-T6: Replica error - log and treat as high lag (safer to pause)
		lm.logger.Errorf("Failed to check replication status: %v", err)
		return false, -1, err
	}

	// Check if replication is running
	if status.SlaveIORunning != "Yes" || status.SlaveSQLRunning != "Yes" {
		// GA-P3-F5-T6: Replication not running - critical error
		lm.logger.Errorf("Replication is NOT running (IO: %s, SQL: %s)", status.SlaveIORunning, status.SlaveSQLRunning)
		if status.LastError != "" {
			lm.logger.Errorf("Replication error: %s", status.LastError)
		}
		return false, -1, fmt.Errorf("replication is not running")
	}

	// Check lag value
	if !status.SecondsBehindMaster.Valid {
		// GA-P3-F5-T6: NULL lag value - replica may be stopped
		lm.logger.Warn("Seconds_Behind_Source/Master is NULL (replica may be stopped)")
		return false, -1, fmt.Errorf("replication lag is NULL")
	}

	lag := int(status.SecondsBehindMaster.Int64)

	// GA-P3-F5-T2: Lag threshold check
	if lag > lm.threshold {
		// GA-P3-F5-T5: Lag warning logging
		lm.logger.Warnf("Replication lag is HIGH: %d seconds (threshold: %d seconds)", lag, lm.threshold)
		return false, lag, nil
	}

	lm.logger.Debugf("Replication lag OK: %d seconds (threshold: %d seconds)", lag, lm.threshold)
	return true, lag, nil
}

// WaitForLag blocks until replication lag falls below threshold.
//
// This is called before each batch to ensure the replica is caught up.
//
// GA-P3-F5-T3: Pre-batch check
// GA-P3-F5-T4: Pause on high lag
func (lm *LagMonitor) WaitForLag(ctx context.Context) error {
	// GA-P3-F5-T7: If disabled, return immediately
	if !lm.enabled {
		return nil
	}

	for {
		// Check for context cancellation (graceful shutdown)
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("lag monitoring cancelled: %w", err)
		}

		// GA-P3-F5-T3: Pre-batch lag check
		ok, lag, err := lm.CheckLag(ctx)
		if err != nil {
			// GA-P3-F5-T6: Replica error - log and retry after interval
			lm.logger.Errorf("Replication check failed: %v (retrying in %s)", err, lm.interval)
		} else if !ok {
			// GA-P3-F5-T4: Pause on high lag
			lm.logger.Warnf("Pausing batch processing due to high replication lag (%d seconds, threshold: %d seconds)", lag, lm.threshold)
			lm.logger.Infof("Waiting %s before rechecking lag...", lm.interval)
		} else {
			// Lag is acceptable - proceed
			return nil
		}

		// Wait before rechecking
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(lm.interval):
			// Continue loop to recheck
		}
	}
}

// IsEnabled returns whether lag monitoring is enabled.
//
// GA-P3-F5-T7: Monitor disabled mode
func (lm *LagMonitor) IsEnabled() bool {
	return lm.enabled
}

// GetThreshold returns the configured lag threshold in seconds.
func (lm *LagMonitor) GetThreshold() int {
	return lm.threshold
}

// GetInterval returns the configured check interval.
func (lm *LagMonitor) GetInterval() time.Duration {
	return lm.interval
}

// SetLogger sets a custom logger for the lag monitor.
func (lm *LagMonitor) SetLogger(log *logger.Logger) {
	lm.logger = log
}

// parseSecondsBehind normalizes a Seconds_Behind_Source / Seconds_Behind_Master
// value into an int64 lag and a validity flag.
//
// go-sql-driver returns this column as a typed integer — uint64 for the unsigned
// column on MySQL 8.4 — which is why a plain int64/string type switch dropped the
// value and lag was misreported as NULL. Older paths, SHOW SLAVE STATUS, and
// sqlmock may instead deliver it as a string or []byte. A nil/empty/unparseable
// value means NULL (replica stopped) and returns valid=false.
func parseSecondsBehind(v interface{}) (int64, bool) {
	switch x := v.(type) {
	case nil:
		return 0, false
	case int64:
		return x, true
	case uint64:
		return int64(x), true
	case int:
		return int64(x), true
	case []byte:
		return parseLagDigits(string(x))
	case string:
		return parseLagDigits(x)
	default:
		return parseLagDigits(fmt.Sprintf("%v", x))
	}
}

// quoteSQLString renders a value as a safe single-quoted MySQL string literal,
// escaping backslashes and single quotes. Used for the FOR CHANNEL '<name>'
// clause, whose name comes from operator config.
func quoteSQLString(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "'", "''")
	return "'" + s + "'"
}

func parseLagDigits(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func statusFieldToString(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return ""
	}
}
