package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// autoZeroMode is the session sql_mode that adds NO_AUTO_VALUE_ON_ZERO to base, keeping every
// mode base holds. The pin preserves the inherited modes on each physical connection with
// base "@@SESSION.sql_mode"; a test's emulated sql_mode global is a quoted literal (Global).
func autoZeroMode(base string) string {
	return "CONCAT_WS(',', NULLIF(" + base + ", ''), 'NO_AUTO_VALUE_ON_ZERO')"
}

// AutoIncrementZeroModeError reports a session whose explicit AUTO_INCREMENT
// zero preservation could not be proven during connection establishment.
type AutoIncrementZeroModeError struct {
	Mode  string
	Cause error
}

func (e *AutoIncrementZeroModeError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("AUTO_INCREMENT_ZERO_MODE_CHECK: cannot read session sql_mode: %v", e.Cause)
	}
	return "AUTO_INCREMENT_ZERO_MODE_CHECK: session lacks NO_AUTO_VALUE_ON_ZERO; restart with a connection that honors GoArchive DSN initialization"
}
func (e *AutoIncrementZeroModeError) Unwrap() error { return e.Cause }
func assertAutoIncrementZeroMode(ctx context.Context, db *sql.DB) error {
	var mode string
	if err := db.QueryRowContext(ctx, "SELECT @@SESSION.sql_mode").Scan(&mode); err != nil {
		return &AutoIncrementZeroModeError{Cause: err}
	}
	for _, token := range strings.Split(mode, ",") {
		if strings.EqualFold(strings.TrimSpace(token), "NO_AUTO_VALUE_ON_ZERO") {
			return nil
		}
	}
	return &AutoIncrementZeroModeError{Mode: mode}
}
