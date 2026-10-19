package database

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"

	mysql "github.com/go-sql-driver/mysql"
)

// A session keeps at most max_error_count of a statement's warnings. At 0 it keeps none, so
// neither the INSERT diagnostics nor the range check (warning 3170) can read what a
// statement raised. Every new source and destination connection is checked, and a session
// at 0 is raised to MySQL's default (M7c-F5). Setting the session value needs
// SESSION_VARIABLES_ADMIN or SYSTEM_VARIABLES_ADMIN on Percona 8.4.11 and Oracle 8.0.40,
// 8.4.11 and 9.7.1 (probe PR-10), so a session above 0 is never touched: a least-privilege
// account pays one SELECT per connection and never meets the privilege.
const (
	maxErrorCountProbe = "SELECT @@SESSION.max_error_count"
	maxErrorCountSet   = "SET SESSION max_error_count = 1024"
)

// mysqlSpecificAccessDenied is ER_SPECIFIC_ACCESS_DENIED_ERROR, MySQL's answer to a SET the
// account may not run.
const mysqlSpecificAccessDenied = 1227

// MaxErrorCountError reports a connection whose session keeps no warnings and could not be
// raised. Denied is true when MySQL refused the SET for want of a privilege; its message
// names the two remedies.
type MaxErrorCountError struct {
	Addr   string
	Denied bool
	Reason string
	Cause  error
}

func (e *MaxErrorCountError) Error() string {
	if e.Denied {
		return fmt.Sprintf("MAX_ERROR_COUNT_CHECK: %s: the session's max_error_count is 0, so MySQL keeps none of a statement's warnings for GoArchive to read, and this account may not raise it (%v). Raise the server's global max_error_count (MySQL's default is 1024), or grant SESSION_VARIABLES_ADMIN to this account so GoArchive raises its own sessions to 1024", e.Addr, e.Cause)
	}
	if e.Cause != nil {
		return fmt.Sprintf("MAX_ERROR_COUNT_CHECK: %s: %s: %v", e.Addr, e.Reason, e.Cause)
	}
	return fmt.Sprintf("MAX_ERROR_COUNT_CHECK: %s: %s", e.Addr, e.Reason)
}

func (e *MaxErrorCountError) Unwrap() error { return e.Cause }

// sessionConn is what the check needs of a driver connection; go-sql-driver's connection
// implements both (probe PR-11).
type sessionConn interface {
	driver.QueryerContext
	driver.ExecerContext
}

// errProbeShape marks a probe result of the wrong shape: a decision of the server, like a
// MySQL error, and so named. Any other failure (the new connection died, the network
// failed) is returned unnamed, so database/sql's driver.ErrBadConn handling and
// connectWithRetry's backoff treat it as the transient it is (Fable plan review P-3).
var errProbeShape = errors.New("unexpected result")

// probeFailure classifies err from a probe statement: the named refusal when the server
// decided it (a *mysql.MySQLError or a result of the wrong shape), else the plain error.
func probeFailure(addr, reason string, err error) error {
	var me *mysql.MySQLError
	if errors.As(err, &me) || errors.Is(err, errProbeShape) {
		return &MaxErrorCountError{Addr: addr, Reason: reason, Cause: err}
	}
	return fmt.Errorf("max_error_count probe on %s: %w", addr, err)
}

// ensureMaxErrorCount reads the session's max_error_count on conn and, at 0, sets it to 1024
// and reads it back.
func ensureMaxErrorCount(ctx context.Context, conn driver.Conn, addr string) error {
	c, ok := conn.(sessionConn)
	if !ok {
		return &MaxErrorCountError{Addr: addr, Reason: fmt.Sprintf("the driver connection (%T) cannot run the session check", conn)}
	}
	n, err := readMaxErrorCount(ctx, c)
	if err != nil {
		return probeFailure(addr, "cannot read the session's max_error_count", err)
	}
	if n > 0 {
		return nil
	}
	if _, err := c.ExecContext(ctx, maxErrorCountSet, nil); err != nil {
		var me *mysql.MySQLError
		if errors.As(err, &me) && me.Number == mysqlSpecificAccessDenied {
			return &MaxErrorCountError{Addr: addr, Denied: true, Cause: err}
		}
		return probeFailure(addr, "cannot set the session's max_error_count to 1024", err)
	}
	n, err = readMaxErrorCount(ctx, c)
	if err != nil {
		return probeFailure(addr, "cannot read the session's max_error_count after setting it", err)
	}
	if n == 0 {
		return &MaxErrorCountError{Addr: addr, Reason: "the session's max_error_count is still 0 after SET SESSION max_error_count = 1024"}
	}
	return nil
}

// readMaxErrorCount runs the probe. The driver returns the variable as uint64 on the text
// protocol (probe PR-11; packets.go parses an unsigned BIGINT with ParseUint); int64 is
// accepted too, and any other shape wraps errProbeShape.
func readMaxErrorCount(ctx context.Context, c driver.QueryerContext) (uint64, error) {
	rows, err := c.QueryContext(ctx, maxErrorCountProbe, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()
	dest := make([]driver.Value, len(rows.Columns()))
	if len(dest) != 1 {
		return 0, fmt.Errorf("%w: %s returned %d columns, want 1", errProbeShape, maxErrorCountProbe, len(dest))
	}
	if err := rows.Next(dest); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, fmt.Errorf("%w: %s returned no row", errProbeShape, maxErrorCountProbe)
		}
		return 0, err
	}
	switch v := dest[0].(type) {
	case uint64:
		return v, nil
	case int64:
		if v >= 0 {
			return uint64(v), nil
		}
	}
	return 0, fmt.Errorf("%w: %s returned %v (%T), want an unsigned integer", errProbeShape, maxErrorCountProbe, dest[0], dest[0])
}

// warningsConnector runs ensureMaxErrorCount, then ensureSessionCharset (CHS-1), on every
// physical connection its pool opens, startup and replacements alike: the pools retire each
// connection within defaultConnMaxLife, and a replacement takes the globals as they are then.
// A connection that fails either check is closed and never reaches the pool.
type warningsConnector struct {
	driver.Connector
	addr string
}

func (c warningsConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	if err := ensureMaxErrorCount(ctx, conn, c.addr); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := ensureSessionCharset(ctx, conn, c.addr); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// poolConnector is the connector OpenPool opens a pool on: the driver's own, from mc's DSN
// exactly as sql.Open would build it, wrapped in warningsConnector for addr when the pool's
// statements read warnings and job data (source and destination; replica pools read replica
// status only, so they are pinned by BuildDSN but not checked).
func poolConnector(mc *mysql.Config, addr string, readsWarnings bool) (driver.Connector, error) {
	connector, err := (mysql.MySQLDriver{}).OpenConnector(mc.FormatDSN())
	if err != nil {
		return nil, err
	}
	if !readsWarnings {
		return connector, nil
	}
	return warningsConnector{Connector: connector, addr: addr}, nil
}
