package database

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"

	mysql "github.com/go-sql-driver/mysql"
)

// Every value GoArchive reads arrives in the session's character_set_results, and every
// value it binds arrives in character_set_client, so the supported character sets are
// exact only through a utf8mb4 session (CHS-1). BuildDSN pins it; a server or proxy that
// ignores or rewrites SET NAMES would still narrow it, so every new source and destination
// connection is checked at connect. A multiplexing proxy that changes the session between
// statements is not covered, as with the other session pins.
const (
	sessionCharsetProbe = "SELECT @@character_set_client, @@character_set_connection, @@character_set_results, @@collation_connection"
	sessionCharset      = "utf8mb4"
	sessionCollation    = "utf8mb4_general_ci"
)

// SessionCharsetError reports a connection whose session is not utf8mb4 /
// utf8mb4_general_ci. Values are the client, connection and results character sets and the
// connection collation as the server reported them, "NULL" for a NULL; Reason and Cause are
// set instead when the probe itself failed in a way the server decided.
type SessionCharsetError struct {
	Addr   string
	Values [4]string
	Reason string
	Cause  error
}

func (e *SessionCharsetError) Error() string {
	if e.Reason != "" {
		if e.Cause != nil {
			return fmt.Sprintf("SESSION_CHARSET_CHECK: %s: %s: %v", e.Addr, e.Reason, e.Cause)
		}
		return fmt.Sprintf("SESSION_CHARSET_CHECK: %s: %s", e.Addr, e.Reason)
	}
	return fmt.Sprintf("SESSION_CHARSET_CHECK: %s: the session is client=%s connection=%s results=%s collation=%s, not utf8mb4 with utf8mb4_general_ci, so text would read back changed. GoArchive sets it with SET NAMES at connect; an init_connect or a proxy on this server changes it after",
		e.Addr, e.Values[0], e.Values[1], e.Values[2], e.Values[3])
}

func (e *SessionCharsetError) Unwrap() error { return e.Cause }

// ensureSessionCharset reads the session's character sets on conn and refuses any other
// than the pinned ones. A server decision (a MySQL error, a result of the wrong shape) is
// named; any other failure is returned unnamed, as probeFailure does, so it stays transient.
func ensureSessionCharset(ctx context.Context, conn driver.Conn, addr string) error {
	c, ok := conn.(sessionConn)
	if !ok {
		return &SessionCharsetError{Addr: addr, Reason: fmt.Sprintf("the driver connection (%T) cannot run the session check", conn)}
	}
	got, err := readSessionCharset(ctx, c)
	if err != nil {
		var me *mysql.MySQLError
		if errors.As(err, &me) || errors.Is(err, errProbeShape) {
			return &SessionCharsetError{Addr: addr, Reason: "cannot read the session's character set", Cause: err}
		}
		return fmt.Errorf("session charset probe on %s: %w", addr, err)
	}
	if got != [4]string{sessionCharset, sessionCharset, sessionCharset, sessionCollation} {
		return &SessionCharsetError{Addr: addr, Values: got}
	}
	return nil
}

// readSessionCharset runs the probe. The text protocol returns each value as []byte; string
// is accepted too, and nil is a NULL. Any other shape wraps errProbeShape.
func readSessionCharset(ctx context.Context, c driver.QueryerContext) ([4]string, error) {
	var out [4]string
	rows, err := c.QueryContext(ctx, sessionCharsetProbe, nil)
	if err != nil {
		return out, err
	}
	defer func() { _ = rows.Close() }()
	dest := make([]driver.Value, len(rows.Columns()))
	if len(dest) != len(out) {
		return out, fmt.Errorf("%w: %s returned %d columns, want %d", errProbeShape, sessionCharsetProbe, len(dest), len(out))
	}
	if err := rows.Next(dest); err != nil {
		if errors.Is(err, io.EOF) {
			return out, fmt.Errorf("%w: %s returned no row", errProbeShape, sessionCharsetProbe)
		}
		return out, err
	}
	for i, v := range dest {
		switch v := v.(type) {
		case []byte:
			out[i] = string(v)
		case string:
			out[i] = v
		case nil:
			out[i] = "NULL"
		default:
			return out, fmt.Errorf("%w: %s returned %v (%T) in column %d, want a string", errProbeShape, sessionCharsetProbe, v, v, i+1)
		}
	}
	return out, nil
}
