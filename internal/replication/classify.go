package replication

import (
	"database/sql/driver"
	"errors"
	"net"
	"strconv"
)

// holdCause is why the gate is holding the job. All three causes hold; they
// differ only in the message an operator reads and the remediation it implies.
type holdCause int

const (
	// causeUnreachable means the server is not answering: a dial failure, a
	// timeout, or a connection the driver has given up on.
	causeUnreachable holdCause = iota
	// causeUnreadable means the server answered with an error, or the failure
	// could not be identified at all. It is the fail-closed default: an
	// unrecognized failure is never evidence of health.
	causeUnreadable
	// causeUnhealthy means the read succeeded and the replication state itself
	// fails policy.
	causeUnhealthy
)

// String names the cause for the hold log line.
func (c holdCause) String() string {
	switch c {
	case causeUnreachable:
		return "unreachable"
	case causeUnreadable:
		return "unreadable"
	case causeUnhealthy:
		return "unhealthy"
	default:
		return "holdCause(" + strconv.Itoa(int(c)) + ")"
	}
}

// classify maps a failed status read onto a hold cause. It never sees a
// context error: WaitForLag returns those before classifying, so a query
// aborted by shutdown is never reported as an unreachable server.
//
// Unwrapping traverses the library's *replication.OpError, which wraps every
// read failure it reports.
func classify(err error) holdCause {
	// *net.OpError is covered by this match too — it implements net.Error.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return causeUnreachable
	}
	if errors.Is(err, driver.ErrBadConn) {
		return causeUnreachable
	}

	// Everything else is unreadable: a server error such as *mysql.MySQLError
	// 1227 (privilege denied), a decode or protocol failure, or an error this
	// code has never seen. Fail closed rather than guess.
	return causeUnreadable
}
