package database

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"
	mysql "github.com/go-sql-driver/mysql"
)

// The failover fence, G12 (spec §4.2; GR C8). A newly elected primary of a Group Replication group is
// read-only and applies the backlog it held when it was elected; a session whose group_replication_consistency
// includes BEFORE_ON_PRIMARY_FAILOVER is held there until it has, so it never reads the old state. GoArchive's
// reads and writes of data and of tracking tables must run on such a session, so every physical source and
// destination connection is made to be one before the pool hands it out, a replacement opened in the middle of a
// run included: establish or refuse.
//
// Two facts, kept apart, both read by the library on the driver connection itself (GroupConsistencyOnDriverConn):
// the plugin's state decides whether the rule applies, and the session's own value decides whether the session is
// fenced. The server's global value decides nothing, and neither fact is inferred from the other.
const (
	// fencedConsistency is the value GoArchive sets on a session that has none of the fencing values.
	fencedConsistency = "BEFORE_ON_PRIMARY_FAILOVER"
	// groupConsistencySet is the one statement the fence issues, and only where the table asks for it. It passes a
	// fence itself, so it can be a new session's first statement on a primary that is holding.
	groupConsistencySet = "SET SESSION group_replication_consistency = '" + fencedConsistency + "'"
)

// GroupConsistencyError reports a connection that cannot be established as fenced: GROUP_CONSISTENCY_CHECK. The
// plugin is active, and the session's value is unknown, unfamiliar, could not be read, could not be set or did not read
// back as set. Cause is the server's own error where it gave one.
type GroupConsistencyError struct {
	Addr   string
	Reason string
	Cause  error
}

func (e *GroupConsistencyError) Error() string {
	reason := e.Reason
	if e.Cause != nil {
		reason += ": " + e.Cause.Error()
	}
	return fmt.Sprintf("GROUP_CONSISTENCY_CHECK: %s: %s. GoArchive makes every session on a server whose Group Replication plugin is active wait for a newly elected primary's backlog, and it will not run on one it cannot make so. "+
		"Either give this account the privilege the server asks for that SET (SESSION_VARIABLES_ADMIN or SYSTEM_VARIABLES_ADMIN, where it asks for one), "+
		"or make a stronger value this account's or the server's default: group_replication_consistency %s, BEFORE, AFTER or BEFORE_AND_AFTER", e.Addr, reason, fencedConsistency)
}

func (e *GroupConsistencyError) Unwrap() error { return e.Cause }

// groupConsistencyRefusal is the refusal for a decision of the server or of the connection's shape.
func groupConsistencyRefusal(addr, reason string, cause error) error {
	return &GroupConsistencyError{Addr: addr, Reason: reason, Cause: cause}
}

// groupConsistencyFailure classifies an error from the library's read or from the SET: a MySQL error is the server's
// decision, named; anything else (the connection died, the context ended) is returned unnamed and wrapped, so the pool
// treats it as the transient it is. Nothing is returned bare: database/sql takes a bare driver sentinel for an
// instruction, and the library never returns one bare.
func groupConsistencyFailure(addr, reason string, err error) error {
	if _, ok := errors.AsType[*mysql.MySQLError](err); ok {
		return groupConsistencyRefusal(addr, reason, err)
	}
	return fmt.Errorf("group consistency check on %s: %s: %w", addr, reason, err)
}

// ensureGroupConsistency applies spec §4.2's decision table to conn. It leaves the connection open: closing a refused one
// is the connector's.
//
//	plugin not ACTIVE (or not installed), any value   nothing: the rule does not apply, and the value is not judged
//	plugin state unreadable                           refused
//	ACTIVE, EVENTUAL                                  SET to BEFORE_ON_PRIMARY_FAILOVER, read again, require it
//	ACTIVE, BEFORE_ON_PRIMARY_FAILOVER, BEFORE,
//	        AFTER or BEFORE_AND_AFTER                 nothing: each includes the fence, and a stronger choice is kept
//	ACTIVE, another value or none                     refused
//	ACTIVE, unreadable, SET refused, not read back    refused
func ensureGroupConsistency(ctx context.Context, conn driver.Conn, addr string) error {
	q, ok := conn.(driver.QueryerContext)
	if !ok {
		return groupConsistencyRefusal(addr, fmt.Sprintf("the driver connection (%T) cannot run the library's read of the plugin's state and the session's consistency", conn), nil)
	}
	const readFailed = "cannot read the Group Replication plugin's state and the session's group_replication_consistency"
	found, err := dbsrepl.GroupConsistencyOnDriverConn(ctx, q)
	if err != nil {
		return groupConsistencyFailure(addr, readFailed, err)
	}
	if found.PluginStatus != "ACTIVE" {
		return nil
	}
	if !found.SessionConsistencyKnown {
		return groupConsistencyRefusal(addr, "the Group Replication plugin is ACTIVE and the server returned no group_replication_consistency value for this session, so GoArchive cannot tell whether the session is fenced", nil)
	}
	switch found.SessionConsistency {
	case fencedConsistency, "BEFORE", "AFTER", "BEFORE_AND_AFTER":
		return nil
	case "EVENTUAL":
	default:
		return groupConsistencyRefusal(addr, fmt.Sprintf("the Group Replication plugin is ACTIVE and the session's group_replication_consistency is %q, a value GoArchive does not know, so it cannot tell whether the session is fenced", found.SessionConsistency), nil)
	}

	x, ok := conn.(driver.ExecerContext)
	if !ok {
		return groupConsistencyRefusal(addr, fmt.Sprintf("the driver connection (%T) cannot run the SET that fences the session", conn), nil)
	}
	if _, err := x.ExecContext(ctx, groupConsistencySet, nil); err != nil {
		return groupConsistencyFailure(addr, "cannot set the session's group_replication_consistency to "+fencedConsistency, err)
	}
	back, err := dbsrepl.GroupConsistencyOnDriverConn(ctx, q)
	if err != nil {
		return groupConsistencyFailure(addr, "cannot read the session's group_replication_consistency after the SET", err)
	}
	switch {
	case !back.SessionConsistencyKnown:
		return groupConsistencyRefusal(addr, "after the SET the server returned no group_replication_consistency value for this session", nil)
	case back.SessionConsistency != fencedConsistency:
		return groupConsistencyRefusal(addr, fmt.Sprintf("after the SET the session's group_replication_consistency reads %q, not %s", back.SessionConsistency, fencedConsistency), nil)
	}
	return nil
}
