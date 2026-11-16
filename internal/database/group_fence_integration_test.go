//go:build integration

package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"
)

// The fence leaves a session as it found it, except for the one variable it sets (spec §4.2's connector's side; GR C8). A
// session's variables are read from a second session, by thread, before and after ensureGroupConsistency runs on the driver
// connection: the comparison ignores exactly statement_id and timestamp, the two session variables that change on their own
// between statements on 8.4 (dbsgomysql's measurement, with autocommit on and off).

var changesByItself = map[string]bool{"statement_id": true, "timestamp": true}

// sessionSnapshot is one thread's session variables, read from the observer's session.
func sessionSnapshot(ctx context.Context, observer *sql.DB, connID int64) (map[string]string, error) {
	rows, err := observer.QueryContext(ctx, "SELECT v.VARIABLE_NAME, v.VARIABLE_VALUE FROM performance_schema.variables_by_thread v "+
		"JOIN performance_schema.threads th ON th.THREAD_ID = v.THREAD_ID WHERE th.PROCESSLIST_ID = ?", connID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]string{}
	for rows.Next() {
		var name string
		var value sql.NullString
		if err := rows.Scan(&name, &value); err != nil {
			return nil, err
		}
		out[strings.ToLower(name)] = value.String
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("variables_by_thread has no row for connection %d", connID)
	}
	return out, nil
}

// snapshotDiff lists the variables that differ between two snapshots, leaving out the two that change by themselves.
func snapshotDiff(a, b map[string]string) []string {
	names := map[string]bool{}
	for k := range a {
		names[k] = true
	}
	for k := range b {
		names[k] = true
	}
	var out []string
	for k := range names {
		if !changesByItself[k] && a[k] != b[k] {
			out = append(out, fmt.Sprintf("%s: %q -> %q", k, a[k], b[k]))
		}
	}
	sort.Strings(out)
	return out
}

func connectionID(ctx context.Context, conn *sql.Conn) (int64, error) {
	var id int64
	err := conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&id)
	return id, err
}

// TestMySQLTheFenceLeavesTheSessionAsFound: where the rule does not apply (a server without the plugin) or the session is
// already at least as strong, ensureGroupConsistency changes no session variable at all; where it raises an EVENTUAL
// session on a server whose plugin is ACTIVE, the variable it sets is the only one that differs. A SET of another variable
// is seen by the same comparison, so the equal result means something.
func TestMySQLTheFenceLeavesTheSessionAsFound(t *testing.T) {
	ctx := context.Background()
	// A session's variables are read on its own server, by another session of that server.
	observerOf := func(prefix string) *sql.DB { return rawPool(t, estateDatabaseConfig(t, prefix), nil) }
	observer := observerOf("TEST_SOURCE")

	// run is ensureGroupConsistency on one connection of prefix's server whose session starts at start ("" is the
	// server's own value), and the variables that differ across it.
	run := func(t *testing.T, prefix, start string) (diff []string, found dbsrepl.GroupConsistency) {
		t.Helper()
		cfg := estateDatabaseConfig(t, prefix)
		observer := observerOf(prefix)
		var params map[string]string
		if start != "" {
			params = map[string]string{"group_replication_consistency": "'" + start + "'"}
		}
		conn, err := rawPool(t, cfg, params).Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		id, err := connectionID(ctx, conn)
		if err != nil {
			t.Fatal(err)
		}
		before, err := sessionSnapshot(ctx, observer, id)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.Raw(func(dc any) error { return ensureGroupConsistency(ctx, dc.(driver.Conn), prefix) }); err != nil {
			t.Fatalf("ensureGroupConsistency on %s: %v", prefix, err)
		}
		after, err := sessionSnapshot(ctx, observer, id)
		if err != nil {
			t.Fatal(err)
		}
		found, err = dbsrepl.NewInspector(conn).GroupConsistency(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return snapshotDiff(before, after), found
	}

	t.Run("a server without the plugin", func(t *testing.T) {
		for _, start := range []string{"EVENTUAL", "BEFORE"} {
			diff, found := run(t, "TEST_DEST", start)
			if found.PluginStatus == "ACTIVE" {
				t.Fatalf("TEST_DEST has its plugin ACTIVE: %+v", found)
			}
			if len(diff) != 0 {
				t.Errorf("start %s: the session changed across the check: %s", start, strings.Join(diff, "; "))
			}
		}
	})

	t.Run("a server whose plugin is ACTIVE", func(t *testing.T) {
		if g, err := dbsrepl.NewInspector(observer).GroupReplication(ctx); err != nil || g.PluginStatus != "ACTIVE" {
			t.Skip("the source is not a Group Replication member: this test runs on the group estate (TOPOLOGY=group)")
		}
		for _, stronger := range []string{"BEFORE", "AFTER", "BEFORE_AND_AFTER", "BEFORE_ON_PRIMARY_FAILOVER"} {
			if diff, _ := run(t, "TEST_SOURCE", stronger); len(diff) != 0 {
				t.Errorf("start %s: the session changed across the check: %s", stronger, strings.Join(diff, "; "))
			}
		}
		diff, found := run(t, "TEST_SOURCE", "EVENTUAL")
		want := []string{`group_replication_consistency: "EVENTUAL" -> "BEFORE_ON_PRIMARY_FAILOVER"`}
		if !slices.Equal(diff, want) || found.SessionConsistency != "BEFORE_ON_PRIMARY_FAILOVER" {
			t.Errorf("an EVENTUAL session changed %v (reads %q), want exactly %v", diff, found.SessionConsistency, want)
		}
	})

	t.Run("the comparison sees a SET", func(t *testing.T) {
		conn, err := rawPool(t, estateDatabaseConfig(t, "TEST_DEST"), nil).Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		id, err := connectionID(ctx, conn)
		if err != nil {
			t.Fatal(err)
		}
		observer := observerOf("TEST_DEST")
		before, err := sessionSnapshot(ctx, observer, id)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.ExecContext(ctx, "SET SESSION sort_buffer_size = 262145"); err != nil {
			t.Fatal(err)
		}
		after, err := sessionSnapshot(ctx, observer, id)
		if err != nil {
			t.Fatal(err)
		}
		if d := snapshotDiff(before, after); len(d) != 1 || !strings.HasPrefix(d[0], "sort_buffer_size:") {
			t.Errorf("the comparison reports %v after a SET of sort_buffer_size, want exactly that variable", d)
		}
	})
}
