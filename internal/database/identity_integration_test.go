//go:build integration

package database

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/internal/config"
)

// TestConnectRefusesSameDatabase_Integration is issue #13's reproduction shape:
// both pools point at TEST_SOURCE. Connect must refuse with the typed error and
// release both pools. It is the witness for REMOVING THE GUARD; it cannot see a
// predicate that lost one term, because identical endpoints satisfy either term
// alone (spec §5, review R1).
func TestConnectRefusesSameDatabase_Integration(t *testing.T) {
	src := estateDatabaseConfig(t, "TEST_SOURCE")
	m := NewManager(&config.Config{Source: src, Destination: src})
	t.Cleanup(func() { _ = m.Close() })

	err := m.Connect(context.Background())
	if err == nil {
		t.Fatal("Connect() with source == destination returned nil; the same-database guard is missing")
	}
	var same *SameDatabaseError
	if !errors.As(err, &same) {
		t.Fatalf("Connect() error = %v, want *SameDatabaseError", err)
	}
	if !strings.Contains(err.Error(), "SRC_DEST_IDENTITY_CHECK") {
		t.Fatalf("refusal does not name the check: %v", err)
	}
	if same.Source.ServerUUID != same.Destination.ServerUUID || !strings.EqualFold(same.Source.Schema, same.Destination.Schema) {
		t.Fatalf("refusal carries non-identical identities: %+v", same)
	}
	if m.Source != nil || m.Destination != nil {
		t.Fatal("Connect() left pools open after refusing")
	}
}

// Same server, different schema is a supported layout (archive `app` into
// `app_archive` on one instance). Witness for removing the SCHEMA term of the
// predicate: a uuid-only comparison would refuse this.
func TestConnectAllowsSameServerDifferentSchema_Integration(t *testing.T) {
	src := estateDatabaseConfig(t, "TEST_SOURCE")
	dst := src
	dst.Database = "mysql" // exists on every server; Connect only pings and probes, nothing is written
	if strings.EqualFold(src.Database, dst.Database) {
		t.Fatalf("fixture: TEST_SOURCE_DB is %q; this test needs a source schema other than mysql", src.Database)
	}

	m := NewManager(&config.Config{Source: src, Destination: dst})
	t.Cleanup(func() { _ = m.Close() })
	if err := m.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() refused same server / different schema: %v", err)
	}
}

// Different servers, same schema NAME must be accepted — the ordinary estate
// layout (sakila → sakila_archive) uses different names and cannot witness this,
// so both pools select `mysql`. Witness for removing the UUID term: a
// schema-only comparison would refuse this (review R1).
//
// Connect runs first; the test then proves the fixture's precondition through
// the connected pools — the two servers really do report different UUIDs and
// both selected the same schema. A refusal would already have failed the test,
// so checking the precondition second does not weaken it.
func TestConnectAllowsDifferentServersSameSchema_Integration(t *testing.T) {
	src := estateDatabaseConfig(t, "TEST_SOURCE")
	dst := estateDatabaseConfig(t, "TEST_DEST")
	src.Database, dst.Database = "mysql", "mysql"

	m := NewManager(&config.Config{Source: src, Destination: dst})
	t.Cleanup(func() { _ = m.Close() })
	if err := m.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() refused different servers with the same schema name: %v", err)
	}

	ctx := context.Background()
	a, err := readServerIdentity(ctx, m.Source)
	if err != nil {
		t.Fatalf("source identity: %v", err)
	}
	b, err := readServerIdentity(ctx, m.Destination)
	if err != nil {
		t.Fatalf("destination identity: %v", err)
	}
	if a.ServerUUID == b.ServerUUID {
		t.Fatalf("fixture: TEST_SOURCE and TEST_DEST report the same server_uuid %s; this witness needs two servers", a.ServerUUID)
	}
	if !strings.EqualFold(a.Schema, b.Schema) {
		t.Fatalf("fixture: schemas differ (%q vs %q); this witness needs the same schema name on both", a.Schema, b.Schema)
	}
}
