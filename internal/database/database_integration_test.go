//go:build integration

package database

import (
	"context"
	"os"
	"strconv"
	"testing"

	_ "github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/internal/config"
)

// Estate-backed fleet tests. Build tags are per-FILE, so the tagged cases live
// here rather than in database_test.go.
//
// Estate (tests/README.md): source 127.0.0.1:3305, destination 127.0.0.1:3307,
// replica 127.0.0.1:3308. Credentials come from tests/.env:
//
//	set -a; source tests/.env; set +a
//
// Like the archiver and verifier integration suites, a missing environment
// FAILS rather than skips: building with -tags=integration is the opt-in, and a
// silent skip makes `go test` print `ok` for a suite that verified nothing.

// estateDatabaseConfig builds a DatabaseConfig for one estate role from the
// TEST_SOURCE_* / TEST_DEST_* / TEST_REPLICA_* environment variables.
func estateDatabaseConfig(t *testing.T, prefix string) config.DatabaseConfig {
	t.Helper()

	host := os.Getenv(prefix + "_HOST")
	portStr := os.Getenv(prefix + "_PORT")
	user := os.Getenv(prefix + "_USER")
	password := os.Getenv(prefix + "_PASSWORD")

	if host == "" || portStr == "" || user == "" || password == "" {
		t.Fatalf(`integration environment not loaded: %[1]s_HOST/_PORT/_USER/_PASSWORD are not set.

  Load the test environment before calling go test:

      set -a; source tests/.env; set +a

  Or use the runner, which sources it for you:

      bash tests/scripts/run-tests.sh --setup --integration-only

  Reference: tests/README.md § Prerequisites`, prefix)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("%s_PORT = %q is not a number: %v", prefix, portStr, err)
	}

	return config.DatabaseConfig{
		Host:     host,
		Port:     port,
		User:     user,
		Password: password,
		Database: os.Getenv(prefix + "_DB"),
		TLS:      "disable",
	}
}

// estateReplicaServer returns the estate replica (3308) as a fleet entry.
func estateReplicaServer(t *testing.T) config.ReplicationServerConfig {
	t.Helper()
	db := estateDatabaseConfig(t, "TEST_REPLICA")
	return config.ReplicationServerConfig{
		Host:     db.Host,
		Port:     db.Port,
		User:     db.User,
		Password: db.Password,
		TLS:      "disable",
		Type:     "async",
	}
}

// TestManagerConnect_ReplicaFleetPartialFailure_Integration proves startup stays
// fail-closed per server (spec §5.1): one unconnectable fleet member aborts
// Connect, the error names that server, and every already-opened handle —
// source, destination and the healthy replica — is closed and nilled.
func TestManagerConnect_ReplicaFleetPartialFailure_Integration(t *testing.T) {
	good := estateReplicaServer(t)

	// Port 1 is unbound: connectWithRetry fails at its PingContext.
	bad := config.ReplicationServerConfig{
		Host: "127.0.0.1", Port: 1, User: good.User, Password: good.Password,
		TLS: "disable", Type: "async",
	}

	cfg := &config.Config{
		Source:      estateDatabaseConfig(t, "TEST_SOURCE"),
		Destination: estateDatabaseConfig(t, "TEST_DEST"),
		Replication: config.ReplicationConfig{
			Enabled:                   true,
			SecondsBehindSourceWithin: 10,
			CheckInterval:             5,
			CacheTTL:                  15,
			Servers:                   []config.ReplicationServerConfig{good, bad},
		},
	}

	m := NewManager(cfg)
	t.Cleanup(func() { _ = m.Close() })

	err := m.Connect(context.Background())
	if err == nil {
		t.Fatal("Connect() with an unconnectable fleet member returned nil error")
	}
	if !containsSubstring(err.Error(), bad.Addr()) {
		t.Errorf("Connect() error = %q, should name the failing server %q", err.Error(), bad.Addr())
	}

	if m.Source != nil {
		t.Error("Source should be closed and nil after a fleet connection failure")
	}
	if m.Destination != nil {
		t.Error("Destination should be closed and nil after a fleet connection failure")
	}
	if m.Replicas != nil {
		t.Errorf("Replicas = %d entries after a fleet connection failure, want nil (the healthy member must be closed too)", len(m.Replicas))
	}
}

// TestManagerPing_ReplicaFleet_Integration pins fleet Ping: a healthy estate
// pings clean, and a dead fleet member produces an error naming that server.
func TestManagerPing_ReplicaFleet_Integration(t *testing.T) {
	replica := estateReplicaServer(t)

	cfg := &config.Config{
		Source:      estateDatabaseConfig(t, "TEST_SOURCE"),
		Destination: estateDatabaseConfig(t, "TEST_DEST"),
		Replication: config.ReplicationConfig{
			Enabled:                   true,
			SecondsBehindSourceWithin: 10,
			CheckInterval:             5,
			CacheTTL:                  15,
			Servers:                   []config.ReplicationServerConfig{replica},
		},
	}

	m := NewManager(cfg)
	t.Cleanup(func() { _ = m.Close() })

	if err := m.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() to the estate failed: %v", err)
	}
	if len(m.Replicas) != 1 {
		t.Fatalf("len(Replicas) = %d, want 1", len(m.Replicas))
	}

	if err := m.Ping(context.Background()); err != nil {
		t.Fatalf("Ping() on a healthy estate returned error: %v", err)
	}

	// Kill the fleet member only; source and destination stay alive, so the
	// error can only come from the fleet leg of Ping.
	if err := m.Replicas[0].Close(); err != nil {
		t.Fatalf("closing Replicas[0]: %v", err)
	}

	err := m.Ping(context.Background())
	if err == nil {
		t.Fatal("Ping() with a closed fleet member returned nil error")
	}
	want := "replica " + replica.Addr()
	if !containsSubstring(err.Error(), want) {
		t.Errorf("Ping() error = %q, should contain %q", err.Error(), want)
	}
}
