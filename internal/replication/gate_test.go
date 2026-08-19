package replication

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/logger"
)

// newTestLogger returns a real file-backed JSON logger plus a reader that
// decodes what it has written so far. It deliberately uses the existing logger
// API — a test-only export in a production package is reachable by nothing and
// fails the dead-code guard.
func newTestLogger(t *testing.T) (*logger.Logger, func() []map[string]any) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "gate.log")
	log, err := logger.New(&config.LoggingConfig{
		Level:  "debug",
		Format: "json",
		Output: path,
		// FileOnly keeps the assertions off the console stream.
		FileOnly: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })

	lines := func() []map[string]any {
		_ = log.Sync()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var out []map[string]any
		for _, l := range bytes.Split(data, []byte("\n")) {
			if len(l) == 0 {
				continue
			}
			var m map[string]any
			if err := json.Unmarshal(l, &m); err != nil {
				t.Fatalf("non-JSON log line %q: %v", l, err)
			}
			out = append(out, m)
		}

		return out
	}

	return log, lines
}

// msgsAt returns every message logged at one level, in order.
func msgsAt(lines []map[string]any, level string) []string {
	var out []string
	for _, m := range lines {
		if m["level"] != level {
			continue
		}
		if s, ok := m["msg"].(string); ok {
			out = append(out, s)
		}
	}

	return out
}

// hasMsg reports whether an exact message was logged at that level.
func hasMsg(lines []map[string]any, level, msg string) bool {
	for _, s := range msgsAt(lines, level) {
		if s == msg {
			return true
		}
	}

	return false
}

// openIdleDB returns a *sql.DB that has never dialed anything: sql.Open only
// validates the DSN, so no server is involved.
func openIdleDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("mysql", "gate:gate@tcp(127.0.0.1:1)/gate")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	return db
}

func TestGateNewDisabled(t *testing.T) {
	log, lines := newTestLogger(t)

	g, err := New(config.ReplicationConfig{Enabled: false}, nil, log)
	if err != nil {
		t.Fatalf("New returned %v, want nil", err)
	}
	if g == nil {
		t.Fatal("New returned a nil Gate")
	}
	if g.enabled {
		t.Error("gate.enabled = true, want false")
	}

	decoded := lines()
	const want = "Replication monitoring is DISABLED (no replication block enabled)"
	if !hasMsg(decoded, "info", want) {
		t.Errorf("info lines = %#v, want one equal to %q", msgsAt(decoded, "info"), want)
	}
	for _, m := range msgsAt(decoded, "info") {
		if strings.Contains(m, "ENABLED:") {
			t.Errorf("logged %q, want no startup summary for a disabled gate", m)
		}
	}
}

func TestGateNewCountMismatch(t *testing.T) {
	log, _ := newTestLogger(t)

	cfg := config.ReplicationConfig{
		Enabled: true,
		Servers: []config.ReplicationServerConfig{
			{Host: "10.0.0.1", Port: 3306},
			{Host: "10.0.0.2", Port: 3306},
		},
	}

	g, err := New(cfg, []*sql.DB{openIdleDB(t)}, log)
	if err == nil {
		t.Fatal("New returned nil error, want a construction error")
	}
	if g != nil {
		t.Error("New returned a non-nil Gate alongside an error")
	}
	const want = "replication: 2 server configs but 1 connections"
	if err.Error() != want {
		t.Errorf("error = %q, want %q", err.Error(), want)
	}
}

func TestGateNewNilHandle(t *testing.T) {
	log, _ := newTestLogger(t)

	cfg := config.ReplicationConfig{
		Enabled: true,
		Servers: []config.ReplicationServerConfig{
			{Host: "10.0.0.1", Port: 3306},
			{Host: "::1", Port: 3307},
		},
	}

	g, err := New(cfg, []*sql.DB{openIdleDB(t), nil}, log)
	if err == nil {
		t.Fatal("New returned nil error, want a construction error")
	}
	if g != nil {
		t.Error("New returned a non-nil Gate alongside an error")
	}
	if !strings.Contains(err.Error(), "[::1]:3307") {
		t.Errorf("error = %q, want it to name %q", err.Error(), "[::1]:3307")
	}
}

func TestGateNewNilLoggerTolerated(t *testing.T) {
	g, err := New(config.ReplicationConfig{Enabled: false}, nil, nil)
	if err != nil {
		t.Fatalf("New returned %v, want nil", err)
	}
	if g.logger == nil {
		t.Error("gate.logger is nil, want the default logger")
	}
}

func TestGateNewStartupSummary(t *testing.T) {
	log, lines := newTestLogger(t)

	cfg := config.ReplicationConfig{
		Enabled:                   true,
		SecondsBehindSourceWithin: 10,
		CheckInterval:             5,
		CacheTTL:                  15,
		Servers: []config.ReplicationServerConfig{
			{Host: "10.0.0.1", Port: 3306},
			{Host: "::1", Port: 3307, Channels: []string{"", "billing"}},
		},
	}

	g, err := New(cfg, []*sql.DB{openIdleDB(t), openIdleDB(t)}, log)
	if err != nil {
		t.Fatalf("New returned %v, want nil", err)
	}

	if !g.enabled {
		t.Error("gate.enabled = false, want true")
	}
	if g.tolerance != 10 {
		t.Errorf("gate.tolerance = %d, want 10", g.tolerance)
	}
	if g.interval != 5*time.Second {
		t.Errorf("gate.interval = %s, want 5s", g.interval)
	}
	if g.ttl != 15*time.Second {
		t.Errorf("gate.ttl = %s, want 15s", g.ttl)
	}
	if g.now == nil || g.sleep == nil {
		t.Error("gate.now/gate.sleep are nil, want defaulted seams")
	}
	if len(g.servers) != 2 {
		t.Fatalf("gate.servers = %d, want 2", len(g.servers))
	}
	if g.servers[0].id != "10.0.0.1:3306" || g.servers[1].id != "[::1]:3307" {
		t.Errorf("server ids = %q/%q, want %q/%q",
			g.servers[0].id, g.servers[1].id, "10.0.0.1:3306", "[::1]:3307")
	}
	if len(g.servers[0].channels) != 0 {
		t.Errorf("server 0 channels = %#v, want none (all channels)", g.servers[0].channels)
	}
	if len(g.servers[1].channels) != 2 {
		t.Errorf("server 1 channels = %#v, want two", g.servers[1].channels)
	}
	if g.servers[0].reader == nil || g.servers[1].reader == nil {
		t.Error("gate.servers carry nil readers, want an inspector each")
	}

	decoded := lines()
	for _, want := range []string{
		"Replication monitoring ENABLED: 2 server(s), tolerance 10s, interval 5s, cache TTL 15s",
		"  server 10.0.0.1:3306 channels: all",
		"  server [::1]:3307 channels: <default>, billing",
	} {
		if !hasMsg(decoded, "info", want) {
			t.Errorf("info lines = %#v, want one equal to %q", msgsAt(decoded, "info"), want)
		}
	}
	if len(msgsAt(decoded, "error")) != 0 {
		t.Errorf("error lines = %#v, want none", msgsAt(decoded, "error"))
	}
}
