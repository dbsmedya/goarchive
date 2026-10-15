//go:build integration

package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/lock"
)

// orderFixture is the order tree on both servers, seeded with three old orders.
func orderFixture(t *testing.T, name string) fixture {
	t.Helper()
	f := newFixture(t, name, orderJobs)
	mustExec(t, f.src, orderDDL...)
	mustExec(t, f.dst, orderDDL...)
	seed(t, f)
	return f
}

// setConfig replaces old with new in the fixture's config file.
func setConfig(t *testing.T, f fixture, old, new string) {
	t.Helper()
	cfg, err := os.ReadFile(f.config)
	if err != nil {
		t.Fatal(err)
	}
	body := strings.Replace(string(cfg), old, new, 1)
	if body == string(cfg) {
		t.Fatalf("fixture config has no %q to replace", old)
	}
	if err := os.WriteFile(f.config, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// setLogging replaces the fixture's logging block (level error, JSON on stderr).
func setLogging(t *testing.T, f fixture, block string) {
	t.Helper()
	setConfig(t, f, "logging:\n  level: error\n  output: stderr\n", block)
}

func readAll(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// jsonLines reports whether every non-empty line of s is one JSON object.
func jsonLines(s string) bool {
	for _, l := range strings.Split(strings.TrimSpace(s), "\n") {
		var m map[string]any
		if l != "" && json.Unmarshal([]byte(l), &m) != nil {
			return false
		}
	}
	return true
}

// syncBuffer is a bytes.Buffer safe to read while a run writes to it.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// TestMySQLOutputStandard (console-output spec §3): what each logging setup prints, per
// stream, for a real run.
func TestMySQLOutputStandard(t *testing.T) {
	fileLog := func(t *testing.T, f fixture, format string) string {
		p := filepath.Join(t.TempDir(), "run.log")
		setLogging(t, f, "logging:\n  level: info\n  format: "+format+"\n  output: "+p+"\n")
		return p
	}

	t.Run("A cron: silent", func(t *testing.T) {
		f := orderFixture(t, "std_cron")
		logPath := fileLog(t, f, "json")
		out, errOut, code := mainStreams(t, "archive", "-c", f.config, "--job", "archive_orders")
		if code != 0 || out != "" || errOut != "" {
			t.Errorf("code %d, stdout %q, stderr %q: want 0 and silence", code, out, errOut)
		}
		log := readAll(t, logPath)
		for _, w := range []string{"Statement cap:", `"msg":"Batch 1:`, `"msg":"Run completed"`} {
			if !strings.Contains(log, w) {
				t.Errorf("log file lacks %q:\n%s", w, log)
			}
		}
	})

	t.Run("A with progress", func(t *testing.T) {
		f := orderFixture(t, "std_progress")
		logPath := fileLog(t, f, "json")
		out, errOut, code := mainStreams(t, "archive", "-c", f.config, "--job", "archive_orders", "--progress=1h")
		for _, w := range []string{"Job archive_orders (archive): starting. Log: " + logPath + "\n", "progress: roots 3/~3 (100.0%)",
			"Job archive_orders (archive): completed in ", "  roots: 3 in 2 batches\n", "  table        copied  deleted  verified (sha256)\n",
			"  orders            3        3                  3\n", "  order_lines       6        6                  6\n"} {
			if !strings.Contains(out, w) {
				t.Errorf("stdout lacks %q:\n%s", w, out)
			}
		}
		if code != 0 || strings.Contains(out, "{") || errOut != "" {
			t.Errorf("code %d, stdout %q, stderr %q: want 0, no JSON, nothing on stderr", code, out, errOut)
		}
	})

	t.Run("A with progress and a pause", func(t *testing.T) {
		f := orderFixture(t, "std_pause")
		fileLog(t, f, "json")
		pause := filepath.Join(t.TempDir(), "pause")
		if err := os.WriteFile(pause, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		setConfig(t, f, "processing:\n  batch_size: 2\n", "processing:\n  batch_size: 2\n  sentinel_file: "+pause+"\n")
		resetFlags(rootCmd)
		defer func() {
			resetFlags(rootCmd)
			rootCmd.SetOut(nil)
			rootCmd.SetErr(nil)
			rootCmd.SetArgs(nil)
		}()
		var out, errb syncBuffer
		done := make(chan int, 1)
		go func() {
			done <- Main([]string{"archive", "-c", f.config, "--job", "archive_orders", "--progress=1h"}, &out, &errb)
		}()
		deadline := time.Now().Add(30 * time.Second)
		for !strings.Contains(errb.String(), "Paused: sentinel file") {
			if time.Now().After(deadline) {
				t.Fatalf("no pause on stderr:\n%s", errb.String())
			}
			time.Sleep(50 * time.Millisecond)
		}
		if err := os.Remove(pause); err != nil {
			t.Fatal(err)
		}
		if code := <-done; code != 0 {
			t.Fatalf("code %d, stderr:\n%s", code, errb.String())
		}
		e := errb.String()
		if !strings.Contains(e, "WARN  Paused: sentinel file") || !strings.Contains(e, "INFO  Resumed: sentinel file") || strings.Contains(e, "Batch") {
			t.Errorf("stderr:\n%s\nwant the pause, its end, and no batch line", e)
		}
	})

	t.Run("C stdout with progress", func(t *testing.T) {
		f := orderFixture(t, "std_cstdout")
		setLogging(t, f, "logging:\n  level: error\n  format: json\n  output: stdout\n")
		out, errOut, code := mainStreams(t, "archive", "-c", f.config, "--job", "archive_orders", "--progress=1h", "--verification=none")
		if code != 0 || !jsonLines(out) {
			t.Errorf("code %d, stdout:\n%s\nwant 0 and JSON lines only", code, out)
		}
		for _, w := range []string{`"msg":"Run starting"`, `"msg":"progress"`, `"msg":"Run completed"`} {
			if !strings.Contains(out, w) {
				t.Errorf("stdout lacks %s (written past level error under --progress):\n%s", w, out)
			}
		}
		if !strings.HasPrefix(errOut, "WARNING: verification: none") || strings.Contains(errOut, "{") {
			t.Errorf("stderr = %q, want the plain banner", errOut)
		}
	})

	t.Run("C stderr failure", func(t *testing.T) {
		f := orderFixture(t, "std_cstderr")
		mustExec(t, f.dst, "DROP TABLE order_lines")
		out, errOut, code := mainStreams(t, "archive", "-c", f.config, "--job", "archive_orders")
		if code != 1 || out != "" || !jsonLines(errOut) || strings.Contains(errOut, "Error:") {
			t.Errorf("code %d, stdout %q, stderr:\n%s\nwant 1, nothing on stdout, JSON only on stderr", code, out, errOut)
		}
		if n := strings.Count(errOut, `"level":"error"`); n != 1 || !strings.Contains(errOut, `"msg":"Run failed"`) {
			t.Errorf("stderr has %d error entries, want the one closing entry:\n%s", n, errOut)
		}
	})

	t.Run("A failure summary", func(t *testing.T) {
		f := orderFixture(t, "std_summary")
		logPath := fileLog(t, f, "json")
		mustExec(t, f.dst, "DROP TABLE order_lines")
		out, errOut, code := mainStreams(t, "archive", "-c", f.config, "--job", "archive_orders")
		want := "Job archive_orders (archive): failed before the first batch. Log: " + logPath + "\nError: "
		if code != 1 || out != "" || !strings.HasPrefix(errOut, want) || strings.Count(errOut, "Error:") != 1 {
			t.Errorf("code %d, stdout %q, stderr %q: want 1 and the summary then one Error: line", code, out, errOut)
		}
	})

	t.Run("validate banners", func(t *testing.T) {
		// Codex MR R2: validate prints the R45 banners as a run would, past level error, on
		// stderr in stderr's format: JSON on the fixture's JSON stderr log, plain with a file log.
		f := orderFixture(t, "std_vbanner")
		_, errOut, code := mainStreams(t, "validate", "-c", f.config, "--job", "archive_orders", "--verification=none", "--force-triggers")
		if code != 0 || !jsonLines(errOut) || !strings.Contains(errOut, `"msg":"WARNING: verification: none`) ||
			!strings.Contains(errOut, `"msg":"WARNING: --force-triggers`) {
			t.Errorf("JSON stderr: code %d, stderr:\n%s\nwant both banners as JSON entries", code, errOut)
		}
		logPath := fileLog(t, f, "json")
		_, errOut, code = mainStreams(t, "validate", "-c", f.config, "--job", "archive_orders", "--verification=none")
		if code != 0 || !strings.HasPrefix(errOut, "WARNING: verification: none") || !strings.Contains(readAll(t, logPath), "WARNING: verification: none") {
			t.Errorf("file log: code %d, stderr %q; want the plain banner on stderr and in the log", code, errOut)
		}
	})

	t.Run("validate caps once", func(t *testing.T) {
		f := orderFixture(t, "std_caps")
		setLogging(t, f, "logging:\n  level: info\n  format: text\n  output: stdout\n")
		out, _, code := mainStreams(t, "validate", "-c", f.config, "--job", "archive_orders")
		if code != 0 || strings.Count(out, "source       by key     orders (id): ") != 1 || strings.Contains(out, "Statement cap:") {
			t.Errorf("code %d, stdout:\n%s\nwant the caps printed once and not logged", code, out)
		}
	})
}

// TestMySQLExitCodes (console-output spec §3.4): contention exits 4, a refusal 1.
func TestMySQLExitCodes(t *testing.T) {
	ctx := context.Background()
	t.Run("job running", func(t *testing.T) {
		f := orderFixture(t, "exit_job")
		held := lock.NewSet(f.dst, lock.JobLockName(f.dstName, "archive_orders"))
		if err := held.Acquire(ctx); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = held.Release(ctx) }()
		_, errOut, code := mainStreams(t, "archive", "-c", f.config, "--job", "archive_orders")
		if code != 4 || !strings.Contains(errOut, "already running") {
			t.Errorf("code %d, stderr %q: want 4 naming the running job", code, errOut)
		}
	})
	t.Run("table locked", func(t *testing.T) {
		f := orderFixture(t, "exit_table")
		held := lock.NewSet(f.src, lock.TableLockName(f.srcName, "orders"))
		if err := held.Acquire(ctx); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = held.Release(ctx) }()
		_, errOut, code := mainStreams(t, "archive", "-c", f.config, "--job", "archive_orders")
		if code != 4 || !strings.Contains(errOut, "in use by another GoArchive run") {
			t.Errorf("code %d, stderr %q: want 4 naming the locked table", code, errOut)
		}
	})
	t.Run("preflight refusal", func(t *testing.T) {
		f := orderFixture(t, "exit_refuse")
		mustExec(t, f.dst, "DROP TABLE order_lines")
		if _, _, code := mainStreams(t, "archive", "-c", f.config, "--job", "archive_orders"); code != 1 {
			t.Errorf("code %d, want 1", code)
		}
	})
}
