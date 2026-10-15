package logger

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/config"
)

func newCaptured(t *testing.T, cfg config.LoggingConfig, tee bool) (*Logger, *bytes.Buffer, *bytes.Buffer) {
	t.Helper()
	var out, errb bytes.Buffer
	l, err := New(&cfg, Options{Stdout: &out, Stderr: &errb, Tee: tee})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l, &out, &errb
}

func fileCfg(t *testing.T, format string) (config.LoggingConfig, string) {
	p := filepath.Join(t.TempDir(), "run.log")
	return config.LoggingConfig{Level: "info", Format: format, Output: p}, p
}

func readFile(t *testing.T, l *Logger, p string) string {
	t.Helper()
	_ = l.Sync()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// Setup A without --progress: nothing reaches the console.
func TestFileLogNoTee(t *testing.T) {
	cfg, p := fileCfg(t, "json")
	l, out, errb := newCaptured(t, cfg, false)
	l.Infow("Batch 1: 5 roots")
	l.Warnw("replication hold: x")
	if out.Len()+errb.Len() != 0 {
		t.Errorf("console = %q / %q, want nothing", out, errb)
	}
	if f := readFile(t, l, p); !strings.Contains(f, `"msg":"Batch 1: 5 roots"`) || !strings.Contains(f, `"msg":"replication hold: x"`) {
		t.Errorf("file lacks entries:\n%s", f)
	}
}

// Setup A with --progress: warnings and marked entries reach stderr as text; info does not.
func TestFileLogTee(t *testing.T) {
	cfg, p := fileCfg(t, "json")
	l, out, errb := newCaptured(t, cfg, true)
	l = l.WithJob("j")
	l.Infow("Batch 1: 5 roots")
	l.Warnw("replication hold: x", "server", "r1:3306")
	l.Infow("Resumed: sentinel file removed", Live)
	l.Errorw("Run failed", "error", "boom", FileOnly)
	if out.Len() != 0 {
		t.Errorf("stdout = %q, want nothing (the tee is stderr)", out)
	}
	e := errb.String()
	for _, w := range []string{"WARN  replication hold: x server=r1:3306\n", "INFO  Resumed: sentinel file removed\n"} {
		if !strings.Contains(e, w) {
			t.Errorf("stderr lacks %q:\n%s", w, e)
		}
	}
	for _, no := range []string{"Batch 1", "Run failed", "{", "job=", liveKey, fileOnlyKey} {
		if strings.Contains(e, no) {
			t.Errorf("stderr has %q:\n%s", no, e)
		}
	}
	f := readFile(t, l, p)
	if !strings.Contains(f, `"msg":"Run failed"`) || !strings.Contains(f, `"error":"boom"`) {
		t.Errorf("file lacks the closing entry:\n%s", f)
	}
	if strings.Contains(f, liveKey) || strings.Contains(f, fileOnlyKey) {
		t.Errorf("file carries a routing mark:\n%s", f)
	}
}

func TestFileOnlyNoTee(t *testing.T) {
	cfg, _ := fileCfg(t, "json")
	cfg.FileOnly = true
	l, out, errb := newCaptured(t, cfg, true)
	l.Warnw("replication hold: x")
	if out.Len()+errb.Len() != 0 {
		t.Errorf("file_only console = %q / %q", out, errb)
	}
}

// Setup B: plain text, no ANSI codes, on the configured stream.
func TestConsoleTextNoColour(t *testing.T) {
	for _, output := range []string{"stdout", "stderr", ""} {
		l, out, errb := newCaptured(t, config.LoggingConfig{Level: "info", Format: "text", Output: output}, false)
		l.Warnw("hold")
		got := out.String()
		if output == "stderr" {
			got = errb.String()
		}
		if !strings.Contains(got, "WARN") || strings.Contains(got, "\x1b[") {
			t.Errorf("output %q: %q, want WARN without colour codes", output, got)
		}
	}
}

// Setup C: every entry is JSON; Always passes every level; marks never appear.
func TestConsoleJSONAndAlways(t *testing.T) {
	l, out, _ := newCaptured(t, config.LoggingConfig{Level: "error", Format: "json", Output: "stdout"}, false)
	if !l.ConsoleJSON() || l.ConsoleStderr() {
		t.Fatalf("ConsoleJSON=%v ConsoleStderr=%v", l.ConsoleJSON(), l.ConsoleStderr())
	}
	l.Infow("dropped by level")
	l.Always().Infow("progress", "roots_done", 3)
	l.Always().Warnw("WARNING: banner", Live)
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		if !strings.HasPrefix(line, "{") {
			t.Errorf("non-JSON line %q", line)
		}
	}
	if s := out.String(); strings.Contains(s, "dropped") || !strings.Contains(s, `"msg":"progress"`) ||
		!strings.Contains(s, `"msg":"WARNING: banner"`) || strings.Contains(s, liveKey) {
		t.Errorf("stdout:\n%s", s)
	}
}

// Levels drop ordinary entries on every stream, and never the Always entries.
func TestLevelsOnTee(t *testing.T) {
	cfg, _ := fileCfg(t, "json")
	cfg.Level = "error"
	l, _, errb := newCaptured(t, cfg, true)
	l.Warnw("hold")
	l.Infow("Resumed", Live)
	if errb.Len() != 0 {
		t.Errorf("level error teed %q", errb)
	}
}

// Text rendering keeps one entry on one line.
func TestTeeEscapes(t *testing.T) {
	cfg, _ := fileCfg(t, "json")
	l, _, errb := newCaptured(t, cfg, true)
	l.Warnw("a\nb", "path", "x\ry", "error", errors.New("e\n2"), "counts", map[string]int64{"t": 1})
	got := errb.String()
	if strings.Count(got, "\n") != 1 || !strings.Contains(got, `a\nb`) || !strings.Contains(got, `path=x\ry`) ||
		!strings.Contains(got, `error=e\n2`) || !strings.Contains(got, "counts=map[t:1]") {
		t.Errorf("rendered %q", got)
	}
}
