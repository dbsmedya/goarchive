package jobspec

import (
	"errors"
	"runtime"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/config"
)

// The control block in a file (TEL-1 C6, spec §4.1 and §7 rows 31, 68 and 69): global and per job,
// strict, the job winning field by field, and the job-name and path-length rules at load for a job
// with control enabled.

// controlJob is a minimal valid job named name, with the given control block (a YAML fragment,
// indented under the job, or empty).
func controlJob(name, control string) string {
	return `
jobs:
  ` + name + `:
    job_type: archive
    root_table: orders
    primary_key: id
    where: "1=1"
` + control
}

func globalControl(block string) string { return block + "\n" }

func parseWith(t *testing.T, global, jobs string) (*Document, error) {
	t.Helper()
	return Parse([]byte(sharedYAML + global + jobs))
}

// TestControlBlock (spec §7 row 68): enabled defaults to false, socket_dir is optional, an unknown key
// under control is refused globally and under a job, a relative socket_dir and one beginning with ~
// are refused, each with its message.
func TestControlBlock(t *testing.T) {
	d, err := parseWith(t, "", controlJob("j", ""))
	if err != nil {
		t.Fatal(err)
	}
	if got := d.Jobs["j"].EffectiveControl(d.Shared.Control); got.Enabled || got.SocketDir != "" {
		t.Errorf("no control block = %+v, want off with no directory", got)
	}
	d, err = parseWith(t, globalControl("control:\n  enabled: true"), controlJob("j", ""))
	if err != nil || !d.Jobs["j"].EffectiveControl(d.Shared.Control).Enabled || d.Shared.Control.SocketDir != "" {
		t.Errorf("enabled alone = %+v, %v, want it on with socket_dir optional", d, err)
	}

	for _, tc := range []struct {
		name, global, job, field, msg string
	}{
		{"unknown key, global", "control:\n  enabled: true\n  socket_path: /x\n", "", "control.socket_path", "unknown key"},
		{"unknown key, job", "", "    control:\n      enabled: true\n      sockets: /x\n", "jobs.j.control.sockets", "unknown key"},
		{"relative, global", "control:\n  socket_dir: run\n", "", "control.socket_dir", "absolute"},
		{"home, global", "control:\n  socket_dir: ~/run\n", "", "control.socket_dir", "expand"},
		{"relative, job", "", "    control:\n      socket_dir: run\n", "jobs.j.control.socket_dir", "absolute"},
		{"home, job", "", "    control:\n      socket_dir: ~/run\n", "jobs.j.control.socket_dir", "expand"},
	} {
		_, err := parseWith(t, tc.global, controlJob("j", tc.job))
		var ve config.ValidationErrors
		if err == nil || !errors.As(err, &ve) || !hasError(ve, tc.field, tc.msg) {
			t.Errorf("%s: Parse() = %v, want an error on %s containing %q", tc.name, err, tc.field, tc.msg)
		}
	}
}

// TestControlPerJobOverride (spec §7 row 69): a job's control.enabled and control.socket_dir each win
// over the global value, each omitted one inherits it, and a job can switch control off under a global
// true and on under a global false.
func TestControlPerJobOverride(t *testing.T) {
	for _, tc := range []struct {
		name, global, job string
		want              config.ControlConfig
	}{
		{"inherits both", "control:\n  enabled: true\n  socket_dir: /run/a\n", "", config.ControlConfig{Enabled: true, SocketDir: "/run/a"}},
		{"off under a global true", "control:\n  enabled: true\n  socket_dir: /run/a\n", "    control:\n      enabled: false\n", config.ControlConfig{Enabled: false, SocketDir: "/run/a"}},
		{"on under a global false", "control:\n  enabled: false\n", "    control:\n      enabled: true\n", config.ControlConfig{Enabled: true}},
		{"on under no block at all", "", "    control:\n      enabled: true\n", config.ControlConfig{Enabled: true}},
		{"socket_dir wins", "control:\n  enabled: true\n  socket_dir: /run/a\n", "    control:\n      socket_dir: /run/b\n", config.ControlConfig{Enabled: true, SocketDir: "/run/b"}},
		{"both win", "control:\n  enabled: false\n  socket_dir: /run/a\n", "    control:\n      enabled: true\n      socket_dir: /run/b\n", config.ControlConfig{Enabled: true, SocketDir: "/run/b"}},
	} {
		d, err := parseWith(t, tc.global, controlJob("j", tc.job))
		if err != nil {
			t.Errorf("%s: Parse() = %v", tc.name, err)
			continue
		}
		if got := d.Jobs["j"].EffectiveControl(d.Shared.Control); got != tc.want {
			t.Errorf("%s: effective control = %+v, want %+v", tc.name, got, tc.want)
		}
	}
}

// TestJobNameRuleWithControl (spec §7 row 31): a job with control enabled and a name that cannot be a
// file name is refused at load with the message; the same name with control off for that job loads;
// validateJobNames' own refusals are unchanged.
func TestJobNameRuleWithControl(t *testing.T) {
	for _, name := range []string{"a/b", ".hidden", "a:b", "a*b", `a\b`} {
		quoted := `"` + strings.ReplaceAll(name, `\`, `\\`) + `"`
		on := "    control:\n      enabled: true\n"
		off := "    control:\n      enabled: false\n"
		_, err := parseWith(t, "", controlJob(quoted, on))
		var ve config.ValidationErrors
		if err == nil || !errors.As(err, &ve) || !hasError(ve, jobField(name)+".control", "cannot name a control socket") {
			t.Errorf("%q with control on: Parse() = %v, want it refused at load naming the file-name rule", name, err)
		}
		if _, err := parseWith(t, "", controlJob(quoted, off)); err != nil {
			t.Errorf("%q with control off for the job: Parse() = %v, want it loaded", name, err)
		}
		if _, err := parseWith(t, "control:\n  enabled: true\n", controlJob(quoted, off)); err != nil {
			t.Errorf("%q with control off under a global true: Parse() = %v, want it loaded", name, err)
		}
		if _, err := parseWith(t, "", controlJob(quoted, "")); err != nil {
			t.Errorf("%q with no control block: Parse() = %v, want it loaded", name, err)
		}
		_, err = parseWith(t, "control:\n  enabled: true\n", controlJob(quoted, ""))
		if err == nil || !errors.As(err, &ve) || !hasError(ve, jobField(name)+".control", "cannot name a control socket") {
			t.Errorf("%q under a global true: Parse() = %v, want it refused", name, err)
		}
	}
	// validateJobNames' own refusals stay as they are, with control on or off.
	for _, control := range []string{"", "    control:\n      enabled: true\n"} {
		_, err := parseWith(t, "", controlJob(`"a b"`, control))
		var ve config.ValidationErrors
		if err == nil || !errors.As(err, &ve) || !hasError(ve, jobField("a b"), "spaces") {
			t.Errorf("a name with a space, control %q: Parse() = %v, want validateJobNames' refusal", control, err)
		}
	}
}

// TestControlPathLengthAtLoad (spec §4.1): with socket_dir set, a path over the platform's limit is
// refused at load, naming the path, its length and the limit; the default directory is checked at the
// start of the run, not here.
func TestControlPathLengthAtLoad(t *testing.T) {
	name := strings.Repeat("n", 100)
	_, err := parseWith(t, "", controlJob(name, "    control:\n      enabled: true\n      socket_dir: /run/goarchive\n"))
	var ve config.ValidationErrors
	if err == nil || !errors.As(err, &ve) || !hasError(ve, jobField(name)+".control", "/run/goarchive/"+name+".sock") || !hasError(ve, jobField(name)+".control", "bytes") {
		t.Errorf("a path over the limit: Parse() = %v, want it refused naming the path and its length", err)
	}
	// Under the limit it loads.
	if _, err := parseWith(t, "", controlJob("short", "    control:\n      enabled: true\n      socket_dir: /run/goarchive\n")); err != nil {
		t.Errorf("a short path: Parse() = %v", err)
	}
	// The default directory's length is the run's check: a long name with the default loads.
	if _, err := parseWith(t, "", controlJob(name, "    control:\n      enabled: true\n")); err != nil {
		t.Errorf("a long name under the default directory: Parse() = %v, want it loaded (checked at the start of the run)", err)
	}
	// With control off the path is nobody's business.
	if _, err := parseWith(t, "", controlJob(name, "    control:\n      enabled: false\n      socket_dir: /run/goarchive\n")); err != nil {
		t.Errorf("control off: Parse() = %v", err)
	}
}

// TestDeviceNameLoadsWhereNotReserved (rev 5.2): on Linux and macOS a job named like a Windows device
// loads with control enabled. (The Windows refusal is controlpath's Windows-only case.)
func TestDeviceNameLoadsWhereNotReserved(t *testing.T) {
	if isWindows() {
		t.Skip("on Windows the name is refused at load; controlpath's Windows case covers it")
	}
	for _, name := range []string{"con", "Nul", "com1.daily", "LPT9"} {
		if _, err := parseWith(t, "", controlJob(name, "    control:\n      enabled: true\n")); err != nil {
			t.Errorf("%q with control enabled: Parse() = %v, want it loaded outside Windows", name, err)
		}
	}
}

func isWindows() bool { return runtime.GOOS == "windows" }
