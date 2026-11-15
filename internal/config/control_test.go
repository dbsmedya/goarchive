package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The control block (TEL-1 C6, spec §4.1): whether a run listens on its control socket, and the
// directory of the socket. Global and per job, the job winning field by field.

func absDir() string { return filepath.Join(os.TempDir(), "goarchive-run") }

// TestControlDefaults (spec §7 row 68): control is off by default, and socket_dir is optional.
func TestControlDefaults(t *testing.T) {
	c := DefaultConfig()
	if c.Control.Enabled || c.Control.SocketDir != "" {
		t.Errorf("default control = %+v, want off and no directory", c.Control)
	}
}

// TestControlSocketDirRules (spec §4.1): socket_dir is an absolute path; a relative path and one that
// begins with ~ are refused, each with its own message, since the file expands no ~ and a relative
// path would depend on the directory a run was started from.
func TestControlSocketDirRules(t *testing.T) {
	if errs := ValidateControlConfig("control", ControlConfig{}); len(errs) != 0 {
		t.Errorf("an empty socket_dir = %v, want accepted: the default is used", errs)
	}
	if errs := ValidateControlConfig("control", ControlConfig{Enabled: true, SocketDir: absDir()}); len(errs) != 0 {
		t.Errorf("an absolute socket_dir = %v, want accepted", errs)
	}
	rel := ValidateControlConfig("control", ControlConfig{SocketDir: "run/goarchive"})
	home := ValidateControlConfig("control", ControlConfig{SocketDir: "~/run"})
	if len(rel) != 1 || rel[0].Field != "control.socket_dir" || !strings.Contains(rel[0].Message, "absolute") || !strings.Contains(rel[0].Message, "relative") {
		t.Errorf("a relative socket_dir = %v, want one error on control.socket_dir saying it must be absolute", rel)
	}
	if len(home) != 1 || home[0].Field != "control.socket_dir" || !strings.Contains(home[0].Message, "~") || !strings.Contains(home[0].Message, "expand") {
		t.Errorf("a ~ socket_dir = %v, want one error on control.socket_dir saying ~ is not expanded", home)
	}
	if len(rel) == 1 && len(home) == 1 && rel[0].Message == home[0].Message {
		t.Error("a relative path and a ~ path share one message, want each its own")
	}
	// The block is judged whether or not it is enabled: a typo is caught before it is switched on.
	if errs := ValidateControlConfig("control", ControlConfig{Enabled: false, SocketDir: "rel"}); len(errs) != 1 {
		t.Errorf("a relative socket_dir with control off = %v, want it refused too", errs)
	}
}

// TestSharedValidationIncludesControl: ValidateShared reports the control block's errors.
func TestSharedValidationIncludesControl(t *testing.T) {
	c := sharedWith(ReplicationConfig{})
	c.Control = ControlConfig{Enabled: true, SocketDir: "relative"}
	file, _ := c.ValidateShared().(ValidationErrors)
	if messageOn(file, "control.socket_dir") == "" {
		t.Errorf("ValidateShared = %v, want an error on control.socket_dir", file)
	}
}

// TestControlOverrides (spec §7 row 69, the merge): a job's enabled and socket_dir each win over the
// global value, and each omitted one inherits it; a job can switch control off under a global true
// and on under a global false.
func TestControlOverrides(t *testing.T) {
	yes, no := true, false
	dir, other := "/a", "/b"
	for _, tc := range []struct {
		name   string
		global ControlConfig
		job    *ControlOverrides
		want   ControlConfig
	}{
		{"no overrides inherit", ControlConfig{true, "/a"}, nil, ControlConfig{true, "/a"}},
		{"an empty block inherits", ControlConfig{true, "/a"}, &ControlOverrides{}, ControlConfig{true, "/a"}},
		{"off under a global true", ControlConfig{true, "/a"}, &ControlOverrides{Enabled: &no}, ControlConfig{false, "/a"}},
		{"on under a global false", ControlConfig{false, ""}, &ControlOverrides{Enabled: &yes}, ControlConfig{true, ""}},
		{"socket_dir wins, enabled inherited", ControlConfig{true, "/a"}, &ControlOverrides{SocketDir: &other}, ControlConfig{true, "/b"}},
		{"both win", ControlConfig{false, "/a"}, &ControlOverrides{Enabled: &yes, SocketDir: &other}, ControlConfig{true, "/b"}},
		{"enabled wins, socket_dir inherited", ControlConfig{false, dir}, &ControlOverrides{Enabled: &yes}, ControlConfig{true, "/a"}},
	} {
		if got := tc.job.Apply(tc.global); got != tc.want {
			t.Errorf("%s: Apply = %+v, want %+v", tc.name, got, tc.want)
		}
	}
}
