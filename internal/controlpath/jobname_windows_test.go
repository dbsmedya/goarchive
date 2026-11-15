//go:build windows

package controlpath

import (
	"strings"
	"testing"
)

// TestResolveRefusesADeviceNameOnWindows (spec §4.1, rev 5.2): on Windows the resolver refuses a job
// named like a device, with the job and the reserved name in the message. Nothing is created or
// bound: Windows 11 may allow "nul.sock", and the rule does not ask the system.
func TestResolveRefusesADeviceNameOnWindows(t *testing.T) {
	for _, name := range []string{"con", "NUL", "com1.daily"} {
		path, err := Resolve(true, `C:\run`, name, -1)
		if err == nil || path != "" {
			t.Fatalf("Resolve(%q) = %q, %v, want a refusal", name, path, err)
		}
		if !strings.Contains(err.Error(), name) || !strings.Contains(err.Error(), "rename the job") {
			t.Errorf("Resolve(%q) = %q, want the job and the remedy", name, err)
		}
	}
	if path, err := Resolve(true, `C:\run`, "console", -1); err != nil || path != `C:\run\console.sock` {
		t.Errorf("Resolve(console) = %q, %v, want it accepted", path, err)
	}
}
