package controlpath

import (
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// TestJobNameSafe is the file-name rule (spec §4.1): [A-Za-z0-9_-][A-Za-z0-9_.-]*.
func TestJobNameSafe(t *testing.T) {
	for _, c := range []struct {
		name string
		ok   bool
	}{
		{"archive-orders", true},
		{"a", true},
		{"A_b.c-9", true},
		{"-leading-dash", true},
		{"_leading_underscore", true},
		{"trailing.", true},
		{strings.Repeat("a", 255), true},
		{"", false},
		{".hidden", false},
		{"..", false},
		{".", false},
		{"a/b", false},
		{`a\b`, false},
		{"../a", false},
		{"a b", false},
		{"a\tb", false},
		{"a\x00b", false},
		{"a\nb", false},
		{"café", false},
		{"a:b", false},
		{"a*b", false},
	} {
		if got := JobNameSafe(c.name); got != c.ok {
			t.Errorf("JobNameSafe(%q) = %v, want %v", c.name, got, c.ok)
		}
	}
}

// TestReservedOnWindows (spec §7, rev 5.2): the name test is a pure function and its table runs on
// every platform. Each reserved device name, in upper, lower and mixed case, alone and before a
// ".", is reserved, and the function says which device it is; a name that merely starts or ends
// like one, or has the device name after the dot, is not.
func TestReservedOnWindows(t *testing.T) {
	devices := []string{"CON", "PRN", "AUX", "NUL"}
	for i := 1; i <= 9; i++ {
		devices = append(devices, "COM"+string(rune('0'+i)), "LPT"+string(rune('0'+i)))
	}
	for _, device := range devices {
		for _, spelling := range []string{device, strings.ToLower(device), device[:1] + strings.ToLower(device[1:])} {
			for _, name := range []string{spelling, spelling + ".daily", spelling + ".a.b", spelling + "."} {
				got, ok := ReservedOnWindows(name)
				if !ok || got != device {
					t.Errorf("ReservedOnWindows(%q) = %q, %v, want %q, true", name, got, ok, device)
				}
			}
		}
	}
	for _, name := range []string{
		"", "console", "com10", "com", "lpt", "daily.con", "xcon", "nul_1", "com0", "lpt0", "com11", "con-1", "aux1",
		"prn_", "_con", "-nul", "nul-", "comm1", "lpt10", "archive_orders", "con1", ".con",
	} {
		if got, ok := ReservedOnWindows(name); ok {
			t.Errorf("ReservedOnWindows(%q) = %q, true, want false", name, got)
		}
	}
}

// TestDeviceNamesAreRefusedOnWindowsOnly (spec §4.1, rev 5.2): the rule is Windows'. On Linux and
// macOS a job named like a device is a safe file name; on Windows the name is refused with a message
// that carries the job and the reserved name and says to rename it. The rule asks nothing of the
// system: no file with such a name is made.
func TestDeviceNamesAreRefusedOnWindowsOnly(t *testing.T) {
	wantSafe := runtime.GOOS != "windows"
	for _, name := range []string{"con", "Nul", "com1.daily", "LPT9"} {
		if got := JobNameSafe(name); got != wantSafe {
			t.Errorf("JobNameSafe(%q) = %v on %s, want %v", name, got, runtime.GOOS, wantSafe)
		}
		err := JobNameError(name)
		if (err == nil) != wantSafe {
			t.Errorf("JobNameError(%q) = %v on %s, want an error exactly on Windows", name, err, runtime.GOOS)
		}
		if err != nil {
			device, _ := ReservedOnWindows(name)
			for _, want := range []string{strconv.Quote(name), device, "rename the job"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("JobNameError(%q) = %q, want it to contain %q", name, err, want)
				}
			}
		}
	}
	// A name that is not reserved is safe everywhere, and the other rules are unchanged.
	for _, name := range []string{"console", "com10", "daily.con"} {
		if err := JobNameError(name); err != nil {
			t.Errorf("JobNameError(%q) = %v, want nil", name, err)
		}
	}
	if err := JobNameError("a b"); err == nil || !strings.Contains(err.Error(), jobNameRule) {
		t.Errorf("JobNameError(%q) = %v, want the file-name rule", "a b", err)
	}
}
