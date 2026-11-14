package controlpath

import (
	"errors"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// databaseHome reads the home that the system's user database records for uid by a route
// that does not go through HomeOf: getent on Linux, dscacheutil on macOS. have is false where
// the test has no such route (Windows, or a Linux without getent).
func databaseHome(t *testing.T, uid int) (home string, found, have bool) {
	t.Helper()
	switch runtime.GOOS {
	case "linux":
		out, err := exec.Command("getent", "passwd", strconv.Itoa(uid)).Output()
		var exit *exec.ExitError
		switch {
		case errors.As(err, &exit) && exit.ExitCode() == 2:
			return "", false, true // getent: the key was not found
		case err != nil:
			t.Logf("no database oracle: getent: %v", err)
			return "", false, false
		}
		fields := strings.Split(strings.TrimRight(string(out), "\n"), ":")
		if len(fields) < 6 {
			t.Fatalf("getent passwd %d: %q", uid, out)
		}
		return fields[5], true, true
	case "darwin":
		out, err := exec.Command("dscacheutil", "-q", "user", "-a", "uid", strconv.Itoa(uid)).Output()
		if err != nil {
			t.Logf("no database oracle: dscacheutil: %v", err)
			return "", false, false
		}
		for _, line := range strings.Split(string(out), "\n") {
			if rest, ok := strings.CutPrefix(line, "dir: "); ok {
				return strings.TrimSpace(rest), true, true
			}
		}
		return "", false, true
	}
	return "", false, false
}
