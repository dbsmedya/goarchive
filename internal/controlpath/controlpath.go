// Package controlpath decides where a run's control socket lives and whether a thing found
// there may be used. It imports nothing of goarchive, so that the configuration loader, the
// run and the client share one answer without a cycle (TEL-1, spec §4.1 and §4.2).
//
// The package creates, changes and removes nothing. Resolve reads the user database through
// HomeOf and nothing else; JudgeDir and JudgeSocket judge the result of an Lstat the caller
// made. Only the control server's Start creates a directory.
package controlpath

import (
	"fmt"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
)

// sockSuffix follows the job name in the socket's file name; the lock is <job>.lock beside it.
const sockSuffix = ".sock"

// jobNameRule is the file-name rule for a job with control enabled (spec §4.1). It is an
// additional rule: the job-name rules of the configuration stay as they are for every job.
const jobNameRule = `[A-Za-z0-9_-][A-Za-z0-9_.-]*`

var jobName = regexp.MustCompile(`^` + jobNameRule + `$`)

// JobNameSafe reports whether name may be used as a file name: it has no separator, no
// leading dot, no space and no byte outside ASCII letters, digits, '_', '-' and '.', and on
// Windows it is not a reserved device name.
func JobNameSafe(name string) bool { return JobNameError(name) == nil }

// JobNameError says why name cannot name a control socket, or returns nil. It is the file-name
// rule, and on Windows the reserved device names (ReservedOnWindows).
func JobNameError(name string) error {
	if !jobName.MatchString(name) {
		return fmt.Errorf("job name %q cannot name a control socket: it must match %s", name, jobNameRule)
	}
	if runtime.GOOS == "windows" {
		if device, ok := ReservedOnWindows(name); ok {
			return fmt.Errorf("job name %q is the reserved Windows device name %s (a name is compared up to its first \".\", "+
				"without regard to case), so <job>.sock and <job>.lock would name a device, not a file: rename the job", name, device)
		}
	}
	return nil
}

// windowsDevices are the names Windows reserves in every directory.
var windowsDevices = func() map[string]bool {
	m := map[string]bool{"CON": true, "PRN": true, "AUX": true, "NUL": true}
	for i := '1'; i <= '9'; i++ {
		m["COM"+string(i)], m["LPT"+string(i)] = true, true
	}
	return m
}()

// ReservedOnWindows reports whether name is a reserved Windows device name, and which: its text up
// to the first ".", compared without regard to case, is CON, PRN, AUX, NUL, COM1 to COM9 or LPT1 to
// LPT9. Microsoft's naming rules reserve the name also when an extension follows, so "nul.sock"
// names the device NUL. It is the list and nothing asked of the system, and it is pure: it answers
// the same on every platform, though only Windows refuses a job by it (JobNameError).
func ReservedOnWindows(name string) (device string, reserved bool) {
	stem, _, _ := strings.Cut(name, ".")
	device = strings.ToUpper(stem)
	return device, windowsDevices[device]
}

// MaxLen is the usable length of a socket path in bytes: the platform's sun_path less its
// terminating NUL. Go's SockaddrUnix refuses a name of len(raw.Path) bytes or more.
func MaxLen() int { return maxLen }

// CheckLength reports whether path fits a socket address. Bytes are counted, not characters.
// It is the one limit: the run and the client both call it, so they cannot disagree.
func CheckLength(path string) error {
	if n := len(path); n > maxLen {
		return fmt.Errorf("control socket path %q is %d bytes; the limit is %d bytes on this platform: "+
			"use a shorter control.socket_dir or a shorter job name", path, n, maxLen)
	}
	return nil
}

// Resolve returns the control socket's path for a job: <socket_dir>/<job>.sock, where an
// empty socketDir means <home>/.goarchive/run and <home> is the home that the system's user
// database records for euid (HomeOf; never $HOME). It returns "" and no error when control
// is not enabled, and then it looks nothing up, whatever the other arguments say.
//
// It makes no lookup for an explicit socketDir, creates nothing, and refuses a relative
// directory, a job name that is not a safe file name and a path over MaxLen.
func Resolve(enabled bool, socketDir, job string, euid int) (string, error) {
	if !enabled {
		return "", nil
	}
	if err := JobNameError(job); err != nil {
		return "", err
	}
	dir := socketDir
	switch {
	case dir == "":
		home, err := HomeOf(euid)
		if err != nil {
			return "", err
		}
		dir = filepath.Join(home, ".goarchive", "run")
	case !filepath.IsAbs(dir):
		return "", fmt.Errorf("control.socket_dir %q is not an absolute path", dir)
	}
	path := filepath.Join(dir, job+sockSuffix)
	if err := CheckLength(path); err != nil {
		return "", err
	}
	return path, nil
}
