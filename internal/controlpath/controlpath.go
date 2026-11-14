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
)

// sockSuffix follows the job name in the socket's file name; the lock is <job>.lock beside it.
const sockSuffix = ".sock"

// jobNameRule is the file-name rule for a job with control enabled (spec §4.1). It is an
// additional rule: the job-name rules of the configuration stay as they are for every job.
const jobNameRule = `[A-Za-z0-9_-][A-Za-z0-9_.-]*`

var jobName = regexp.MustCompile(`^` + jobNameRule + `$`)

// JobNameSafe reports whether name may be used as a file name: it has no separator, no
// leading dot, no space and no byte outside ASCII letters, digits, '_', '-' and '.'.
func JobNameSafe(name string) bool { return jobName.MatchString(name) }

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
	if !JobNameSafe(job) {
		return "", fmt.Errorf("job name %q cannot name a control socket: it must match %s", job, jobNameRule)
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
