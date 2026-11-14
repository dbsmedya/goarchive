//go:build !linux && !darwin && !windows

package controlpath

import (
	"errors"
	"runtime"
)

// lookupHome has no environment-proof lookup on a platform that is not released: the
// default directory is refused there, and control.socket_dir is the way.
func lookupHome(int) (string, bool, error) {
	return "", false, errors.New("no user-database lookup is built for " + runtime.GOOS)
}
