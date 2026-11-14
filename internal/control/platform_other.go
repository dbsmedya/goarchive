//go:build !linux && !darwin && !windows

package control

import (
	"errors"
	"net"
	"os"
	"runtime"
)

// A platform that is not released has no lock and no peer credentials: a server cannot start
// there (fail closed), and the package still builds.

func lockFile(*os.File) error {
	return errors.New("file locks are not built for " + runtime.GOOS)
}

func peerUID(net.Conn) (uid, pid int, err error) {
	return 0, 0, errNoPeerCredentials
}
