package control

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

// peerUID returns the effective uid and the pid that the kernel recorded for the process on
// the other end of c when it connected (SO_PEERCRED). The connection must be a Unix one.
func peerUID(c net.Conn) (uid, pid int, err error) {
	uc, ok := c.(*net.UnixConn)
	if !ok {
		return 0, 0, fmt.Errorf("%w: %T is not a Unix connection", errNoPeerCredentials, c)
	}
	rc, err := uc.SyscallConn()
	if err != nil {
		return 0, 0, err
	}
	var cred *unix.Ucred
	var credErr error
	if err := rc.Control(func(fd uintptr) {
		cred, credErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return 0, 0, err
	}
	if credErr != nil {
		return 0, 0, fmt.Errorf("read peer credentials: %w", credErr)
	}
	return int(cred.Uid), int(cred.Pid), nil
}
