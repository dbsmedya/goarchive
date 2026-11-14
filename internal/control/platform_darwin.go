package control

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

// xucredVersion is XUCRED_VERSION of <sys/ucred.h>: the layout of the structure that
// LOCAL_PEERCRED fills in. Any other version is refused rather than read.
const xucredVersion = 0

// peerUID returns the effective uid of the process on the other end of c when it connected
// (LOCAL_PEERCRED) and its pid (LOCAL_PEERPID; 0 when the system does not give it). The
// connection must be a Unix one.
func peerUID(c net.Conn) (uid, pid int, err error) {
	uc, ok := c.(*net.UnixConn)
	if !ok {
		return 0, 0, fmt.Errorf("%w: %T is not a Unix connection", errNoPeerCredentials, c)
	}
	rc, err := uc.SyscallConn()
	if err != nil {
		return 0, 0, err
	}
	var cred *unix.Xucred
	var credErr error
	var peerPID int
	if err := rc.Control(func(fd uintptr) {
		cred, credErr = unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
		if credErr == nil {
			peerPID, _ = unix.GetsockoptInt(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERPID)
		}
	}); err != nil {
		return 0, 0, err
	}
	if credErr != nil {
		return 0, 0, fmt.Errorf("read peer credentials: %w", credErr)
	}
	if cred.Version != xucredVersion {
		return 0, 0, fmt.Errorf("read peer credentials: unknown credential version %d", cred.Version)
	}
	return int(cred.Uid), peerPID, nil
}
