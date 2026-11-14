package control

import (
	"errors"
	"fmt"
	"net"
	"os"

	"golang.org/x/sys/windows"
)

// lockFile takes an exclusive, non-blocking lock on the first byte of the open file f with
// LockFileEx and keeps it until f is closed or the process ends, however it ends. It conflicts
// with a lock taken through any other handle of the same file, in this process or another.
// errLockHeld says another holds it.
func lockFile(f *os.File) error {
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var lockErr error
	if err := rc.Control(func(fd uintptr) {
		lockErr = windows.LockFileEx(windows.Handle(fd),
			windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, new(windows.Overlapped))
	}); err != nil {
		return err
	}
	switch {
	case lockErr == nil:
		return nil
	case errors.Is(lockErr, windows.ERROR_LOCK_VIOLATION):
		return errLockHeld
	}
	return fmt.Errorf("lock %s: %w", f.Name(), lockErr)
}

// peerUID: Windows gives no peer credentials for a Unix socket, so the same-user rule is
// not enforced there (spec §4.1). It always returns errNoPeerCredentials.
func peerUID(net.Conn) (uid, pid int, err error) {
	return 0, 0, errNoPeerCredentials
}
