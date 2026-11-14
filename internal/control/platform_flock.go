//go:build linux || darwin

package control

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// lockFile takes an exclusive, non-blocking lock on the open file f with flock(2) and keeps
// it until f is closed or the process ends, however it ends. The lock belongs to the open
// file, not to its name (spec fact 15), and conflicts with a lock taken through any other
// open of the same file, in this process or another. errLockHeld says another holds it.
func lockFile(f *os.File) error {
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var lockErr error
	if err := rc.Control(func(fd uintptr) {
		for {
			lockErr = unix.Flock(int(fd), unix.LOCK_EX|unix.LOCK_NB)
			if !errors.Is(lockErr, unix.EINTR) {
				return
			}
		}
	}); err != nil {
		return err
	}
	switch {
	case lockErr == nil:
		return nil
	case errors.Is(lockErr, unix.EWOULDBLOCK):
		return errLockHeld
	}
	return fmt.Errorf("lock %s: %w", f.Name(), lockErr)
}
