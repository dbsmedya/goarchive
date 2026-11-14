//go:build unix

package controlpath

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// judgeOwnerAndMode is spec §4.1 rule 2 for a directory that exists: owned by the run's
// effective user, no group or other permission bit. A directory that is not is refused, not
// repaired.
func judgeOwnerAndMode(fi os.FileInfo, euid int) error {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("has no readable owner")
	}
	if int(st.Uid) != euid {
		return fmt.Errorf("is owned by uid %d, not by this run's uid %d", st.Uid, euid)
	}
	if perm := fi.Mode().Perm(); perm&0o077 != 0 {
		return fmt.Errorf("has mode %04o: group or other permission bits are set", perm)
	}
	return nil
}
