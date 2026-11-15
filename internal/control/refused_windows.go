package control

import (
	"errors"

	"golang.org/x/sys/windows"
)

// isPlatformRefused is Windows' own ECONNREFUSED, which the syscall package's constant is not.
// What Windows answers for a socket with no listener, and for one with a full queue, is measured
// in the Windows CI job (spec §4.2, ruling 12); until it is, this is the classification the
// design says applies.
func isPlatformRefused(err error) bool { return errors.Is(err, windows.WSAECONNREFUSED) }
