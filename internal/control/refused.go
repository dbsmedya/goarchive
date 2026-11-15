package control

import (
	"errors"
	"syscall"
)

// isRefused reports whether err is the refusal that a socket with no listener gets: ECONNREFUSED,
// and nothing else (spec §4.2 Start step 4).
func isRefused(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED) || isPlatformRefused(err)
}
