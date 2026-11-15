package control

import (
	"errors"
	"syscall"
)

// IsRefused reports whether err is the refusal that a socket with no listener gets: ECONNREFUSED,
// and nothing else (spec §4.2 Start step 4). The client reads the same refusal as "no run listens
// here" (spec §4.11, exit 5).
func IsRefused(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED) || isPlatformRefused(err)
}
