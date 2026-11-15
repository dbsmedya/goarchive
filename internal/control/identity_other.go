//go:build !unix && !windows

package control

import (
	"errors"
	"os"
	"runtime"
)

// A platform that is not released has no identity: a server cannot start there (fail closed), and
// the package still builds.

func identityAt(string) (identity, error) {
	return identity{}, errors.New("file identity is not built for " + runtime.GOOS)
}

func identityOfFile(*os.File) (identity, error) {
	return identity{}, errors.New("file identity is not built for " + runtime.GOOS)
}
