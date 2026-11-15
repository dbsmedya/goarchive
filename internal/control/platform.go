package control

import (
	"errors"
	"os"
)

// errLockHeld says that another open file holds the lock: another process serves this
// socket, or another descriptor of this one does (spec §4.2 Start step 3).
var errLockHeld = errors.New("the lock is held by another open file")

// errNoPeerCredentials says that the platform gives no credentials for a connection, or that
// the connection is not a Unix one. A caller that needs them refuses the connection; Windows
// has none, and the same-user rule is not enforced there (spec §4.1).
var errNoPeerCredentials = errors.New("no peer credentials are available for this connection")

// isSocket reports whether fi, the result of an Lstat, identifies a socket. Lstat does not
// follow links, so a symbolic link to a socket is not one. On Windows, Go maps an AF_UNIX
// reparse point to a socket (go1.26.8, os/types_windows.go).
func isSocket(fi os.FileInfo) bool { return fi.Mode().Type() == os.ModeSocket }
