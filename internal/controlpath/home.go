package controlpath

import (
	"fmt"
	"path/filepath"
)

// NoDefaultError says that the effective user has no default socket directory: the user
// database has no entry for it, or the entry has no absolute home. An enabled run is refused
// before anything connects, and the remedy is in the message.
type NoDefaultError struct {
	UID    int    // the effective uid; negative where the platform has none (Windows)
	Reason string // what the user database said
}

func (e *NoDefaultError) Error() string {
	who := "the current account"
	if e.UID >= 0 {
		who = fmt.Sprintf("uid %d", e.UID)
	}
	return fmt.Sprintf("no default control socket directory for %s: %s; set control.socket_dir", who, e.Reason)
}

// HomeOf returns the home directory that the system's user database records for the
// effective user. The environment cannot change the answer: $HOME, $USER and %USERPROFILE%
// are not read, and there is no fallback to them. That rules out os/user's own answer on a
// build without cgo, which, when the database has no entry, returns a record made from them.
//
// The lookup is per platform. Linux reads /etc/passwd itself, so that every artifact behaves
// alike with or without cgo; a build with the osusergo tag would change macOS, and
// tests/harness checks that none sets it. macOS asks libSystem through os/user, with or
// without cgo. Windows takes the profile directory of the process token.
//
// A user with no entry, or whose entry has no absolute home, has no default.
func HomeOf(euid int) (string, error) {
	home, found, err := lookupHome(euid)
	switch {
	case err != nil:
		return "", &NoDefaultError{UID: euid, Reason: "the user database could not be read: " + err.Error()}
	case !found:
		return "", &NoDefaultError{UID: euid, Reason: "the user database has no entry for it"}
	}
	return judgeHome(euid, home)
}

// judgeHome accepts an absolute home and refuses an empty or relative one.
func judgeHome(euid int, home string) (string, error) {
	switch {
	case home == "":
		return "", &NoDefaultError{UID: euid, Reason: "the user database records no home directory"}
	case !filepath.IsAbs(home):
		return "", &NoDefaultError{UID: euid, Reason: fmt.Sprintf("the user database records a relative home directory %q", home)}
	}
	return home, nil
}
