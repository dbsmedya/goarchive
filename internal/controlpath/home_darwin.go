package controlpath

import (
	"errors"
	"os/user"
	"strconv"
)

// lookupHome asks the system through os/user. On macOS that is libSystem (getpwuid_r),
// with or without cgo, and it does not read $USER or $HOME -- unless the osusergo tag is
// set, which tests/harness forbids.
func lookupHome(uid int) (string, bool, error) {
	u, err := user.LookupId(strconv.Itoa(uid))
	var unknown user.UnknownUserIdError
	switch {
	case errors.As(err, &unknown):
		return "", false, nil
	case err != nil:
		return "", false, err
	}
	return u.HomeDir, true, nil
}
