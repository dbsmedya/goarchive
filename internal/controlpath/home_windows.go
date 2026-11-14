package controlpath

import "os/user"

// lookupHome takes the profile directory that Windows records for the process token's
// account, not %USERPROFILE% or %LOCALAPPDATA%. The uid is meaningless here.
func lookupHome(int) (string, bool, error) {
	u, err := user.Current()
	if err != nil {
		return "", false, err
	}
	return u.HomeDir, true, nil
}
