package database

import (
	"fmt"
	"regexp"
	"strings"
)

// Global is a server global a test emulates (M7c-F6): under goarchive a global reaches each
// new session first and the pins apply on top of it, so OpenPool layers a Global under the
// pins the same way. Value is SQL text, quoted as a SET would take it ("'ANSI'", "1048576").
// Manager passes none.
//
//   - sql_mode becomes the base of the auto-zero pin (autoZeroMode), as a real global is the
//     session's starting mode. Its Value must be one single-quoted literal without
//     backslashes, so it cannot change the expression around it.
//   - time_zone, autocommit, sql_notes and sql_select_limit are refused: goarchive pins each
//     to a constant, which a global cannot change.
//   - wait_timeout is refused: goarchive raises it to at least 8 h over the global, and the
//     package's own tests and the wait_timeout_* matrix rows cover that.
//   - max_error_count 0 is refused: warningsConnector raises a session at 0, so the value
//     would be silently undone. Any other value reaches the session unchanged, as a real
//     global's would.
//   - charset and collation are refused: they are the driver's own DSN parameters.
//   - Any other global is added to the DSN's parameters. The driver sends them in one SET in
//     map order, so a global must not refer to another one.
type Global struct {
	Name, Value string
}

var (
	globalName   = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	quotedString = regexp.MustCompile(`^'(?:[^'\\]|'')*'$`)
)

// applyGlobals layers globals into params, a pinned DSN's parameters. It changes params only
// when every global is accepted.
func applyGlobals(params map[string]string, globals []Global) error {
	set := make(map[string]string, len(globals))
	for _, g := range globals {
		if !globalName.MatchString(g.Name) {
			return fmt.Errorf("emulated global %q: not a variable name", g.Name)
		}
		name := strings.ToLower(g.Name)
		switch name {
		case "sql_mode":
			if !quotedString.MatchString(g.Value) {
				return fmt.Errorf("emulated global %s: %s is not one single-quoted literal without backslashes", g.Name, g.Value)
			}
			set[name] = autoZeroMode(g.Value)
		case "time_zone", "autocommit", "sql_notes", "sql_select_limit":
			return fmt.Errorf("emulated global %s: goarchive pins it on every session, so a server global cannot change it", g.Name)
		case "wait_timeout":
			return fmt.Errorf("emulated global %s: goarchive raises it over the server's global; test the pin itself", g.Name)
		case "max_error_count":
			if strings.TrimSpace(g.Value) == "0" {
				return fmt.Errorf("emulated global %s: 0 is raised by the connector's check, so it would be undone", g.Name)
			}
			set[name] = g.Value
		case "charset", "collation":
			return fmt.Errorf("emulated global %s: a driver DSN parameter, not a session variable", g.Name)
		default:
			set[name] = g.Value
		}
	}
	for name, value := range set {
		params[name] = value
	}
	return nil
}
