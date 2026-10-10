package cmd

import "fmt"

// errUnwired is what archive, purge, copy-only and dry-run return until they are wired to
// the 3.0 engine (internal/engine). The 2.x run path they called is deleted (R45), so
// running them would do nothing; their flags and help stay for the wiring to use.
func errUnwired(command string) error {
	return fmt.Errorf("%s is not wired to the 3.0 engine yet: this 3.0 development build cannot run jobs", command)
}
