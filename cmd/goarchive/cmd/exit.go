package cmd

import (
	"errors"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/dbsmedya/goarchive/v3/internal/engine"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
)

// Exit codes (spec §3.4; docs/README_OPERATIONS.md, Output and exit codes).
const (
	exitOK         = 0
	exitFailed     = 1
	exitUsage      = 2
	exitStopped    = 3
	exitNotStarted = 4
)

// usageError marks a usage or configuration error, found before anything connected.
type usageError struct{ err error }

func (e *usageError) Error() string { return e.err.Error() }
func (e *usageError) Unwrap() error { return e.err }

// usage wraps err as a usage error; nil stays nil. It is applied where the error is produced,
// never by the error's own type, so a late file error elsewhere stays a failure.
func usage(err error) error {
	if err == nil {
		return nil
	}
	return &usageError{err}
}

// errStopped is a run that stopped, without error, before the end of its where range. It is
// not printed: Main maps it to 3.
var errStopped = errors.New("stopped before the end of the where range")

// reportedError is an error already written to stderr in stderr's format (a run's closing
// entry on a JSON stderr log), so Main does not print it again (spec §3.4a).
type reportedError struct{ err error }

func (e *reportedError) Error() string { return e.err.Error() }
func (e *reportedError) Unwrap() error { return e.err }

// started is set when a command's RunE begins. cobra returns every error it finds before then
// (flags, arguments, required flags, an unknown command), so those are usage errors.
var started bool

// diagJSON is the job logger when its log is JSON on stderr: Main writes an error there as one
// entry instead of a plain line. Nil before a logger exists, and for any other log.
var diagJSON *logger.Logger

// runE marks the command started, then runs f.
func runE(f func(c *cobra.Command, args []string) error) func(*cobra.Command, []string) error {
	return func(c *cobra.Command, args []string) error {
		started = true
		return f(c, args)
	}
}

// exitCode maps an outcome to its exit code. Precedence: a clean stop (only ever returned
// without an error), then contention, then usage, then failure.
func exitCode(err error) int {
	if err == nil {
		return exitOK
	}
	if errors.Is(err, errStopped) {
		return exitStopped
	}
	var running *engine.JobRunningError
	var table *engine.TableLockedError
	var overlap *engine.OverlapError
	var schema *tracking.SchemaLockedError
	if errors.As(err, &running) || errors.As(err, &table) || errors.As(err, &overlap) || errors.As(err, &schema) {
		return exitNotStarted
	}
	var u *usageError
	if !started || errors.As(err, &u) {
		return exitUsage
	}
	return exitFailed
}

// finish prints err once, on stderr in stderr's format, and returns the exit code.
func finish(err error, stderr io.Writer) int {
	code := exitCode(err)
	if err == nil || code == exitStopped {
		return code
	}
	var done *reportedError
	switch {
	case errors.As(err, &done):
	case diagJSON != nil:
		diagJSON.Always().Errorw("Command failed", "error", err.Error())
		_ = diagJSON.Sync()
	default:
		_, _ = fmt.Fprintln(stderr, "Error:", err)
	}
	return code
}

// Main runs the CLI with args on the given streams and returns its exit code. Execute is Main
// on the process's arguments and streams.
func Main(args []string, stdout, stderr io.Writer) int {
	started, diagJSON = false, nil
	rootCmd.SetArgs(args)
	rootCmd.SetOut(stdout)
	rootCmd.SetErr(stderr)
	return finish(rootCmd.Execute(), stderr)
}
