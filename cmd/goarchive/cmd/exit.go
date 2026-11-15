package cmd

import (
	"errors"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/dbsmedya/goarchive/v3/internal/control"
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

// The codes of `goarchive control` (spec §4.11). It reuses 0 for an OK, 1 for the run's ERR and 2
// for a usage error; 3 and 4 describe a run and are never returned by it. These are new, and used
// by `control` only. 1 always means that a run answered ERR.
const (
	exitNoSocket        = 5 // nothing at the path, or a socket file that refuses: a run that was killed
	exitTimeout         = 6 // the client's own deadline passed: the connection, or the exchange
	exitMalformedReply  = 7 // the reply is not a well-formed OK or ERR
	exitRefusedEndpoint = 8 // the client's own check of the directory or the socket failed
	exitTransport       = 9 // any other failure to connect or to exchange
)

// controlError is a failure of `goarchive control` that carries its own exit code, so none falls
// through to 1 (spec §4.11): a run's ERR is a controlError with code 1, and the others are 5 to 9.
type controlError struct {
	code int
	err  error
}

func (e *controlError) Error() string { return e.err.Error() }
func (e *controlError) Unwrap() error { return e.err }

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
// without an error), then contention, then usage, then failure. Contention is a lock another
// session holds, a control socket in use by a live run (its lock held, or a socket that answered),
// or an unfinished-work refusal whose every blocker is live; a refusal held by a stopped job waits
// for that job's rerun, so it is a failure (EXC-1).
func exitCode(err error) int {
	if err == nil {
		return exitOK
	}
	if errors.Is(err, errStopped) {
		return exitStopped
	}
	var client *controlError
	if errors.As(err, &client) {
		return client.code
	}
	var running *engine.JobRunningError
	var table *engine.TableLockedError
	var schema *tracking.SchemaLockedError
	var socket *control.InUseError
	if errors.As(err, &running) || errors.As(err, &table) || errors.As(err, &schema) || errors.As(err, &socket) {
		return exitNotStarted
	}
	var overlap *engine.OverlapError
	if errors.As(err, &overlap) {
		if overlap.Live() {
			return exitNotStarted
		}
		return exitFailed
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
