package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbsmedya/goarchive/v3/internal/control"
	"github.com/dbsmedya/goarchive/v3/internal/controlpath"
)

// goarchive control: the client of a run's control socket (spec §4.9, §4.11). It spells one request
// line, checks where it is about to connect, sends the line, reads the reply to the end and prints
// it. The run judges every name; the client judges the shape of what it sends and the envelope of
// what comes back, and nothing inside the body.

// controlLimits are the client's bounds (spec §4.11): how long it may take to connect, how long the
// exchange may take once connected, and the longest reply it accepts, in bytes.
type controlLimits struct {
	dial, exchange time.Duration
	maxReply       int
}

// clientLimits are 5 seconds to connect, 10 for the exchange and 1 MiB of reply. A test shortens them.
var clientLimits = controlLimits{dial: 5 * time.Second, exchange: 10 * time.Second, maxReply: 1 << 20}

// controlSet and controlGet hold every -s/--set and -g/--get on the line: the protocol has no
// multi-key request, so the client counts them and refuses a second one.
var controlSet, controlGet []string

var controlCmd = &cobra.Command{
	Use:   "control <socket-path> (<action> | -s <key>=<value> | -g <key>)",
	Short: "Send one request to a run's control socket",
	Long:  controlLong(),
	Args:  cobra.ArbitraryArgs,
	RunE:  runE(runControl),
}

func init() {
	controlCmd.Flags().StringArrayVarP(&controlSet, "set", "s", nil, "change one setting: <key>=<value>")
	controlCmd.Flags().StringArrayVarP(&controlGet, "get", "g", nil, "read one setting: <key>")
	rootCmd.AddCommand(controlCmd)
}

func controlLong() string {
	var b strings.Builder
	b.WriteString(`Send one request to the runtime control socket of a running job and print the
reply. The socket path locates the run, and nothing else does: it is the path
that validate and dry-run print for the job, and that the run's log records.
The client reads no config file, so --config is refused.

  goarchive control <socket-path> <action>
  goarchive control <socket-path> -s|--set <key>=<value>
  goarchive control <socket-path> -g|--get <key>

A call takes exactly one of the three. Each call is one request: one change per
call, so each is accepted or refused on its own. Run it as the OS user that runs
the job.

The run judges the names. These are the actions and settings of the protocol; a
run lists only the settings it uses in its own help action:

`)
	for _, h := range control.HelpTable() {
		b.WriteString("  " + h.Line() + "\n")
	}
	b.WriteString(`
Examples:

  goarchive control /home/archiver/.goarchive/run/orders_archive.sock status-json
  goarchive control /home/archiver/.goarchive/run/orders_archive.sock -s batch_delete_size=200
  goarchive control /home/archiver/.goarchive/run/orders_archive.sock -g batch_size
  docker exec <container> goarchive control /work/control/orders_archive.sock status

Exit codes: 0 the run answered OK (the body is on stdout), 1 the run answered
ERR (Error: <reason>: <message> on stderr), 2 usage error, 5 no socket at the
path or nothing listening on it, 6 the client's own timeout (5 s to connect,
10 s for the exchange), 7 malformed reply, 8 the client refused the directory
or the socket, 9 any other failure to connect or to exchange. See
docs/README_OPERATIONS.md, Runtime control.`)
	return b.String()
}

func runControl(c *cobra.Command, args []string) error {
	if c.Flags().Changed("config") {
		return usage(errors.New("control finds the run by its socket path; --config has no effect"))
	}
	path, line, err := controlLine(args, controlSet, controlGet)
	if err != nil {
		return usage(err)
	}
	body, err := (controlClient{limits: clientLimits, euid: os.Geteuid()}).ask(path, line)
	if err != nil {
		return err
	}
	if _, err := io.WriteString(c.OutOrStdout(), body); err != nil {
		return &controlError{exitTransport, fmt.Errorf("could not write the reply: %w", err)}
	}
	return nil
}

// controlLine judges the command line and spells the request: an action as it is, -s k=v as k=v,
// -g k as k=? (spec §4.3). It checks the shape of what will be sent and nothing else; whether a name
// exists is the run's to say. Every error is a usage error, found before connecting.
func controlLine(args, sets, gets []string) (path, line string, err error) {
	switch {
	case len(sets) > 1:
		return "", "", errors.New("-s/--set may be given once per call: the protocol changes one setting per request, so each change is accepted or refused on its own")
	case len(gets) > 1:
		return "", "", errors.New("-g/--get may be given once per call: the protocol reads one setting per request")
	case len(args) == 0 || args[0] == "":
		return "", "", errors.New("control needs the run's socket path: goarchive control <socket-path> <action>")
	case len(args) > 2:
		return "", "", errors.New("too many arguments: control takes the socket path and one action; use -s <key>=<value> or -g <key> for a setting")
	}
	forms := len(sets) + len(gets)
	if len(args) == 2 {
		forms++
	}
	switch {
	case forms == 0:
		return "", "", errors.New("control needs an action, -s <key>=<value> or -g <key>")
	case forms > 1:
		return "", "", errors.New("give exactly one of an action, -s and -g")
	}

	path = args[0]
	switch {
	case len(sets) == 1:
		key, value, found := strings.Cut(sets[0], "=")
		if !found || key == "" || value == "" {
			return "", "", fmt.Errorf("-s needs <key>=<value> with a key and a value, got %q", sets[0])
		}
		line = sets[0]
	case len(gets) == 1:
		if gets[0] == "" || strings.Contains(gets[0], "=") {
			return "", "", fmt.Errorf("-g needs a setting name that is not empty and has no =, got %q", gets[0])
		}
		line = gets[0] + "=?"
	default:
		if strings.Contains(args[1], "=") {
			return "", "", fmt.Errorf("%q is not an action: use -s <key>=<value> to change a setting", args[1])
		}
		line = args[1]
	}
	if strings.Contains(line, "\n") {
		return "", "", errors.New("the request contains a newline: a request is one line")
	}
	if len(line)+1 > control.MaxRequest {
		return "", "", fmt.Errorf("the request is %d bytes with its line feed: a run reads at most %d", len(line)+1, control.MaxRequest)
	}
	if err := controlpath.CheckLength(path); err != nil {
		return "", "", err
	}
	return path, line, nil
}

// controlClient is one call's context: the bounds and the effective user whose ownership the
// endpoint must show.
type controlClient struct {
	limits controlLimits
	euid   int
}

// ask checks the endpoint, connects, sends the line and returns the body of an OK reply. Every
// failure is a *controlError with its exit code.
func (c controlClient) ask(path, line string) (string, error) {
	if err := checkEndpoint(path, c.euid); err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.limits.dial)
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", path)
	if err != nil {
		return "", classifyConnect(path, err)
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(c.limits.exchange))
	if _, err := io.WriteString(conn, line+"\n"); err != nil {
		return "", c.exchangeError(path, err)
	}
	reply, err := io.ReadAll(io.LimitReader(conn, int64(c.limits.maxReply)+1))
	if err != nil {
		return "", c.exchangeError(path, err)
	}
	return judgeReply(path, reply, c.limits.maxReply)
}

// exchangeError classifies a failure after the connection was made. Only the client's own deadline
// is exit 6; the rest is exit 9. Either way the request may have been applied before the failure
// (spec §4.3), and the message says how to find out.
func (c controlClient) exchangeError(path string, err error) *controlError {
	const unsure = "the request may or may not have been applied: read it back with -g or status-json"
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return &controlError{exitTimeout, fmt.Errorf("no complete reply from the control socket at %s within %v; %s", path, c.limits.exchange, unsure)}
	}
	return &controlError{exitTransport, fmt.Errorf("the exchange with the control socket at %s failed: %v; %s", path, err, unsure)}
}

// isMissing is "nothing is at this path": the name is not there, or a part of the path is not a
// directory.
func isMissing(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR)
}

const noSocketMeaning = "the job is not running, control is not enabled for it, or the path is wrong"

func noSocket(path string) *controlError {
	return &controlError{exitNoSocket, fmt.Errorf("no control socket at %s: %s", path, noSocketMeaning)}
}

func refusedEndpoint(path, found string) *controlError {
	return &controlError{exitRefusedEndpoint, fmt.Errorf("refusing to connect to %s: %s", path, found)}
}

// checkEndpoint looks at where the client is about to connect, without following links and without
// changing anything (spec §4.9): the socket's directory must be a real directory owned by euid with
// no group or other permission bit (the judging function the run's own start uses), and the path
// itself a socket, not a symbolic link and not another type. A path with nothing at it is no socket
// (exit 5); a failed check is exit 8 and nothing is connected, sent, created, repaired or changed.
func checkEndpoint(path string, euid int) error {
	dir := filepath.Dir(path)
	fi, err := os.Lstat(dir)
	switch {
	case isMissing(err):
		return noSocket(path)
	case err != nil:
		return refusedEndpoint(path, "its directory cannot be examined: "+err.Error())
	}
	if err := controlpath.JudgeDir(fi, euid); err != nil {
		return refusedEndpoint(path, fmt.Sprintf("its directory %s %v", dir, err))
	}
	fi, err = os.Lstat(path)
	switch {
	case isMissing(err):
		return noSocket(path)
	case err != nil:
		return refusedEndpoint(path, "it cannot be examined: "+err.Error())
	}
	if err := controlpath.JudgeSocket(fi); err != nil {
		return refusedEndpoint(path, "it "+err.Error())
	}
	return nil
}

// classifyConnect maps the error of a failed connect to the client's failure (spec §4.11). A refused
// connection or a missing path is "no run answers here" (5). Only the client's own passed deadline
// is a timeout (6): an error that merely reports itself as one is not. A live listener whose queue is
// full answers at once, EAGAIN on Linux (which net.Error calls a timeout though nothing waited) and
// WSAENOBUFS on Windows (isQueueFull): it is busy, and an ordinary transport failure (9), as is a
// permission error and any other.
func classifyConnect(path string, err error) *controlError {
	switch {
	case isMissing(err):
		return noSocket(path)
	case control.IsRefused(err):
		return &controlError{exitNoSocket, fmt.Errorf("the control socket at %s is stale: nothing listens on it, so the run that made it has ended (killed?); %s", path, noSocketMeaning)}
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, os.ErrDeadlineExceeded):
		return &controlError{exitTimeout, fmt.Errorf("no connection to the control socket at %s was made within the client's connect deadline", path)}
	case isQueueFull(err):
		return &controlError{exitTransport, fmt.Errorf("the control socket at %s is busy: its connection queue is full; try again", path)}
	case errors.Is(err, fs.ErrPermission):
		return &controlError{exitTransport, fmt.Errorf("permission denied on the control socket at %s: run goarchive control as the OS user that runs the job", path)}
	}
	return &controlError{exitTransport, fmt.Errorf("could not connect to the control socket at %s: %v", path, err)}
}

// judgeReply judges the envelope of a reply and nothing inside the body: it does not know what a
// command's body should hold, since the names are the run's. The first line is OK, or ERR with a
// reason word and a message and nothing after it; the last byte is LF; the reply is no longer than
// max. Nothing is printed unless the envelope is whole. It cannot see a reply cut exactly at a line
// boundary (spec §4.3), so exit 0 means a well-formed OK reply, not a complete snapshot.
func judgeReply(path string, reply []byte, max int) (string, error) {
	malformed := func(why string) error {
		return &controlError{exitMalformedReply, fmt.Errorf("the reply from the control socket at %s is malformed: %s", path, why)}
	}
	switch {
	case len(reply) == 0:
		return "", malformed("it is empty")
	case len(reply) > max:
		return "", malformed(fmt.Sprintf("it is longer than %d bytes", max))
	case reply[len(reply)-1] != '\n':
		return "", malformed("it does not end with a line feed")
	}
	first, rest, _ := strings.Cut(string(reply), "\n")
	switch {
	case first == "OK":
		return rest, nil
	case strings.HasPrefix(first, "ERR "):
		if rest != "" {
			return "", malformed("an ERR line is followed by more lines")
		}
		reason, message, found := strings.Cut(strings.TrimPrefix(first, "ERR "), " ")
		if !found || message == "" || !isReasonWord(reason) {
			return "", malformed("an ERR line needs a reason word and a message")
		}
		// Any well-formed ERR is a refusal, a reason word this client does not know included.
		return "", &controlError{exitFailed, fmt.Errorf("%s: %s", reason, message)}
	}
	return "", malformed("its first line is neither OK nor ERR <reason> <message>")
}

// isReasonWord is the grammar's reason: one or more lower-case letters (spec §4.3).
func isReasonWord(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < 'a' || r > 'z' {
			return false
		}
	}
	return true
}
