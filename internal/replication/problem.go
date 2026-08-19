// Package replication applies goarchive's gating policy to the replication
// facts read by github.com/dbsmedya/dbsgomysql/pkg/replication. The library
// owns fact acquisition; this package owns the policy — which channels are
// gated, what tolerance applies, and how a failing fleet holds the job.
package replication

import (
	"fmt"
	"strconv"
	"strings"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"
)

// checkChannelNotFound identifies a problem goarchive raises itself: a channel
// the operator configured is absent from the server's snapshot. It is a
// goarchive-local ID and is never minted as a library Finding — the library
// documents Finding.Check as a stable identifier from its own catalog.
const checkChannelNotFound = "CHANNEL_NOT_FOUND"

// defaultChannelLabel is how the default (unnamed) channel is spelled in
// operator-facing text, so it is never an invisible empty string.
const defaultChannelLabel = "<default>"

// problem is one reason the replication gate holds the job. It is goarchive's
// own type: library findings convert into it losslessly, and goarchive-local
// causes such as a missing channel use it too.
type problem struct {
	// check is a library catalog ID, or checkChannelNotFound.
	check string
	// message is the human-readable summary, from the library or minted here.
	message string
	// channels names the affected channels in the server's own spelling, where
	// the empty name is the default channel. It is empty for server-scoped
	// checks.
	channels []string
	// facts is the typed payload that caused the problem: a
	// dbsrepl.ChannelStatus for the per-channel library checks, a
	// channelNotFoundFacts for checkChannelNotFound, and whatever the library
	// supplied for everything else.
	facts any
}

// channelNotFoundFacts records what was asked for and what the server actually
// reported, so the rendered line can show both.
type channelNotFoundFacts struct {
	// configured is the requested channel name; "" is the default channel.
	configured string
	// observed lists the channel names the server reported, in server order.
	observed []string
}

// fromFinding converts a library finding without losing information: the check
// ID, message, channel names and typed facts payload all survive.
func fromFinding(f dbsrepl.Finding) problem {
	return problem{
		check:    f.Check,
		message:  f.Message,
		channels: f.Channels,
		facts:    f.Facts,
	}
}

// newChannelNotFound reports a configured channel that the server's snapshot
// does not contain. observed carries the names the server did report.
func newChannelNotFound(configured string, observed []string) problem {
	return problem{
		check: checkChannelNotFound,
		message: fmt.Sprintf(
			"Configured replication channel %s is not present on the server",
			renderChannel(configured),
		),
		channels: []string{configured},
		facts:    channelNotFoundFacts{configured: configured, observed: observed},
	}
}

// renderChannel spells a channel name for operators, turning the default
// channel's empty name into a visible label.
func renderChannel(name string) string {
	if name == "" {
		return defaultChannelLabel
	}

	return name
}

// sanitizeLine collapses newlines and carriage returns to spaces so that no
// server-supplied text can split one log line into several.
func sanitizeLine(s string) string {
	return strings.NewReplacer("\n", " ", "\r", " ").Replace(s)
}

// renderProblems renders every problem, in the order given, into ONE sanitized
// physical line. Assembly order is evaluate's job; this function preserves it.
func renderProblems(ps []problem, tolerance int64) string {
	parts := make([]string, 0, len(ps))
	for _, p := range ps {
		parts = append(parts, renderProblem(p, tolerance))
	}

	return strings.Join(parts, "; ")
}

// renderProblem renders one problem. Every check that has no dedicated shape —
// REPLICATION_CONFIGURED included — falls through to the default branch, which
// assumes neither channels nor a typed facts payload. A dedicated shape whose
// facts are not the type it expects falls through there too, so an unexpected
// payload degrades to a readable line rather than panicking.
func renderProblem(p problem, tolerance int64) string {
	switch p.check {
	case dbsrepl.IDReplicationChannelsRunning:
		if cs, ok := p.facts.(dbsrepl.ChannelStatus); ok {
			return renderRunning(cs)
		}
	case dbsrepl.IDSecondsBehindSourceWithin:
		if cs, ok := p.facts.(dbsrepl.ChannelStatus); ok {
			return renderLag(cs, tolerance)
		}
	case checkChannelNotFound:
		if facts, ok := p.facts.(channelNotFoundFacts); ok {
			return renderNotFound(facts)
		}
	}

	return sanitizeLine(p.check + ": " + p.message)
}

// renderRunning shows the receiver and applier states, plus each last-error
// pair that carries anything. A deliberately stopped channel records no error,
// so an omitted pair is normal rather than missing information.
func renderRunning(cs dbsrepl.ChannelStatus) string {
	var b strings.Builder

	fmt.Fprintf(&b, "channel %s: io=%s sql=%s",
		renderChannel(cs.ChannelName), sanitizeLine(cs.IORunning), sanitizeLine(cs.SQLRunning))

	if cs.LastIOErrno != 0 || cs.LastIOError != "" {
		fmt.Fprintf(&b, " last_io_err=%d %q", cs.LastIOErrno, sanitizeLine(cs.LastIOError))
	}
	if cs.LastSQLErrno != 0 || cs.LastSQLError != "" {
		fmt.Fprintf(&b, " last_sql_err=%d %q", cs.LastSQLErrno, sanitizeLine(cs.LastSQLError))
	}

	return b.String()
}

// renderLag shows the reported estimate against the configured tolerance. An
// unknown estimate renders as NULL, which carries no seconds suffix because it
// is not a duration.
func renderLag(cs dbsrepl.ChannelStatus, tolerance int64) string {
	lag := "NULL"
	if cs.SecondsBehindSource.Valid {
		lag = strconv.FormatInt(cs.SecondsBehindSource.Int64, 10) + "s"
	}

	return fmt.Sprintf("channel %s: lag=%s tolerance=%ds",
		renderChannel(cs.ChannelName), lag, tolerance)
}

// renderNotFound shows the requested channel against the ones the server
// reports, so a typo is diagnosable from the log line alone.
func renderNotFound(facts channelNotFoundFacts) string {
	observed := "none"
	if len(facts.observed) > 0 {
		names := make([]string, 0, len(facts.observed))
		for _, name := range facts.observed {
			names = append(names, renderChannel(name))
		}
		observed = strings.Join(names, ", ")
	}

	return fmt.Sprintf("channel %s not found (server reports: %s)",
		renderChannel(facts.configured), observed)
}
