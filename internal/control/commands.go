package control

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// statusView is what the status commands render: the lines of `status` and the one line of
// `status-json`. The server builds it from the run's sources; the command table only asks.
type statusView interface {
	text() []string
	jsonLine() (string, error)
}

// commands is the command table (spec §4.4) over the store. It does no I/O but a stat of the
// sentinel file for resume's reply, and it logs nothing: a handler writes to the socket only
// (D17).
type commands struct {
	store    *tuning.Store
	view     statusView
	sentinel string
}

func newCommands(store *tuning.Store, view statusView, sentinelFile string) *commands {
	return &commands{store: store, view: view, sentinel: sentinelFile}
}

// execute carries out one request line for peer. The lexical form is judged first, then the name,
// then, inside the store and in the order of the reason words, applicability, the end of the run,
// the value, and the audit queue's room: so the word a failure gets does not depend on which check
// an implementation happens to run first (spec §4.3).
func (c *commands) execute(line string, peer tuning.Peer) reply {
	req, err := parseRequest(line)
	if err != nil {
		return errReply(reasonMalformed, err.Error())
	}
	switch req.kind {
	case kindSet:
		return c.set(req, peer)
	case kindGet:
		return c.get(req)
	}
	return c.action(req.name, peer)
}

func unknown(what, name string) reply {
	return errReply(reasonUnknown, fmt.Sprintf("%s %s is not part of this protocol: send help for the commands this run accepts", what, quoted(name)))
}

func refused(err error) reply {
	var r *tuning.Refusal
	if errors.As(err, &r) {
		return errReply(r.Reason, r.Message)
	}
	return errReply(tuning.ReasonInvalid, err.Error())
}

func (c *commands) action(name string, peer tuning.Peer) reply {
	switch name {
	case "help":
		return c.help()
	case "status":
		return okReply(c.view.text()...)
	case "status-json":
		line, err := c.view.jsonLine()
		if err != nil {
			return errReply(tuning.ReasonInvalid, "the status could not be rendered: "+err.Error())
		}
		return okReply(line)
	case "pause":
		return c.pause(peer)
	case "resume":
		return c.resume(peer)
	case "stop":
		return c.stop(peer)
	}
	return unknown("command", name)
}

func (c *commands) get(req request) reply {
	key, ok := tuning.ParseKey(req.name)
	if !ok {
		return unknown("setting", req.name)
	}
	if applies, why := c.store.Applies(key); !applies {
		return errReply(tuning.ReasonInapplicable, why)
	}
	return okReply(string(key) + "=" + c.store.Load().Text(key))
}

func (c *commands) set(req request, peer tuning.Peer) reply {
	key, ok := tuning.ParseKey(req.name)
	if !ok {
		return unknown("setting", req.name)
	}
	ch, err := c.store.Set(key, req.number, peer)
	if err != nil {
		return refused(err)
	}
	if ch.Unchanged {
		return okReply(fmt.Sprintf("%s: %s (unchanged)", key, ch.New))
	}
	return okReply(fmt.Sprintf("%s: %s -> %s, %s", key, ch.Old, ch.New, ch.Effect))
}

func (c *commands) pause(peer tuning.Peer) reply {
	res, err := c.store.Pause(peer)
	if err != nil {
		return refused(err)
	}
	if !res.Changed {
		return okReply("already paused since " + res.Since.UTC().Format(time.RFC3339))
	}
	return okReply("paused: the batch or recovery page in flight finishes first, then the run waits")
}

func (c *commands) resume(peer tuning.Peer) reply {
	res, err := c.store.Resume(peer)
	if err != nil {
		return refused(err)
	}
	held := c.sentinelPresent()
	switch {
	case res.Changed && held:
		return okReply(fmt.Sprintf("socket pause cleared; still paused: sentinel file %q is present", c.sentinel))
	case res.Changed:
		return okReply("resumed: the run continues at the next batch or recovery page")
	case held:
		return okReply(fmt.Sprintf("no socket pause was set; the run is held by sentinel file %q", c.sentinel))
	}
	return okReply("no socket pause was set")
}

func (c *commands) stop(peer tuning.Peer) reply {
	res, err := c.store.Stop(peer)
	if err != nil {
		return refused(err)
	}
	if res.Already {
		return okReply("already stopping")
	}
	return okReply("stop requested: the batch in flight finishes, then the run stops")
}

// sentinelPresent reports whether the sentinel file exists, as the pause gate judges it: a stat
// that succeeds. It is the one thing the table reads from the file system.
func (c *commands) sentinelPresent() bool {
	if c.sentinel == "" {
		return false
	}
	_, err := os.Stat(c.sentinel)
	return err == nil
}

// help is the actions, then the setters this run accepts: only those (spec §4.4). Its first body
// line is the protocol version.
func (c *commands) help() reply {
	line := func(name, what string) string { return fmt.Sprintf("%-34s %s", name, what) }
	body := []string{
		fmt.Sprintf("protocol %d", protocolVersion),
		line("help", "this list"),
		line("status", "the run's state as text"),
		line("status-json", "the run's state as one JSON object"),
		line("pause", "pause before the next batch or recovery page"),
		line("resume", "end a socket pause"),
		line("stop", "stop gracefully, as the first signal does"),
	}
	for _, k := range c.store.ApplicableKeys() {
		body = append(body, line(string(k)+settingArgument(k), settingHelp[k]))
	}
	body = append(body, line("<setting>=?", "read a setting"))
	return okReply(body...)
}

func settingArgument(k tuning.Key) string {
	if k == tuning.SleepSeconds || k == tuning.DeleteSleepSeconds {
		return "=<s>"
	}
	return "=<n>"
}

var settingHelp = map[tuning.Key]string{
	tuning.BatchSize:                 "roots per batch; never above the run's configured value",
	tuning.BatchDeleteSize:           "keys per DELETE; never above the run's configured value",
	tuning.SleepSeconds:              "seconds between batches",
	tuning.DeleteSleepSeconds:        "seconds between DELETEs",
	tuning.SecondsBehindSourceWithin: "lag tolerance in seconds; above the configured value only one step at a time",
	tuning.CheckInterval:             "seconds between re-checks while the gate holds",
}
