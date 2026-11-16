package control

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// Sink is where the audit emitter writes. The command adapts its logger to it: an entry goes past
// the level filter, marked live as a banner is, and the console tee shows warnings and live
// entries as it does for those. Entry may be slow; nothing but the emitter calls it.
type Sink interface {
	Entry(level tuning.Level, msg string, keysAndValues ...any)
}

type nopSink struct{}

func (nopSink) Entry(tuning.Level, string, ...any) {}

// emitter is the one place the control server writes output (spec §4.5, D17). One goroutine takes
// events from the store's bounded audit queue, in revision order, and writes each as its entry; a
// tolerance raise above the configured value also goes through the Banner as a WARNING line.
// Notes (refusals, refused peers, accept errors, panics) share the queue's free places: when there
// is none, or after the cutoff, a note is counted and not queued, and the count is written once
// the emitter catches up. A place of the queue comes back only when the emitter has written the
// event it held, so an accepted change always has its place and the queue cannot grow.
type emitter struct {
	store  *tuning.Store
	audit  *tuning.Audit
	sink   Sink
	banner func(string)

	// admit orders a note's admission against the cutoff: a note holds the read lock from its
	// check of closed to its put, and the cutoff takes the write lock, so that once it returns no
	// note can add to the queue and what the emitter must still write is a finite list. The queue
	// is never closed under a sender.
	admit  sync.RWMutex
	closed bool

	dropped atomic.Int64

	startOnce, stopOnce sync.Once
	started             atomic.Bool
	stop, done          chan struct{}
}

func newEmitter(store *tuning.Store, sink Sink, banner func(string)) *emitter {
	if sink == nil {
		sink = nopSink{}
	}
	return &emitter{
		store: store, audit: store.Audit(), sink: sink, banner: banner,
		stop: make(chan struct{}), done: make(chan struct{}),
	}
}

// start runs the emitter's goroutine.
func (e *emitter) start() {
	e.startOnce.Do(func() {
		e.started.Store(true)
		go e.run()
	})
}

// note queues a best-effort entry in a free place of the queue. It never blocks and never writes.
func (e *emitter) note(level tuning.Level, peer tuning.Peer, text string) {
	e.admit.RLock()
	defer e.admit.RUnlock()
	if e.closed || !e.audit.Reserve() {
		e.dropped.Add(1)
		return
	}
	e.audit.Put(tuning.Event{Kind: tuning.KindNote, Level: level, Text: text, Peer: peer})
}

// cutoff stops notes from being queued. It is the diagnostics cutoff of the end (spec §4.2), taken
// with the store's terminal transition: from here nothing can lengthen what drain writes.
func (e *emitter) cutoff() {
	e.admit.Lock()
	e.closed = true
	e.admit.Unlock()
}

func (e *emitter) cutOff() bool {
	e.admit.RLock()
	defer e.admit.RUnlock()
	return e.closed
}

// drain asks the emitter to write what is queued, then the count of notes dropped, and stop, and
// waits for it. The terminal transition and the cutoff come first, so the list is finite. It may be
// called again.
func (e *emitter) drain() {
	if !e.started.Load() {
		return
	}
	e.stopOnce.Do(func() { close(e.stop) })
	<-e.done
}

func (e *emitter) run() {
	defer close(e.done)
	events := e.audit.Events()
	for {
		select {
		case ev := <-events:
			e.write(ev)
			e.audit.Release()
			// Once the cutoff is taken the one count line is the drain's, written last.
			if len(events) == 0 && !e.cutOff() {
				e.reportDropped()
			}
		case <-e.stop:
			for {
				select {
				case ev := <-events:
					e.write(ev)
					e.audit.Release()
				default:
					e.reportDropped()
					return
				}
			}
		}
	}
}

// reportDropped writes the count of notes that found no place, once, and starts it again.
func (e *emitter) reportDropped() {
	if n := e.dropped.Swap(0); n > 0 {
		e.sink.Entry(tuning.LevelWarn, fmt.Sprintf("%d control socket log entries were not logged because the log was not keeping up", n))
	}
}

// peerText names who asked, as far as the platform says: the uid where it gives one, the pid where
// it gives one.
func peerText(p tuning.Peer) string {
	s := "control socket"
	if p.UID >= 0 {
		s += fmt.Sprintf(", uid %d", p.UID)
	}
	if p.PID > 0 {
		s += fmt.Sprintf(", pid %d", p.PID)
	}
	return s
}

func peerFields(p tuning.Peer) []any {
	var kv []any
	if p.UID >= 0 {
		kv = append(kv, "uid", p.UID)
	}
	if p.PID > 0 {
		kv = append(kv, "pid", p.PID)
	}
	return kv
}

func (e *emitter) write(ev tuning.Event) {
	at := ev.At.UTC().Format(time.RFC3339)
	switch ev.Kind {
	case tuning.KindNote:
		e.sink.Entry(ev.Level, ev.Text, peerFields(ev.Peer)...)
	case tuning.KindSet:
		msg := fmt.Sprintf("Runtime setting changed: %s %s -> %s (%s); %s", ev.Key, ev.Old, ev.New, peerText(ev.Peer), ev.Key.Effect())
		kv := append([]any{"setting", string(ev.Key), "old", ev.Old, "new", ev.New, "revision", ev.Revision, "accepted_at", at}, peerFields(ev.Peer)...)
		e.sink.Entry(tuning.LevelInfo, msg, kv...)
		e.warnIfRaised(ev)
	case tuning.KindPause, tuning.KindResume, tuning.KindStop:
		msg := fmt.Sprintf("Runtime control: %s requested (%s)", ev.Kind, peerText(ev.Peer))
		kv := append([]any{"action", string(ev.Kind), "revision", ev.Revision, "accepted_at", at}, peerFields(ev.Peer)...)
		e.sink.Entry(tuning.LevelInfo, msg, kv...)
	}
}

// warnIfRaised reports a raise of the lag tolerance or of a group bound above its configured value as a run banner
// is (spec §4.6 and §4.3 Loud): one WARNING line through the Banner. A lowering, a restore and a raise up to the
// configured value are ordinary changes.
func (e *emitter) warnIfRaised(ev tuning.Event) {
	var consequence string
	switch ev.Key {
	case tuning.SecondsBehindSourceWithin:
		consequence = "replicas may fall further behind before deletes hold"
	case tuning.GroupApplierQueueWithin, tuning.GroupCertifierQueueWithin:
		consequence = "members of the group may fall further behind before deletes hold"
	default:
		return
	}
	if e.banner == nil {
		return
	}
	now, errNew := strconv.Atoi(ev.New)
	was, errOld := strconv.Atoi(ev.Old)
	configured, errConf := strconv.Atoi(e.store.Start().Text(ev.Key))
	if errNew != nil || errOld != nil || errConf != nil || now <= was || now <= configured {
		return
	}
	who := ""
	if ev.Peer.UID >= 0 {
		who = fmt.Sprintf(" (uid %d)", ev.Peer.UID)
	}
	e.banner(fmt.Sprintf("WARNING: %s raised to %d, above the configured %d, over the control socket%s: %s.", ev.Key, now, configured, who, consequence))
}
