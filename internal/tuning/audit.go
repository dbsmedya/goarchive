package tuning

import (
	"sync/atomic"
	"time"
)

// Peer is who made a request: the connection's user id and process id where the platform gives
// them. UID is negative when it is unknown (Windows has no peer credentials).
type Peer struct {
	UID int
	PID int
}

// Kind is what an audit event records.
type Kind string

const (
	KindSet    Kind = "set"
	KindPause  Kind = "pause"
	KindResume Kind = "resume"
	KindStop   Kind = "stop"
	// KindNote is a best-effort entry of the control server that is not a change: a refusal, a
	// refused peer, an accept error, a recovered panic, a lost identity. It takes a free place of
	// the queue like a change does, but a note that finds none is counted by its sender, not queued.
	KindNote Kind = "note"
)

// Level is how serious an entry is.
type Level uint8

const (
	LevelInfo Level = iota
	LevelWarn
	LevelError
)

// Event is one accepted change, captured under the writer lock and immutable: the revision it
// created, for a setting its key and the value it actually replaced and its new value, who asked
// and when. A KindNote carries its Level and Text instead, and no revision.
type Event struct {
	Kind     Kind
	Revision uint64
	Key      Key
	Old, New string
	Peer     Peer
	At       time.Time
	Level    Level
	Text     string
}

// Audit is the bounded queue between the store and the emitter that writes its events (D17). A
// place is reserved first, without blocking, and an event is put in it, without blocking: the
// channel holds as many events as there are places, and a place comes back only when the
// consumer has written the event it held. So an accepted change always has its place, the queue
// cannot grow, and nothing under the writer lock waits for output.
type Audit struct {
	events chan Event
	free   atomic.Int64
}

// NewAudit returns a queue of slots places (at least one).
func NewAudit(slots int) *Audit {
	if slots < 1 {
		slots = 1
	}
	a := &Audit{events: make(chan Event, slots)}
	a.free.Store(int64(slots))
	return a
}

// Reserve takes a place if one is free. It never blocks.
func (a *Audit) Reserve() bool {
	for {
		free := a.free.Load()
		if free <= 0 {
			return false
		}
		if a.free.CompareAndSwap(free, free-1) {
			return true
		}
	}
}

// Put places an event in a place the caller reserved. It never blocks.
func (a *Audit) Put(e Event) { a.events <- e }

// Events is the consumer's side. The consumer calls Release after it has written an event.
func (a *Audit) Events() <-chan Event { return a.events }

// Release returns one place, after the consumer wrote the event that held it.
func (a *Audit) Release() { a.free.Add(1) }
