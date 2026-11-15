// Package tuning is the run's settings store (TEL-1, spec §4.5 to §4.7): the six values an
// operator may change while a run works, the socket pause and the stop request, as one
// immutable snapshot that readers load without a lock and a single writer lock that
// linearizes every change.
//
// The engine and the replication gate read the store; only the control server writes it. A
// run without a control socket gets a store that never changes (Constant), so its behaviour is
// today's. The store never writes to a log, to stderr or to the network, and none of its
// operations waits for one: an accepted change leaves its audit event in a bounded queue
// (Audit) for an emitter to write, and is accepted only when the queue has a place for it.
package tuning
