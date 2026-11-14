// Package control is the runtime control socket of a run (TEL-1): the listener, its
// protocol, and the commands that change a run's settings while it works.
//
// So far the package holds the platform primitives only (the lock, the socket and identity
// checks, the peer's credentials). They are unexported, and nothing imports the package yet.
package control
