// Package database provides MySQL database connection management for GoArchive.
package database

import (
	"context"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
)

// SetupGracefulShutdown wires a two-phase shutdown for long-running, mid-flight
// work (the copy→verify→delete loop).
//
//   - First SIGINT/SIGTERM: onFirst is called and the returned stop channel is
//     closed, but the work context is LEFT ALIVE. Callers treat the closed stop
//     channel as "finish the current batch, then stop at the boundary" — so a
//     single Ctrl-C never tears a batch mid-flight and leaves no non-terminal
//     (pending/copied/failed) rows behind. The exception is a replication hold,
//     which a first signal ends at once (internal/replication): the held batch's
//     logged roots stay for the next run's recovery.
//   - Second signal: onSecond is called and the work context is canceled,
//     aborting any in-flight operation. Whatever state this leaves is recoverable
//     by status-aware replay on the next run.
//   - A third signal hits the restored default handler and terminates the process.
//
// The context must be used for all DB work; the stop channel must be checked at batch boundaries
// (see stopRequested / followSleep).
//
// The first phase can also be requested from code: RequestFirst is the control socket's `stop`
// (spec §4.7). It returns at once by handing the request to the signal goroutine, which runs onFirst
// and closes the stop channel as it does for a signal, so a socket handler never writes the log line
// and never waits for it (D17). A real signal after it is the second phase.
func SetupGracefulShutdown(onFirst, onSecond func(os.Signal)) *Shutdown {
	sigChan := make(chan os.Signal, 2)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	s, dispatch := newShutdown(onFirst, onSecond, sigChan)

	go func() {
		// Once both phases are consumed, restore the default handler so a third
		// signal hard-terminates as an emergency escape hatch.
		defer signal.Stop(sigChan)
		dispatch()
	}()

	return s
}

// newShutdown builds a Shutdown and the dispatcher that runs its two phases from sigs and from
// RequestFirst; the caller starts the dispatcher. It is the one place the phases are decided, and
// it takes the signal channel as an argument so a test can queue both inputs before the dispatcher
// runs, which the real signal goroutine cannot be made to wait for.
func newShutdown(onFirst, onSecond func(os.Signal), sigs <-chan os.Signal) (*Shutdown, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Shutdown{ctx: ctx, stop: make(chan struct{}), first: make(chan struct{}, 1)}

	return s, func() {
		// Phase 1: cooperative stop — close stop, keep the work context alive. Whoever reserves
		// the phase first owns it: a request from code reserves it when RequestFirst accepts the
		// request, a signal when the dispatcher takes it. A signal that finds the phase reserved
		// came after an accepted stop, so it is the second phase, and the accepted request is the
		// first however the two were ordered in the select.
		select {
		case sig := <-sigs:
			if !s.reserved.CompareAndSwap(false, true) {
				<-s.first // sent right after the reservation, and never blocks: the channel has room
				if onFirst != nil {
					onFirst(codeRequest{})
				}
				close(s.stop)
				if onSecond != nil {
					onSecond(sig)
				}
				cancel()

				return
			}
			if onFirst != nil {
				onFirst(sig)
			}
			close(s.stop)
		case <-s.first:
			if onFirst != nil {
				onFirst(codeRequest{})
			}
			close(s.stop)
		case <-ctx.Done():
			return
		}

		// Phase 2: hard cancel on the next signal.
		select {
		case sig := <-sigs:
			if onSecond != nil {
				onSecond(sig)
			}
			cancel()
		case <-ctx.Done():
		}
	}
}

// Shutdown is the two-phase shutdown of a run.
type Shutdown struct {
	ctx  context.Context
	stop chan struct{}
	// first carries one accepted request from code to the dispatcher; reserved says the first phase
	// has an owner, a request from code or a signal, and is set once, by whoever gets there first.
	first    chan struct{}
	reserved atomic.Bool
}

// Context is the work context: it stays alive through the first phase and is cancelled by the second.
func (s *Shutdown) Context() context.Context { return s.ctx }

// Stop is the stop channel: it closes when the first phase begins.
func (s *Shutdown) Stop() <-chan struct{} { return s.stop }

// StopRequested reports whether the first phase has been taken, by a signal or from code.
func (s *Shutdown) StopRequested() bool {
	select {
	case <-s.stop:
		return true
	default:
		return false
	}
}

// RequestFirst asks for the first phase from code, as a signal does. It never blocks. It reserves the
// phase before it returns, so a request it accepts is the first phase and a signal that comes after it,
// dispatched before or after, is the second; a request after the phase is taken, by a signal or by an
// earlier request, does nothing. The callback and its log line stay with the signal goroutine.
func (s *Shutdown) RequestFirst() {
	if s.reserved.CompareAndSwap(false, true) {
		s.first <- struct{}{} // room for the one request that can ever reserve
	}
}

// codeRequest stands for a signal in onFirst when the first phase was asked for from code.
type codeRequest struct{}

func (codeRequest) String() string { return "control socket stop request" }
func (codeRequest) Signal()        {}
