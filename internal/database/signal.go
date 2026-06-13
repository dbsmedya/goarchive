// Package database provides MySQL database connection management for GoArchive.
package database

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// SetupSignalHandler creates a context that is canceled on SIGTERM or SIGINT.
// Returns the context which will be cancelled when a shutdown signal is received.
// The database manager should listen to this context and close connections
// when the context is cancelled.
func SetupSignalHandler() context.Context {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	context.AfterFunc(ctx, stop)
	return ctx
}

// SetupSignalHandlerWithCallback creates a context that is canceled on SIGTERM or SIGINT,
// and calls the provided callback function when a signal is received.
func SetupSignalHandlerWithCallback(callback func(os.Signal)) context.Context {
	return SetupSignalHandlerWithSecondSignal(callback, nil)
}

// SetupGracefulShutdown wires a two-phase shutdown for long-running, mid-flight
// work (the copy→verify→delete loop).
//
//   - First SIGINT/SIGTERM: onFirst is called and the returned stop channel is
//     closed, but the work context is LEFT ALIVE. Callers treat the closed stop
//     channel as "finish the current batch, then stop at the boundary" — so a
//     single Ctrl-C never tears a batch mid-flight and leaves no non-terminal
//     (pending/copied/failed) rows behind.
//   - Second signal: onSecond is called and the work context is canceled,
//     aborting any in-flight operation. Whatever state this leaves is recoverable
//     by status-aware replay on the next run.
//   - A third signal hits the restored default handler and terminates the process.
//
// The returned context must be used for all DB work; the stop channel must be
// checked at batch boundaries (see stopRequested / interruptibleSleep).
func SetupGracefulShutdown(onFirst, onSecond func(os.Signal)) (context.Context, <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	stop := make(chan struct{})

	sigChan := make(chan os.Signal, 2)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		// Once both phases are consumed, restore the default handler so a third
		// signal hard-terminates as an emergency escape hatch.
		defer signal.Stop(sigChan)

		// Phase 1: cooperative stop — close stop, keep the work context alive.
		select {
		case sig := <-sigChan:
			if onFirst != nil {
				onFirst(sig)
			}
			close(stop)
		case <-ctx.Done():
			return
		}

		// Phase 2: hard cancel on the next signal.
		select {
		case sig := <-sigChan:
			if onSecond != nil {
				onSecond(sig)
			}
			cancel()
		case <-ctx.Done():
		}
	}()

	return ctx, stop
}

// SetupSignalHandlerWithSecondSignal creates a context that is canceled on first SIGTERM/SIGINT.
// onFirst is called on the first signal before cancellation.
// onSecond is called if a second signal is received after cancellation.
func SetupSignalHandlerWithSecondSignal(onFirst, onSecond func(os.Signal)) context.Context {
	ctx, cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 2)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		defer signal.Stop(sigChan)
		select {
		case sig := <-sigChan:
			if onFirst != nil {
				onFirst(sig)
			}
			cancel()
			if onSecond != nil {
				timer := time.NewTimer(30 * time.Second)
				defer timer.Stop()
				select {
				case sig2 := <-sigChan:
					if sig2 != nil {
						onSecond(sig2)
					}
				case <-timer.C:
				}
			}
		case <-ctx.Done():
			// Context was cancelled elsewhere
		}
	}()

	return ctx
}
