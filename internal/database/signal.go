// Package database provides MySQL database connection management for GoArchive.
package database

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// SetupSignalHandler creates a context that is canceled on SIGTERM or SIGINT.
// Returns the context which will be cancelled when a shutdown signal is received.
// The database manager should listen to this context and close connections
// when the context is cancelled.
func SetupSignalHandler() context.Context {
	ctx, _ := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	return ctx
}

// SetupSignalHandlerWithCallback creates a context that is canceled on SIGTERM or SIGINT,
// and calls the provided callback function when a signal is received.
func SetupSignalHandlerWithCallback(callback func(os.Signal)) context.Context {
	return SetupSignalHandlerWithSecondSignal(callback, nil)
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
				if sig2 := <-sigChan; sig2 != nil {
					onSecond(sig2)
				}
			}
		case <-ctx.Done():
			// Context was cancelled elsewhere
		}
	}()

	return ctx
}
