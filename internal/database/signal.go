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
	ctx, cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		select {
		case sig := <-sigChan:
			// Received shutdown signal
			_ = sig
			cancel()
		case <-ctx.Done():
			// Context was cancelled elsewhere
		}
	}()

	return ctx
}

// SetupSignalHandlerWithCallback creates a context that is canceled on SIGTERM or SIGINT,
// and calls the provided callback function when a signal is received.
func SetupSignalHandlerWithCallback(callback func(os.Signal)) context.Context {
	ctx, cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		select {
		case sig := <-sigChan:
			// Received shutdown signal, call callback then cancel
			if callback != nil {
				callback(sig)
			}
			cancel()
		case <-ctx.Done():
			// Context was cancelled elsewhere
		}
	}()

	return ctx
}
