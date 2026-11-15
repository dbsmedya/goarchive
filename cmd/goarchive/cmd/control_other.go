//go:build !windows

package cmd

import (
	"errors"
	"syscall"
)

// isQueueFull is what a connect to a live listener whose queue is full answers: on Linux EAGAIN, at
// once, which net.Error calls a timeout though nothing waited. (macOS refuses instead; there it reads as
// a stale socket, the window the design accepts, spec §4.2.)
func isQueueFull(err error) bool { return errors.Is(err, syscall.EAGAIN) }
