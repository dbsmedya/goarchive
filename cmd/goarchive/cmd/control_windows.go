//go:build windows

package cmd

import (
	"errors"

	"golang.org/x/sys/windows"
)

// isQueueFull is what a connect to a live listener whose queue is full answers on Windows:
// WSAENOBUFS, at once, neither a timeout nor a refusal (measured in the Windows CI job, c-windows-1.md).
// It is the same fact as EAGAIN on Linux: the run is busy.
func isQueueFull(err error) bool { return errors.Is(err, windows.WSAENOBUFS) }
