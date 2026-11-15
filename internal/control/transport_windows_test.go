//go:build windows

package control

import (
	"errors"
	"testing"

	"golang.org/x/sys/windows"
)

// What Windows was measured to do to a connect and to a close (CI run 43669762242, c-windows-1.md),
// pinned. These pass or fail only on Windows; the other platforms' cases are in transport_test.go.

// pinWindowsStale: a socket file with no listener is refused with WSAECONNREFUSED, and isRefused, which
// the run's start and the client read it by, recognises that error as it really comes out of a dial.
func pinWindowsStale(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, windows.WSAECONNREFUSED) || !isRefused(err) {
		t.Fatalf("got %v, want WSAECONNREFUSED, which isRefused recognises", err)
	}
}

// pinWindowsSaturated: a full queue is WSAENOBUFS ("the system lacked sufficient buffer space or a queue
// was full"), not a refusal and not a timeout, so the start's classifier calls it undecided: there is
// no macOS-like window on Windows in which a live listener reads as a stale socket.
func pinWindowsSaturated(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, windows.WSAENOBUFS) {
		t.Fatalf("got %v, want WSAENOBUFS", err)
	}
	if isRefused(err) {
		t.Fatalf("a full queue reads as a refusal: %v", err)
	}
	if isTimeout(err) {
		t.Fatalf("a full queue reports itself as a timeout: %v", err)
	}
	if got := classifyStartAttempt(err); got != startUndecided {
		t.Fatalf("classifyStartAttempt(%v) = %v, want undecided", err, got)
	}
}

// pinWindowsReset: closing with unread input is a reset, WSAECONNRESET, as on Linux.
func pinWindowsReset(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, windows.WSAECONNRESET) {
		t.Fatalf("got %v, want WSAECONNRESET", err)
	}
}
