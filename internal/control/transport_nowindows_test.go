//go:build !windows

package control

import "testing"

// The Windows pins (transport_windows_test.go) have nothing to check elsewhere.
func pinWindowsStale(*testing.T, error)     {}
func pinWindowsSaturated(*testing.T, error) {}
func pinWindowsReset(*testing.T, error)     {}
