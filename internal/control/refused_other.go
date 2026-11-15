//go:build !windows

package control

func isPlatformRefused(error) bool { return false }
