//go:build !linux && !windows

package controlpath

// maxLen is 103: sun_path is 104 bytes on macOS and the BSDs, and Go refuses a name of 104.
const maxLen = 103
