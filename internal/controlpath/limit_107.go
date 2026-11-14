//go:build linux || windows

package controlpath

// maxLen is 107: sun_path is 108 bytes on Linux and Windows, and Go refuses a name of 108.
const maxLen = 107
