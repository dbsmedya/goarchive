package mermaidascii

// This file contains global variables that were originally CLI flags.
// They are now set during diagram parsing/rendering.

// graphDirection is the global direction for graph layout ("LR" or "TD")
// This is set during parsing based on the graph type declaration.
var graphDirection = "LR"

// Global flags (originally CLI flags, now set via Config)
var (
	Verbose          bool
	Coords           bool
	boxBorderPadding = 1
	paddingBetweenX  = 5
	paddingBetweenY  = 5
	useAscii         bool
)

// Default padding and graph direction constants
const (
	defaultBoxBorderPadding = 1
	defaultPaddingBetweenX  = 5
	defaultPaddingBetweenY  = 5
	defaultGraphDirection   = "LR"
)
