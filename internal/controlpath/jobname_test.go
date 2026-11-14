package controlpath

import (
	"strings"
	"testing"
)

// TestJobNameSafe is the file-name rule (spec §4.1): [A-Za-z0-9_-][A-Za-z0-9_.-]*.
func TestJobNameSafe(t *testing.T) {
	for _, c := range []struct {
		name string
		ok   bool
	}{
		{"archive-orders", true},
		{"a", true},
		{"A_b.c-9", true},
		{"-leading-dash", true},
		{"_leading_underscore", true},
		{"trailing.", true},
		{strings.Repeat("a", 255), true},
		{"", false},
		{".hidden", false},
		{"..", false},
		{".", false},
		{"a/b", false},
		{`a\b`, false},
		{"../a", false},
		{"a b", false},
		{"a\tb", false},
		{"a\x00b", false},
		{"a\nb", false},
		{"café", false},
		{"a:b", false},
		{"a*b", false},
	} {
		if got := JobNameSafe(c.name); got != c.ok {
			t.Errorf("JobNameSafe(%q) = %v, want %v", c.name, got, c.ok)
		}
	}
}
