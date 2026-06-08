package archiver

import "testing"

func TestPlaceholderCheck(t *testing.T) {
	// 60000 cols * 2 batch = 120000 > 65535 -> fail
	if err := checkPlaceholderLimit("wide", 60000, 2); err == nil {
		t.Fatalf("expected placeholder limit error for wide table")
	}
	// 5 cols * 1000 batch = 5000 < 65535 -> ok
	if err := checkPlaceholderLimit("narrow", 5, 1000); err != nil {
		t.Fatalf("unexpected error for narrow table: %v", err)
	}
	// Exact boundary: 65535 placeholders must fail (>=), 65534 must pass.
	if err := checkPlaceholderLimit("exact", 65535, 1); err == nil {
		t.Fatal("expected limit error at exact boundary (65535)")
	}
	if err := checkPlaceholderLimit("below", 65534, 1); err != nil {
		t.Fatalf("unexpected error one below boundary (65534): %v", err)
	}
}
