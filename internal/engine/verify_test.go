package engine

import "testing"

// TestRowSerializer pins the canonical row form both servers' rows are hashed in: pairs in
// column-name order, NUL-separated, one newline per row, NULL and bytes distinguishable.
func TestRowSerializer(t *testing.T) {
	s := newRowSerializer([]string{"z", "a", "m", "u", "n"})
	got := string(s.appendRow([]any{[]byte{0xff, 0x00}, int64(-7), nil, uint64(1 << 63), "2026-10-10 12:00:00"}))
	want := "a=-7\x00m=NULL\x00n=2026-10-10 12:00:00\x00u=9223372036854775808\x00z=0xff00\n"
	if got != want {
		t.Errorf("appendRow = %q, want %q", got, want)
	}
	if got := string(s.appendRow([]any{"x", int64(1), []byte("NULL"), uint64(0), nil})); got != "a=1\x00m=0x4e554c4c\x00n=NULL\x00u=0\x00z=x\n" {
		t.Errorf("second row = %q: the buffer must be reset and bytes must not read as NULL", got)
	}
}
