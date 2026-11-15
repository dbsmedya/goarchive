package control

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
)

// TestParserIgnoresTail (spec §7, Codex r5): with both lines supplied in one buffer, the first
// is the request and the tail is ignored: never parsed, never refused.
func TestParserIgnoresTail(t *testing.T) {
	line, err := readRequest(bytes.NewReader([]byte("batch_delete_size=100\nbatch_size=1\n")))
	if err != nil || line != "batch_delete_size=100" {
		t.Errorf("readRequest = %q, %v, want the first line alone", line, err)
	}
	// The same through a reader that hands out one byte at a time: the boundary is the first LF,
	// not the first read.
	line, err = readRequest(iotest.OneByteReader(strings.NewReader("pause\nresume\n")))
	if err != nil || line != "pause" {
		t.Errorf("readRequest over single bytes = %q, %v, want the first line alone", line, err)
	}
}

// TestRequestLimitIgnoresTail (spec §7, Codex r5): the 4096-byte limit applies to the first
// request only. A valid first line with a tail that takes the total above 4096 bytes is accepted
// and processed once; the request itself is at most 4096 bytes with its LF, and 4096 bytes
// without an LF are not a request.
func TestRequestLimitIgnoresTail(t *testing.T) {
	tail := strings.Repeat("x", 5000)
	if line, err := readRequest(strings.NewReader("status\n" + tail)); err != nil || line != "status" {
		t.Errorf("a short line and a 5000-byte tail: %q, %v, want %q accepted", line, err, "status")
	}
	for _, tc := range []struct {
		name  string
		input string
		want  error
	}{
		{"4095 bytes and the LF", strings.Repeat("a", 4095) + "\n", nil},
		{"4096 bytes and the LF is over the limit", strings.Repeat("a", 4096) + "\n", errTooLong},
		{"4096 bytes without an LF", strings.Repeat("a", 4096), errTooLong},
		{"more than 4096 bytes without an LF", strings.Repeat("a", 9000), errTooLong},
		{"the end of input before an LF", "status", errNoLineFeed},
		{"nothing at all", "", errNoLineFeed},
	} {
		line, err := readRequest(strings.NewReader(tc.input))
		if !errors.Is(err, tc.want) {
			t.Errorf("%s: %v, want %v", tc.name, err, tc.want)
		}
		if tc.want == nil && len(line) != 4095 {
			t.Errorf("%s: the line is %d bytes, want 4095", tc.name, len(line))
		}
	}
}

// TestReadRequestKeepsTheTransportError: a read that fails for another reason than the end of
// input (a reset, a deadline) is not a malformed request: the caller closes without a reply.
func TestReadRequestKeepsTheTransportError(t *testing.T) {
	boom := errors.New("connection reset")
	_, err := readRequest(io.MultiReader(strings.NewReader("sta"), iotest.ErrReader(boom)))
	if !errors.Is(err, boom) || errors.Is(err, errNoLineFeed) || errors.Is(err, errTooLong) {
		t.Errorf("readRequest = %v, want the transport error and neither malformed one", err)
	}
}

// TestLexicalForm (spec §7 row 6): each of these is malformed, whatever names exist; the valid
// forms are told apart by kind.
func TestLexicalForm(t *testing.T) {
	for _, line := range []string{
		" status", "status ", "\tstatus", "status\t", "status\r", "", "Status", "STATUS", "Batch_Size=5",
		"k==1", "k=", "=5", "=", "=?", "k=??", "k=? ", "k =5", "k= 5", "k=5 ", "1k=5", "_k=5", "-k", "k=-5", "k=+5",
		"k=.5", "k=5.", "k=1e3", "k=.nan", "k=.inf", "k=NaN", "k=5.5.5", "k=0x10", "k=1_000", "k=٣",
		"batch_size=5\n", "a b", "a=1=2", "k=1.", "k=1..2", "k\x00=1", "sta\x00tus", "stätus",
	} {
		if _, err := parseRequest(line); err == nil {
			t.Errorf("parseRequest(%q) accepted, want malformed", line)
		}
	}

	for _, tc := range []struct {
		line   string
		want   request
		wantOK bool
	}{
		{"status", request{name: "status", kind: kindAction}, true},
		{"status-json", request{name: "status-json", kind: kindAction}, true},
		{"batch_size=10", request{name: "batch_size", kind: kindSet, number: "10"}, true},
		{"sleep_seconds=0.5", request{name: "sleep_seconds", kind: kindSet, number: "0.5"}, true},
		{"sleep_seconds=0.000000001", request{name: "sleep_seconds", kind: kindSet, number: "0.000000001"}, true},
		{"batch_size=007", request{name: "batch_size", kind: kindSet, number: "007"}, true},
		{"batch_size=?", request{name: "batch_size", kind: kindGet}, true},
		{"frobnicate", request{name: "frobnicate", kind: kindAction}, true},
		{"x1_y-z=1.0", request{name: "x1_y-z", kind: kindSet, number: "1.0"}, true},
	} {
		got, err := parseRequest(tc.line)
		if err != nil || got != tc.want {
			t.Errorf("parseRequest(%q) = %+v, %v, want %+v", tc.line, got, err, tc.want)
		}
	}
}

// TestFraming (spec §7 row 1): a reply is `OK` LF and a body, or exactly one line `ERR <reason>
// <message>` LF; every line of it ends with LF, and no message carries a line break.
func TestFraming(t *testing.T) {
	for _, tc := range []struct {
		name  string
		reply reply
		want  string
	}{
		{"ok, empty body", okReply(), "OK\n"},
		{"ok, a body", okReply("protocol 1", "help  list"), "OK\nprotocol 1\nhelp  list\n"},
		{"error", errReply(reasonMalformed, "bad"), "ERR malformed bad\n"},
		{"error, a message with line breaks", errReply(reasonInvalid, "a\nb\r\nc"), "ERR invalid a b  c\n"},
	} {
		if got := string(tc.reply.wire()); got != tc.want {
			t.Errorf("%s: wire = %q, want %q", tc.name, got, tc.want)
		}
	}
}
