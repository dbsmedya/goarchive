package engine

import (
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/key"
)

func TestFormatKey(t *testing.T) {
	for _, tc := range []struct {
		k    key.Key
		want string
	}{
		{key.Key{int64(-7)}, "-7"},
		{key.Key{uint64(18446744073709551615)}, "18446744073709551615"},
		{key.Key{"1.50"}, `"1.50"`},
		{key.Key{[]byte("o'0")}, `"o'0"`},
		{key.Key{[]byte{0x00, 0xff}}, `"\x00\xff"`},
		{key.Key{[]byte("o0"), int64(1)}, `("o0", 1)`},
	} {
		if got := formatKey(tc.k); got != tc.want {
			t.Errorf("formatKey(%v) = %s, want %s", tc.k, got, tc.want)
		}
	}
}

func TestFormatKeysNamesTenAndCountsTheRest(t *testing.T) {
	var keys []key.Key
	for i := range 12 {
		keys = append(keys, key.Key{int64(i)})
	}
	if got, want := formatKeys(keys), "0, 1, 2, 3, 4, 5, 6, 7, 8, 9 and 2 more"; got != want {
		t.Errorf("formatKeys = %q, want %q", got, want)
	}
	if got := formatKeys(keys[:1]); got != "0" {
		t.Errorf("formatKeys(one) = %q", got)
	}
}

// TestUncopiedErrorNamesRowsAndRemedies: the refusal names every table's missing rows, says
// nothing of the page was deleted, and gives each kind of finding its own remedy (spec §3.3).
func TestUncopiedErrorNamesRowsAndRemedies(t *testing.T) {
	child := MissingRows{Table: "line_notes", Count: 1, Keys: "100"}
	root := MissingRows{Table: "orders", Root: true, Count: 2, Keys: `"o0", "o1"`}
	const (
		copyIt  = "Copy it into the destination and rerun"
		lastOne = "the source may hold its last copy"
		abandon = "Abandoning logged roots"
	)
	for _, tc := range []struct {
		name      string
		missing   []MissingRows
		want, not []string
	}{
		{"child", []MissingRows{child}, []string{"line_notes: 1 row(s) (100)", "Nothing of this page was deleted", copyIt, lastOne}, []string{abandon}},
		{"root", []MissingRows{root}, []string{`orders: 2 logged root(s) ("o0", "o1")`, "destination lost it", abandon}, []string{copyIt}},
		{"both", []MissingRows{root, child}, []string{"3 row(s)", copyIt, abandon, "Crash recovery"}, nil},
	} {
		msg := (&UncopiedError{Missing: tc.missing}).Error()
		for _, s := range tc.want {
			if !strings.Contains(msg, s) {
				t.Errorf("%s: message lacks %q:\n%s", tc.name, s, msg)
			}
		}
		for _, s := range tc.not {
			if strings.Contains(msg, s) {
				t.Errorf("%s: message has %q:\n%s", tc.name, s, msg)
			}
		}
	}
}

// TestKeySetErrorOnTheDestination: a destination recovery read names the destination's own
// causes, not the source's cold-data wording.
func TestKeySetErrorOnTheDestination(t *testing.T) {
	msg := (&KeySetError{Table: "orders", Statement: string(opHeld), Requested: 2, Extra: 2, Destination: true}).Error()
	for _, s := range []string{"recovery destination read on orders", "nothing was deleted", "PAD_CHAR_TO_FULL_LENGTH", "rewritten since its copy"} {
		if !strings.Contains(msg, s) {
			t.Errorf("message lacks %q:\n%s", s, msg)
		}
	}
	if strings.Contains(msg, "cold-data") {
		t.Errorf("destination message uses the source wording:\n%s", msg)
	}
}
