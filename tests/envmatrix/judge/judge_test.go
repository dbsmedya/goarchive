package main

import (
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// stream concatenates testdata fixtures: real `go test -json` output of small packages,
// captured once with the paths rewritten (pass, fail, nested, nestedok, panic, timeout,
// build, skip, wantonly).
func stream(t *testing.T, names ...string) io.Reader {
	t.Helper()
	var b strings.Builder
	for _, n := range names {
		data, err := os.ReadFile(filepath.Join("testdata", n+".json"))
		if err != nil {
			t.Fatal(err)
		}
		b.Write(data)
	}
	return strings.NewReader(b.String())
}

// TestJudge (M7c-2 P2): a row meets its expectation only when every failing leaf test cites
// an expected code ("refuse:") or nothing fails ("pass"), and nothing panicked, timed out or
// failed to build.
func TestJudge(t *testing.T) {
	cases := []struct {
		name, expect string
		fixtures     []string
		ok           bool
		reason       string // a substring of one reason when !ok
	}{
		{"clean pass", "pass", []string{"pass"}, true, ""},
		{"a skip is not a failure", "pass", []string{"skip"}, true, ""},
		{"a failure under pass", "pass", []string{"fail"}, false, "jfix/fail TestA"},
		{"one failure cites nothing", "refuse:MAX_ALLOWED_PACKET_CHECK", []string{"fail"}, false, "jfix/fail TestB cites no expected code"},
		{"every leaf cites a code", "refuse:A_CHECK|B_CHECK", []string{"nestedok"}, true, ""},
		{"a leaf cites another code", "refuse:A_CHECK", []string{"nestedok"}, false, "jfix/nestedok TestA/two cites no expected code"},
		{"a parent's output does not count for its leaf", "refuse:SOURCE_SELECT_PERMISSION_CHECK|JOB_SCHEMA_PERMISSION_CHECK", []string{"nested"}, false, "jfix/nested TestA/two cites no expected code"},
		{"a panic fails any row", "refuse:A_CHECK", []string{"panic"}, false, "panic"},
		{"a timeout fails the row", "pass", []string{"timeout"}, false, "timed out"},
		{"a build failure fails the row", "pass", []string{"build"}, false, "package jfix/build failed"},
		{"many packages", "refuse:A_CHECK|B_CHECK", []string{"pass", "skip", "nestedok"}, true, ""},
		// Fable M7c-2 review F1: a code the test only wants is no citation; the run said
		// something else. B_CHECK is cited by the error itself, before its "want".
		{"a wanted code is no citation", "refuse:A_CHECK|B_CHECK", []string{"wantonly"}, false, "jfix/wantonly TestA cites no expected code"},
		{"a cited code before its want", "refuse:B_CHECK", []string{"wantonly"}, false, "jfix/wantonly TestA cites no expected code"},
		{"no events", "pass", nil, false, "no test ran"},
		{"empty refuse", "refuse:", []string{"pass"}, false, "expectation"},
		{"unknown expectation", "maybe", []string{"pass"}, false, "expectation"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v := Judge(c.expect, stream(t, c.fixtures...))
			if v.OK != c.ok {
				t.Fatalf("OK = %v, want %v; reasons %q", v.OK, c.ok, v.Reasons)
			}
			if c.ok && len(v.Reasons) != 0 {
				t.Errorf("reasons %q on a row that is OK", v.Reasons)
			}
			if !c.ok && !strings.Contains(strings.Join(v.Reasons, "\n"), c.reason) {
				t.Errorf("reasons %q lack %q", v.Reasons, c.reason)
			}
		})
	}
}

// TestJudgeCounts: the counts are of test results at every level, as M7c-1's summaries
// counted them, and the failing list names leaves only.
func TestJudgeCounts(t *testing.T) {
	v := Judge("refuse:A_CHECK|B_CHECK", stream(t, "pass", "skip", "nestedok"))
	if v.Pass != 3 || v.Fail != 3 || v.Skip != 1 {
		t.Errorf("pass/fail/skip = %d/%d/%d, want 3/3/1", v.Pass, v.Fail, v.Skip)
	}
	if want := []string{"jfix/nestedok TestA/one", "jfix/nestedok TestA/two"}; !reflect.DeepEqual(v.Failing, want) {
		t.Errorf("failing = %q, want %q", v.Failing, want)
	}
}

// TestJudgeWantIsNoCitation (Fable M7c-2 review F1): in wantonly, TestB's error cites
// B_CHECK before its "want nil", and TestA only wants A_CHECK. Under refuse:B_CHECK
// exactly TestA is uncited.
func TestJudgeWantIsNoCitation(t *testing.T) {
	v := Judge("refuse:A_CHECK|B_CHECK", stream(t, "wantonly"))
	if len(v.Reasons) != 1 || !strings.Contains(v.Reasons[0], "TestA cites no expected code") {
		t.Errorf("reasons = %q, want exactly TestA uncited", v.Reasons)
	}
}
