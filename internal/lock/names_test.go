package lock

import (
	"strings"
	"testing"
)

// TestLockNames (T1): every name fits MySQL's 64-character limit for the longest inputs
// config allows, folds case, and keeps apart inputs the 2.x sanitizer merged.
func TestLockNames(t *testing.T) {
	long64, long255 := strings.Repeat("d", 64), strings.Repeat("j", 255)
	for _, n := range []string{
		TableLockName(long64, strings.Repeat("t", 64)),
		JobLockName(long64, long255),
	} {
		if len(n) > 64 {
			t.Errorf("%q is %d characters; MySQL allows 64", n, len(n))
		}
	}

	same := []struct{ a, b string }{
		{TableLockName("Shop", "Orders"), TableLockName("shop", "orders")},
		{JobLockName("GoArchive", "Archive_Old"), JobLockName("goarchive", "archive_old")},
	}
	for _, p := range same {
		if p.a != p.b {
			t.Errorf("case variants differ: %q, %q", p.a, p.b)
		}
	}

	differ := []struct{ a, b string }{
		{JobLockName("s", "a.b"), JobLockName("s", "a_b")},                   // G4: the sanitizer merged them
		{TableLockName("ab", "c"), TableLockName("a", "bc")},                 // the NUL separator
		{TableLockName("shop1", "orders"), TableLockName("shop2", "orders")}, // G3
		{TableLockName("s", "x"), JobLockName("s", "x")},                     // the prefix
	}
	for _, p := range differ {
		if p.a == p.b {
			t.Errorf("distinct identities share the name %q", p.a)
		}
	}

	// The documented SQL formula (probe L8) computes this same name.
	if got, want := JobLockName("goarchive", "archive_old_orders"), "goarchive:j:8dc6d9835d69244341908c2bd02168ed"; got != want {
		t.Errorf("JobLockName = %q, want %q", got, want)
	}
}
