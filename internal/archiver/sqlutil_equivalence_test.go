package archiver

import (
	"testing"

	libsqlutil "github.com/dbsmedya/dbsgomysql/pkg/sqlutil"
	oldsqlutil "github.com/dbsmedya/goarchive/internal/sqlutil"
)

// TestSqlutilQuoteIdentifierEquivalence proves the library's QuoteIdentifier produces
// byte-identical output to goarchive's for every input goarchive can encounter.
func TestSqlutilQuoteIdentifierEquivalence(t *testing.T) {
	inputs := []string{
		"", "id", "my_table", "MyTable", "t1",
		"has`backtick", "has``two", "`leading", "trailing`",
		"with space", "with-dash", "with.dot", "with$dollar",
		"üñïçødé", "\x00nul", "\xff\xfe invalid utf8",
		"archiver_job", "archiver_job_log_42",
	}
	for _, in := range inputs {
		if got, want := libsqlutil.QuoteIdentifier(in), oldsqlutil.QuoteIdentifier(in); got != want {
			t.Errorf("QuoteIdentifier(%q): library=%q goarchive=%q", in, got, want)
		}
	}
}

// TestSqlutilIsSimpleIdentifierEquivalence proves IsSimpleIdentifier agrees with
// IsValidIdentifier on every class of input. Spec §2 asserts they are
// semantics-identical; this is the proof, not the assumption.
func TestSqlutilIsSimpleIdentifierEquivalence(t *testing.T) {
	inputs := []string{
		"", "id", "my_table", "MyTable", "T1", "_leading_underscore", "trailing_",
		"0starts_with_digit", "123", "___",
		"has space", "has-dash", "has.dot", "has$dollar", "has`backtick",
		"has\nnewline", "trailing\n", "\nleading",
		"üñïçødé", "\x00nul", "\xff\xfe",
		"a", "aa", "aA0_",
	}
	for _, in := range inputs {
		if got, want := libsqlutil.IsSimpleIdentifier(in), oldsqlutil.IsValidIdentifier(in); got != want {
			t.Errorf("IsSimpleIdentifier(%q)=%v but IsValidIdentifier(%q)=%v", in, got, in, want)
		}
	}
}
