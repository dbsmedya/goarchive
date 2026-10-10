package tracking

import (
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
)

func TestJobInputValidate(t *testing.T) {
	good := JobInput{Name: "j", Type: jobspec.Purge, RootKey: orders(), Binding: base()}
	if err := good.validate(); err != nil {
		t.Fatal(err)
	}
	longest := good
	longest.Name = strings.Repeat("n", 255)
	if err := longest.validate(); err != nil {
		t.Fatalf("a 255-character name: %v", err)
	}
	for name, edit := range map[string]func(in *JobInput){
		"no name":                 func(in *JobInput) { in.Name = "" },
		"no type":                 func(in *JobInput) { in.Type = "" },
		"no root key":             func(in *JobInput) { in.RootKey = key.Def{Table: "orders"} },
		"no root table":           func(in *JobInput) { in.RootKey.Table = "" },
		"no source server":        func(in *JobInput) { in.Binding.SourceServerUUID = "" },
		"no source database":      func(in *JobInput) { in.Binding.SourceDatabase = "" },
		"no destination database": func(in *JobInput) { in.Binding.DestinationDatabase = "" },
		"a non-ASCII name":        func(in *JobInput) { in.Name = "sipariş" },
		"a name with a space":     func(in *JobInput) { in.Name = "a b" },
		"a control character":     func(in *JobInput) { in.Name = "a\x01" },
		"a 256-character name":    func(in *JobInput) { in.Name = strings.Repeat("n", 256) }, // job_name VARCHAR(255) (M4-F4)
	} {
		in := good
		in.RootKey.Columns = orders().Columns
		edit(&in)
		if err := in.validate(); err == nil {
			t.Errorf("%s: passed", name)
		}
	}
}

// TestStatementCaps: statements on the log carry at most key.MaxTuples tuples for the
// log's own columns, and an INSERT also stays under the 65,535-placeholder limit.
func TestStatementCaps(t *testing.T) {
	s := &Store{schema: "s", budget: 8 << 20}
	j := s.newJob(7, JobInput{Name: "j", Type: jobspec.Archive, RootKey: orders()})
	if want := key.MaxTuples(logDef("", orders()).Columns, 8<<20); j.maxTuples != want || want < 1 {
		t.Errorf("maxTuples = %d, want %d", j.maxTuples, want)
	}
	if j.insertMax != min(j.maxTuples, 65535/4) {
		t.Errorf("insertMax = %d", j.insertMax)
	}
	if j.logTable != "`s`.`archiver_job_log_7`" {
		t.Errorf("logTable = %s", j.logTable)
	}
	wide := key.Def{Table: "w", Columns: make([]key.Column, 40)}
	for i := range wide.Columns {
		wide.Columns[i] = key.Column{Name: "c", Type: "int", Family: key.FamilySigned}
	}
	// At a 1 GiB budget the range cap allows more tuples than an INSERT has placeholders for.
	big := &Store{schema: "s", budget: 1 << 30}
	if j := big.newJob(1, JobInput{RootKey: wide}); j.insertMax != 65535/41 {
		t.Errorf("a 40-column key: insertMax %d, want %d", j.insertMax, 65535/41)
	}
	// A membership statement also binds the status (Logged) or two (Transition), so a
	// one-column key keeps two placeholders free.
	one := key.Def{Table: "o", Columns: []key.Column{{Name: "id", Type: "bigint", Family: key.FamilySigned}}}
	if j := big.newJob(1, JobInput{RootKey: one}); j.maxTuples != 65535-2 {
		t.Errorf("a one-column key at 1 GiB: maxTuples %d, want %d", j.maxTuples, 65535-2)
	}
}
