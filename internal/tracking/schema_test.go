package tracking

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/lock"
)

// TestClassify is spec §6.1 and R37 as a table: only a 3.0 label, or a schema that is new,
// is accepted.
func TestClassify(t *testing.T) {
	for _, tc := range []struct {
		name  string
		st    schemaState
		found string // "" means accepted
	}{
		{"empty", schemaState{}, ""},
		{"labelled 3.0", schemaState{meta: true, labelled: true, label: "3.0", job: true, jobIs30: true}, ""},
		{"label table without its row", schemaState{meta: true}, ""},
		{"an empty unlabelled 3.0 archiver_job", schemaState{job: true, jobIs30: true}, ""},
		{"label 2.2", schemaState{meta: true, labelled: true, label: "2.2", job: true}, `report schema_version "2.2"`},
		{"label 3.0.1", schemaState{meta: true, labelled: true, label: "3.0.1", job: true}, `report schema_version "3.0.1"`},
		{"an empty label", schemaState{meta: true, labelled: true, label: "", job: true}, `report schema_version ""`},
		{"labelled 3.0 without archiver_job", schemaState{meta: true, labelled: true, label: "3.0"}, "but have no archiver_job table"},
		{"an unlabelled 2.x archiver_job", schemaState{job: true}, "hold a 2.x archiver_job table"},
		{"an unlabelled 2.x archiver_job with rows", schemaState{job: true, jobHasRow: true}, "hold a 2.x archiver_job table"},
		{"an unlabelled 3.0 archiver_job with rows", schemaState{job: true, jobIs30: true, jobHasRow: true}, "hold job rows but no schema_version label"},
		{"a 3.0 label over a 2.x archiver_job", schemaState{meta: true, labelled: true, label: "3.0", job: true},
			`hold a 2.x archiver_job table under schema_version "3.0"`},
	} {
		err := classify("goarchive", tc.st)
		var se *SchemaError
		switch {
		case tc.found == "" && err != nil:
			t.Errorf("%s: %v", tc.name, err)
		case tc.found != "" && (!errors.As(err, &se) || !strings.Contains(err.Error(), tc.found)):
			t.Errorf("%s: %v, want %q", tc.name, err, tc.found)
		}
	}
}

// TestSchemaErrorMessage: a 2.x schema gets R37's procedure; a 3.0 schema that lost
// archiver_job gets its own remedy.
func TestSchemaErrorMessage(t *testing.T) {
	msg := classify("goarchive", schemaState{meta: true, labelled: true, label: "2.2"}).Error()
	for _, want := range []string{`tracking tables in schema "goarchive" report schema_version "2.2"`,
		`requires tracking-schema label "3.0"`, "Finish every 2.x job with the 2.x binary, then drop the 2.x tracking tables",
		"docs/README_UPGRADING_3_0.md"} {
		if !strings.Contains(msg, want) {
			t.Errorf("2.x refusal lacks %q:\n%s", want, msg)
		}
	}
	msg = classify("goarchive", schemaState{meta: true, labelled: true, label: "3.0"}).Error()
	if strings.Contains(msg, "2.x binary") || !strings.Contains(msg, "Restore archiver_job") {
		t.Errorf("3.0 refusal:\n%s", msg)
	}
	// A forged 3.0 label over 2.x tables gets R37's procedure.
	msg = classify("goarchive", schemaState{meta: true, labelled: true, label: "3.0", job: true}).Error()
	if !strings.Contains(msg, "Finish every 2.x job with the 2.x binary") {
		t.Errorf("forged-label refusal:\n%s", msg)
	}
	// 3.0 tables that lost their label row must not be told to drop themselves as 2.x.
	msg = classify("goarchive", schemaState{job: true, jobIs30: true, jobHasRow: true}).Error()
	if strings.Contains(msg, "2.x binary") || !strings.Contains(msg, "INSERT INTO goarchive_meta (id, schema_version) VALUES (1, '3.0')") {
		t.Errorf("unlabelled 3.0 refusal:\n%s", msg)
	}
}

// TestSchemaLockedErrorMessage (TRK-1, Fable F5): the message states what the wait knows,
// with a form for a holder that released between the wait's end and the lookup.
func TestSchemaLockedErrorMessage(t *testing.T) {
	held := &SchemaLockedError{Schema: "goarchive", Wait: 60 * time.Second, Lock: &lock.HeldError{Name: "goarchive:s:x", Holder: 812}}
	for _, want := range []string{`schema lock goarchive:s:x on tracking schema "goarchive" for the whole 60s wait`,
		"connection 812 still holds it", "Nothing was done", "SHOW PROCESSLIST"} {
		if !strings.Contains(held.Error(), want) {
			t.Errorf("%q lacks %q", held.Error(), want)
		}
	}
	gone := &SchemaLockedError{Schema: "goarchive", Wait: 60 * time.Second, Lock: &lock.HeldError{Name: "goarchive:s:x"}}
	if msg := gone.Error(); !strings.Contains(msg, "released it a moment ago") || strings.Contains(msg, "connection 0") {
		t.Errorf("holder-0 message = %q", msg)
	}
}
