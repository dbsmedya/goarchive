package tracking

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/lock"
)

// TestClassify is spec §6.1 and R37 as a table: only the 3.1 label, or a schema that is new,
// is accepted. A schema labelled 3.0 is refused with its own procedure (GR C9, G4).
func TestClassify(t *testing.T) {
	for _, tc := range []struct {
		name  string
		st    schemaState
		found string // "" means accepted
	}{
		{"empty", schemaState{}, ""},
		{"labelled 3.1", schemaState{meta: true, labelled: true, label: "3.1", job: true, jobIs30: true}, ""},
		{"labelled 3.0", schemaState{meta: true, labelled: true, label: "3.0", job: true, jobIs30: true}, `report schema_version "3.0"`},
		{"labelled 3.0 with no archiver_job", schemaState{meta: true, labelled: true, label: "3.0"}, `report schema_version "3.0"`},
		{"label table without its row", schemaState{meta: true}, ""},
		{"an empty unlabelled 3.0 archiver_job", schemaState{job: true, jobIs30: true}, ""},
		{"label 2.2", schemaState{meta: true, labelled: true, label: "2.2", job: true}, `report schema_version "2.2"`},
		{"label 3.0.1", schemaState{meta: true, labelled: true, label: "3.0.1", job: true}, `report schema_version "3.0.1"`},
		{"an empty label", schemaState{meta: true, labelled: true, label: "", job: true}, `report schema_version ""`},
		{"labelled 3.1 without archiver_job", schemaState{meta: true, labelled: true, label: "3.1"}, "are labelled 3.1 but have no archiver_job table"},
		{"an unlabelled 2.x archiver_job", schemaState{job: true}, "hold a 2.x archiver_job table"},
		{"an unlabelled 2.x archiver_job with rows", schemaState{job: true, jobHasRow: true}, "hold a 2.x archiver_job table"},
		{"an unlabelled 3.0 archiver_job with rows", schemaState{job: true, jobIs30: true, jobHasRow: true}, "hold job rows but no schema_version label"},
		{"a 3.0 label over a 2.x archiver_job", schemaState{meta: true, labelled: true, label: "3.0", job: true},
			`hold a 2.x archiver_job table under schema_version "3.0"`},
		{"a 3.1 label over a 2.x archiver_job", schemaState{meta: true, labelled: true, label: "3.1", job: true},
			`hold a 2.x archiver_job table under schema_version "3.1"`},
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

// TestSchemaErrorMessage: a 2.x schema gets R37's procedure; a 3.1 schema that lost
// archiver_job gets its own remedy.
func TestSchemaErrorMessage(t *testing.T) {
	msg := classify("goarchive", schemaState{meta: true, labelled: true, label: "2.2"}).Error()
	for _, want := range []string{`tracking tables in schema "goarchive" report schema_version "2.2"`,
		`requires tracking-schema label "3.1"`, "Finish every 2.x job with the 2.x binary, then drop the 2.x tracking tables",
		"docs/README_UPGRADING_3_0.md"} {
		if !strings.Contains(msg, want) {
			t.Errorf("2.x refusal lacks %q:\n%s", want, msg)
		}
	}
	msg = classify("goarchive", schemaState{meta: true, labelled: true, label: "3.1"}).Error()
	if strings.Contains(msg, "2.x binary") || !strings.Contains(msg, "Restore archiver_job") {
		t.Errorf("3.1 refusal:\n%s", msg)
	}
	// A forged label over 2.x tables gets R37's procedure, whichever 3.x label it is.
	for _, label := range []string{"3.0", "3.1"} {
		msg = classify("goarchive", schemaState{meta: true, labelled: true, label: label, job: true}).Error()
		if !strings.Contains(msg, "Finish every 2.x job with the 2.x binary") {
			t.Errorf("forged-label refusal for %s:\n%s", label, msg)
		}
	}
	// Tables that lost their label row must not be told to drop themselves as 2.x.
	msg = classify("goarchive", schemaState{job: true, jobIs30: true, jobHasRow: true}).Error()
	if strings.Contains(msg, "2.x binary") {
		t.Errorf("unlabelled refusal tells a 3.x schema to drop itself as 2.x:\n%s", msg)
	}
}

// TestSchemaErrorLabelRemedy (SLR-1): only the labels 2.x wrote get its drop procedure. Any
// other label may be a stale schema from an earlier release or a current one from a later
// release, so its remedy names both and never the 2.x drop.
func TestSchemaErrorLabelRemedy(t *testing.T) {
	for _, label := range []string{"2.0", "2.2"} {
		msg := classify("goarchive", schemaState{meta: true, labelled: true, label: label, job: true}).Error()
		if !strings.Contains(msg, "Finish every 2.x job with the 2.x binary") {
			t.Errorf("label %q lacks the 2.x procedure:\n%s", label, msg)
		}
	}
	unknown := []struct {
		name string
		st   schemaState
	}{
		{"3.2", schemaState{meta: true, labelled: true, label: "3.2", job: true, jobIs30: true}},
		{"3.0.1", schemaState{meta: true, labelled: true, label: "3.0.1"}},
		{"1.9", schemaState{meta: true, labelled: true, label: "1.9", job: true}},
		{"empty", schemaState{meta: true, labelled: true, label: "", job: true, jobIs30: true}},
		{"4.0", schemaState{meta: true, labelled: true, label: "4.0"}},
		// A later release may change archiver_job itself: its shape proves nothing about 2.x.
		{"3.2 over a 2.x-shaped archiver_job", schemaState{meta: true, labelled: true, label: "3.2", job: true}},
	}
	for _, tc := range unknown {
		msg := classify("goarchive", tc.st).Error()
		for _, want := range []string{"an earlier or a later GoArchive release", "finish its jobs with its binary",
			"run that release, and do not drop them", `"Tracking-schema upgrade procedures" in docs/README_JOBS_SCHEMA.md`} {
			if !strings.Contains(msg, want) {
				t.Errorf("%s: lacks %q:\n%s", tc.name, want, msg)
			}
		}
		for _, bad := range []string{"2.x binary", "drop the 2.x tracking tables"} {
			if strings.Contains(msg, bad) {
				t.Errorf("%s: carries the 2.x procedure %q:\n%s", tc.name, bad, msg)
			}
		}
	}
}

// TestTheLabelIs31 (GR C9, G4): new schemas are stamped 3.1, the label 3.0 tables carry is not the one this binary writes.
func TestTheLabelIs31(t *testing.T) {
	if SchemaLabel != "3.1" {
		t.Errorf("SchemaLabel = %q, want 3.1", SchemaLabel)
	}
}

// TestASchemaLabelled30IsRefusedWithItsOwnProcedure (spec §4.2, GR C9): tables that GoArchive 3.0 wrote are not opened as they
// are, because 3.1 binds a job on a member to the group and 3.0 bound it to a server; the refusal names the upgrade guide's
// procedure by its heading and carries neither the 2.x drop procedure nor the unknown-label advice, whichever shape the
// tables have, and the schema is left untouched ("do not drop them").
func TestASchemaLabelled30IsRefusedWithItsOwnProcedure(t *testing.T) {
	for _, st := range []schemaState{
		{meta: true, labelled: true, label: "3.0", job: true, jobIs30: true},
		{meta: true, labelled: true, label: "3.0"},
	} {
		err := classify("goarchive", st)
		var se *SchemaError
		if !errors.As(err, &se) {
			t.Fatalf("classify(%+v) = %v, want a SchemaError", st, err)
		}
		msg := err.Error()
		for _, want := range []string{`tracking tables in schema "goarchive" report schema_version "3.0"`, `requires tracking-schema label "3.1"`,
			`docs/README_UPGRADING_3_1.md, "Moving from 3.0"`, "written by GoArchive 3.0", "Stop every GoArchive process that uses the schema"} {
			if !strings.Contains(msg, want) {
				t.Errorf("the 3.0 refusal lacks %q:\n%s", want, msg)
			}
		}
		for _, bad := range []string{"2.x binary", "drop the 2.x tracking tables", "an earlier or a later GoArchive release"} {
			if strings.Contains(msg, bad) {
				t.Errorf("the 3.0 refusal carries %q:\n%s", bad, msg)
			}
		}
	}
}

// TestTheMissingLabelRemedyHasTwoCases (spec §4.2, rev 4.2): job rows with no label row may be 3.0's or 3.1's, so the
// remedy asks which GoArchive wrote them: '3.1' restores the label and runs; '3.0' restores it and is then refused with the
// procedure; or every tracking table is dropped.
func TestTheMissingLabelRemedyHasTwoCases(t *testing.T) {
	msg := classify("goarchive", schemaState{job: true, jobIs30: true, jobHasRow: true}).Error()
	for _, want := range []string{"hold job rows but no schema_version label",
		"INSERT INTO goarchive_meta (id, schema_version) VALUES (1, '3.1')", "INSERT INTO goarchive_meta (id, schema_version) VALUES (1, '3.0')",
		`docs/README_UPGRADING_3_1.md, "Moving from 3.0"`, "drop every tracking table to start afresh"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the missing-label remedy lacks %q:\n%s", want, msg)
		}
	}
	if i31, i30 := strings.Index(msg, "VALUES (1, '3.1')"), strings.Index(msg, "VALUES (1, '3.0')"); i31 < 0 || i30 < i31 {
		t.Errorf("the remedy gives '3.0' before '3.1': it must say which release wrote the tables first:\n%s", msg)
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
