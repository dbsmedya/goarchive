package preflight

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
)

// findingsError turns the findings whose Check is want into one *Error with goarchive's
// own check id, tables sorted. A stage that can receive several kinds partitions them
// itself and rejects the rest with unexpectedFinding.
func findingsError(id, message string, findings []validations.Finding, want string) *Error {
	var tables []string
	for _, f := range findings {
		if f.Check == want {
			tables = append(tables, f.Tables...)
		}
	}
	if len(tables) == 0 {
		return nil
	}
	sort.Strings(tables)
	return &Error{Check: id, Message: message, Tables: tables}
}

// unexpectedFinding fails closed on a finding the stage does not know: a dropped finding
// is a check that stopped running. It is a plain error, because the build is out of date,
// not the schema.
func unexpectedFinding(stage string, f validations.Finding) error {
	return fmt.Errorf(
		"PREFLIGHT_UNKNOWN_FINDING: stage %q received an unrecognised validation check %q "+
			"(tables: %v). This build of GoArchive does not know how to judge it, so preflight "+
			"fails closed. Upgrade GoArchive or pin the validation library version this build "+
			"was released against",
		stage, f.Check, f.Tables)
}

// unexpectedFacts fails closed on a known check whose payload has an unexpected type.
func unexpectedFacts(stage string, f validations.Finding, want string) error {
	return fmt.Errorf(
		"PREFLIGHT_UNEXPECTED_FACTS: stage %q received validation check %q with facts type %T; expected %s",
		stage, f.Check, f.Facts, want)
}

// inspectionError means preflight could not find out; an *Error means the schema is wrong.
func inspectionError(stage string, err error) error {
	return fmt.Errorf("preflight %s inspection failed: %w", stage, err)
}

// baseTableType is the TABLE_TYPE of a real table.
const baseTableType = "BASE TABLE"

// nonBaseTables returns objects that exist but are not base tables, as "name(TYPE)".
// Views and every other type are refused: copy and delete are defined for real tables only.
func nonBaseTables(found []validations.TableInfo) []string {
	var out []string
	for _, info := range found {
		if info.Type != baseTableType {
			out = append(out, info.Table+"("+info.Type+")")
		}
	}
	return out
}

// privilegeOffenders formats privilege findings: "PRIVILEGE(state)" at schema scope,
// "table(state)" at table scope. Only GrantPresent passes; the library emits a finding for
// every other state, and the state is shown so "absent" and "cannot prove" differ. A finding
// whose check is not one of accept fails closed.
func privilegeOffenders(stage string, findings []validations.Finding, accept ...string) ([]string, error) {
	var out []string
	for _, f := range findings {
		if !slices.Contains(accept, f.Check) {
			return nil, unexpectedFinding(stage, f)
		}
		fact, ok := f.Facts.(validations.PrivilegeFact)
		if !ok {
			return nil, unexpectedFacts(stage, f, "validations.PrivilegeFact")
		}
		subject := fact.Privilege.String()
		if fact.Table != "" {
			subject = fact.Table
		}
		out = append(out, subject+"("+fact.State.String()+")")
	}
	return out, nil
}

// partitionClosure splits CheckFKClosure's output: an external child pointing into the job
// (FK_COVERAGE_CHECK) and incomplete discovery (FK_COVERAGE_VISIBILITY_CHECK), which apply
// to different job types.
func partitionClosure(findings []validations.Finding) ([]validations.ForeignKey, []validations.MetadataVisibility, error) {
	var external []validations.ForeignKey
	var visibility []validations.MetadataVisibility
	for _, f := range findings {
		if f.Check != validations.IDFKClosure {
			return nil, nil, unexpectedFinding("fk_closure", f)
		}
		switch facts := f.Facts.(type) {
		case validations.ForeignKey:
			external = append(external, facts)
		case validations.MetadataVisibility:
			visibility = append(visibility, facts)
		default:
			return nil, nil, unexpectedFacts("fk_closure", f, "validations.ForeignKey or validations.MetadataVisibility")
		}
	}
	return external, visibility, nil
}

// triggerOffenders formats one "table(trigger)" per table. The library orders each
// table's triggers by firing order, then name, so the first is deterministic. An empty
// payload is unusable: a finding exists only because the table has triggers.
func triggerOffenders(stage string, findings []validations.Finding) ([]string, error) {
	var out []string
	for _, f := range findings {
		if f.Check != validations.IDTriggersPresent {
			return nil, unexpectedFinding(stage, f)
		}
		triggers, ok := f.Facts.([]validations.TriggerInfo)
		if !ok || len(triggers) == 0 {
			return nil, unexpectedFacts(stage, f, "non-empty []validations.TriggerInfo")
		}
		out = append(out, triggers[0].Table+"("+triggers[0].Name+")")
	}
	return out, nil
}

// cascadeEdges formats each ON DELETE CASCADE edge once. A constraint with both ends in
// the job is in the incoming and the outgoing selection, and two constraints may declare
// the same edge (R43); the formatted edge identifies it.
func cascadeEdges(stage string, findingSets ...[]validations.Finding) ([]string, error) {
	seen := map[string]bool{}
	var out []string
	for _, findings := range findingSets {
		for _, f := range findings {
			if f.Check != validations.IDCascadeRules {
				return nil, unexpectedFinding(stage, f)
			}
			fk, ok := f.Facts.(validations.ForeignKey)
			if !ok {
				return nil, unexpectedFacts(stage, f, "validations.ForeignKey")
			}
			if k := edgeKey(fk); !seen[k] {
				seen[k] = true
				out = append(out, fmt.Sprintf("%s.%s(%s)->%s.%s(%s)",
					fk.ChildSchema, fk.ChildTable, strings.Join(fk.ChildColumns, ","),
					fk.ParentSchema, fk.ParentTable, strings.Join(fk.ParentColumns, ",")))
			}
		}
	}
	sort.Strings(out)
	return out, nil
}

// edgeKey identifies a foreign key's edge: its two tables and its set of (child column,
// parent column) pairs, independent of the constraint's name and column order.
func edgeKey(fk validations.ForeignKey) string {
	pairs := make([]string, len(fk.ChildColumns))
	for i, c := range fk.ChildColumns {
		pairs[i] = c + "=" + fk.ParentColumns[i]
	}
	slices.Sort(pairs)
	return strings.Join([]string{fk.ChildSchema, fk.ChildTable, fk.ParentSchema, fk.ParentTable, strings.Join(pairs, ",")}, "\x00")
}

// asciiFoldEqual reports whether a and b differ only in ASCII letter case. Non-ASCII bytes
// must match exactly, so a difference GoArchive cannot classify counts as a real one.
func asciiFoldEqual(a, b string) bool {
	return len(a) == len(b) && asciiLower(a) == asciiLower(b)
}

// asciiLower lower-cases ASCII letters only. strings.ToLower would fold non-ASCII letters
// that MySQL and DiffSpecs treat as distinct.
func asciiLower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if 'A' <= c && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}

// columnList renders column names for a message: "(a, b)".
func columnList(cols []string) string {
	return "(" + strings.Join(cols, ", ") + ")"
}
