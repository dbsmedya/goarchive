package archiver

import (
	"fmt"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
)

// findingsToPreflightError converts the findings whose Check equals want into one
// *PreflightError carrying goarchive's own check id. Table names are taken in finding
// order, which the library documents as deterministic per check.
//
// Findings whose Check is NOT want are ignored here on purpose: a stage that can
// receive more than one kind must partition them itself and call this once per kind,
// then reject anything left over with unexpectedFindingError. See phase 026's
// FK_CLOSURE handling for the worked example.
func findingsToPreflightError(
	id, message string,
	findings []validations.Finding,
	want string,
) *PreflightError {
	var tables []string
	for _, f := range findings {
		if f.Check != want {
			continue
		}
		tables = append(tables, f.Tables...)
	}
	if len(tables) == 0 {
		return nil
	}
	return &PreflightError{Check: id, Message: message, Tables: tables}
}

// unexpectedFindingError aborts preflight when a stage receives a finding it does not
// recognise. Spec §2: unknown library vocabulary fails closed rather than being
// silently dropped, because a dropped finding is a check that stopped running.
//
// The result is deliberately a plain error, not a *PreflightError: it reports that the
// engine is out of date, not that the schema is wrong.
func unexpectedFindingError(stage string, f validations.Finding) error {
	return fmt.Errorf(
		"PREFLIGHT_UNKNOWN_FINDING: stage %q received an unrecognised validation check %q "+
			"(tables: %v). This build of GoArchive does not know how to judge it, so preflight "+
			"fails closed. Upgrade GoArchive or pin the validation library version this build "+
			"was released against",
		stage, f.Check, f.Tables)
}

// inspectionError wraps a metadata-inspection failure. It is deliberately a plain
// error: a *PreflightError means "the schema is wrong", while this means "we could not
// find out". Phase 010 characterizes that distinction.
func inspectionError(stage string, err error) error {
	return fmt.Errorf("preflight %s inspection failed: %w", stage, err)
}

// baseTableType is information_schema.TABLES.TABLE_TYPE for a real table.
const baseTableType = "BASE TABLE"

// nonBaseTableNames returns the objects that exist but are not BASE TABLEs, decorated
// with the type the server reported.
//
// GoArchive supports BASE TABLE objects. Any other type is rejected fail-closed —
// views, SYSTEM VIEW, and any type a future server version reports — because the
// copy/delete model is defined only for real tables and an unrecognised object type
// cannot be judged safe. The type is carried in the name as "name(TYPE)" so the
// operator error says what the object actually is; that decoration is the diagnostic
// value of this policy and is asserted end-to-end, not just here.
func nonBaseTableNames(found []validations.TableInfo) []string {
	var out []string
	for _, info := range found {
		if info.Type == baseTableType {
			continue
		}
		out = append(out, info.Table+"("+info.Type+")")
	}
	return out
}

// unexpectedFactsError aborts preflight when a RECOGNISED check arrives with a Facts
// payload of an unexpected type. It is deliberately distinct from
// unexpectedFindingError, which reports an unrecognised check ID: here goarchive knows
// the check, so naming it "unrecognised" would misdirect the reader. The fault is the
// payload, and the message says so — stage, check, actual type, expected type.
//
// Like unexpectedFindingError the result is a plain error, not a *PreflightError: it
// reports that the engine is out of date, not that the schema is wrong.
func unexpectedFactsError(
	stage string,
	f validations.Finding,
	want string,
) error {
	return fmt.Errorf(
		"PREFLIGHT_UNEXPECTED_FACTS: stage %q received validation check %q "+
			"with facts type %T; expected %s",
		stage, f.Check, f.Facts, want,
	)
}

// triggerOffenders converts trigger findings into goarchive's one-entry-per-table
// "<table>(<trigger>)" decoration. The library sorts each table's triggers by firing
// order (BEFORE before AFTER) and then by name, so element [0] is the deterministic
// "first" — 1.8 reported whichever row information_schema happened to return.
//
// An empty Facts slice is treated as unusable rather than as "no triggers": a finding
// exists only because the table HAS triggers, so an empty payload means this build
// cannot read the library's answer.
func triggerOffenders(stage string, findings []validations.Finding) ([]string, error) {
	var out []string
	for _, f := range findings {
		if f.Check != validations.IDTriggersPresent {
			return nil, unexpectedFindingError(stage, f)
		}
		triggers, ok := f.Facts.([]validations.TriggerInfo)
		if !ok || len(triggers) == 0 {
			return nil, unexpectedFactsError(stage, f, "non-empty []validations.TriggerInfo")
		}
		out = append(out, triggers[0].Table+"("+triggers[0].Name+")")
	}
	return out, nil
}
