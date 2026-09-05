package archiver

import (
	"fmt"
	"sort"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"

	"github.com/dbsmedya/goarchive/internal/graph"
)

// findingsToPreflightError converts the findings whose Check equals want into one
// *PreflightError carrying goarchive's own check id. Table names are copied from the
// findings and sorted at this presentation boundary because the caller may have built
// its request from graph map iteration.
//
// Findings whose Check is NOT want are ignored here on purpose: a stage that can
// receive more than one kind must partition them itself and call this once per kind,
// then reject anything left over with unexpectedFindingError. See phase 025's
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
	sort.Strings(tables)
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

// judgePrimaryKeyShape applies goarchive's PK-shape policy to library facts.
//
// It is a pure function over facts so the ordering contract — composite is the headline
// rejection and is reported ahead of PK-definition problems, as 1.8's
// ValidateSingleColumnPrimaryKey did — is unit-testable without a database.
//
// expected maps table -> configured primary_key. Tables absent from expected have no
// configured key; ValidatePrimaryKeyColumns rejects those earlier in the check order.
func judgePrimaryKeyShape(facts []validations.PKInfo, expected map[string]string) error {
	var compositeIssues []string
	for _, f := range validations.CheckPKSingleColumn(facts) {
		if f.Check != validations.IDPKSingleColumn {
			return unexpectedFindingError("pk_shape", f)
		}
		pk, ok := f.Facts.(validations.PKInfo)
		if !ok {
			return unexpectedFactsError("pk_shape", f, "validations.PKInfo")
		}
		compositeIssues = append(compositeIssues,
			fmt.Sprintf("%s(%d-column PRIMARY KEY)", pk.Table, len(pk.Columns)))
	}
	if len(compositeIssues) > 0 {
		return &PreflightError{
			Check:   "COMPOSITE_PK_CHECK",
			Message: "Composite primary keys are not supported. GoArchive identifies and deletes rows by a single primary-key column; a multi-column PK would over-match and risk deleting rows outside the archived set. See README 'Known Limits & Caution'",
			Tables:  compositeIssues,
		}
	}

	var pkDefIssues []string
	for _, f := range validations.CheckPKExists(facts) {
		if f.Check != validations.IDPKExists {
			return unexpectedFindingError("pk_shape", f)
		}
		pk, ok := f.Facts.(validations.PKInfo)
		if !ok {
			return unexpectedFactsError("pk_shape", f, "validations.PKInfo")
		}
		pkDefIssues = append(pkDefIssues, fmt.Sprintf("%s(no PRIMARY KEY)", pk.Table))
	}
	for _, f := range validations.CheckPKMatchesExpected(facts, expected) {
		if f.Check != validations.IDPKMatchesExpected {
			return unexpectedFindingError("pk_shape", f)
		}
		pk, ok := f.Facts.(validations.PKInfo)
		if !ok {
			return unexpectedFactsError("pk_shape", f, "validations.PKInfo")
		}
		// Unreachable with dbsgomysql v0.4.1 because CheckPKMatchesExpected already
		// skips PKNone; retained to prevent duplicate reporting if dependency
		// behaviour changes. 1.8's switch reported each table exactly once, and that
		// is the property being protected — not a state reachable today.
		if pk.Kind == validations.PKNone {
			continue
		}
		actual := ""
		if len(pk.Columns) > 0 {
			actual = pk.Columns[0]
		}
		pkDefIssues = append(pkDefIssues, fmt.Sprintf(
			"%s(configured primary_key %q is not the PRIMARY KEY column %q)",
			pk.Table, expected[pk.Table], actual))
	}
	if len(pkDefIssues) > 0 {
		return &PreflightError{
			Check:   "PRIMARY_KEY_CHECK",
			Message: "Every participating table must have a single-column PRIMARY KEY equal to the configured primary_key. GoArchive identifies and deletes rows by that column, so a missing or mismatched PRIMARY KEY can over-match and delete rows outside the archived set",
			Tables:  pkDefIssues,
		}
	}

	return nil
}

// privilegeOffenders converts privilege findings into offender entries. A schema-scoped
// finding (Table == "") formats as "PRIVILEGE(state)", e.g. "CREATE(absent)". A
// table-scoped finding (Table != "") formats as "TABLE(state)" instead, e.g.
// "orders(absent)" — the privilege name is deliberately dropped there because
// PreflightError.Tables is the established table-name surface for goarchive's other
// checks, and each downstream table-privilege check (e.g. DEST_WRITE_PERMISSION_CHECK)
// tests exactly one privilege, already named in its own Message. This asymmetry is
// intentional and must not change.
//
// Deviation D1 / invariant I2: a privilege check passes only on GrantPresent. Every
// other state — GrantAbsent, GrantUnconfirmed, GrantUnknown — fails closed, and the
// exact state is surfaced so an operator can tell "you do not have this" from "we
// cannot prove you have this". The library emits a finding for exactly the failing
// states, so this function does not re-apply the rule; it only formats.
func privilegeOffenders(stage string, findings []validations.Finding) ([]string, error) {
	var out []string
	for _, f := range findings {
		switch f.Check {
		case validations.IDSchemaPrivileges, validations.IDTablePrivileges:
		default:
			return nil, unexpectedFindingError(stage, f)
		}
		fact, ok := f.Facts.(validations.PrivilegeFact)
		if !ok {
			return nil, unexpectedFactsError(stage, f, "validations.PrivilegeFact")
		}
		subject := fact.Privilege.String()
		if fact.Table != "" {
			subject = fact.Table
		}
		out = append(out, subject+"("+fact.State.String()+")")
	}
	return out, nil
}

// unindexedFKColumns converts FK-index findings into goarchive's "<table>.<column>"
// entries, one per child column.
//
// 1.8 read information_schema.KEY_COLUMN_USAGE, which returns one row per constraint
// COLUMN, so a two-column foreign key produced two entries. validations.ForeignKey
// carries ChildColumns as a slice, so the fan-out is explicit here to preserve that.
func unindexedFKColumns(stage string, findings []validations.Finding) ([]string, error) {
	var out []string
	for _, f := range findings {
		if f.Check != validations.IDFKIndexed {
			return nil, unexpectedFindingError(stage, f)
		}
		fk, ok := f.Facts.(validations.ForeignKey)
		if !ok {
			return nil, unexpectedFactsError(stage, f, "validations.ForeignKey")
		}
		for _, col := range fk.ChildColumns {
			out = append(out, fk.ChildTable+"."+col)
		}
	}
	return out, nil
}

// asciiFoldEqual reports whether two identifiers differ only by ASCII letter case.
// Non-ASCII bytes must match exactly, which fails safe: a difference GoArchive cannot
// classify is treated as a genuine difference rather than as a mere casing slip.
func asciiFoldEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if 'A' <= ca && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if 'A' <= cb && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// asciiLower returns s with ASCII upper-case letters lower-cased and every other byte
// untouched. It is the identifier fold asciiFoldEqual compares under, materialised so a
// folded name can be STORED as a value — D3's uniqueness signature compares typed parts
// with slices.Equal, so the fold has to happen at construction, not at comparison.
// strings.ToLower is NOT a substitute: it folds non-ASCII letters that MySQL and the
// library's DiffSpecs treat as distinct, which would fail open (TestD3NonASCIICaseIsNotFolded).
func asciiLower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if 'A' <= c && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}

// judgePrimaryKeyColumns applies goarchive's configured-primary_key identity policy.
//
// It answers three questions in the order ValidatePrimaryKeyColumns used in 1.8:
//
//  1. is a primary_key configured at all?          -> PK_COLUMN_CHECK
//  2. does a column with that EXACT name exist?    -> pass
//  3. does one differ only by ASCII letter case?   -> PK_COLUMN_CASE_CHECK
//     otherwise                                    -> PK_COLUMN_CHECK
//
// Case issues are reported ahead of missing columns because a casing slip is the subtle
// failure and its guidance ("fix the casing") differs from a truly-absent column's.
//
// The case comparison deliberately scans EVERY column of the table, not only
// primary-key columns: information_schema.COLUMNS.COLUMN_NAME collates
// case-insensitively (utf8mb3_tolower_ci), so 1.8's lookup matched any column, and
// preserving that requires the same breadth. The library's CheckPKNameCase inspects PK
// columns only and is therefore NOT used here.
func judgePrimaryKeyColumns(
	columnsByTable map[string][]string,
	expected map[string]string,
	graphTables []string,
) error {
	var missing []string
	var caseIssues []string

	for _, table := range graphTables {
		pkColumn, configured := expected[table]
		if !configured {
			missing = append(missing, fmt.Sprintf(
				"%s(primary key must be explicitly configured; implicit default to 'id' is not allowed)", table))
			continue
		}

		cols := columnsByTable[table]
		exact := false
		var caseMatch string
		for _, col := range cols {
			if col == pkColumn {
				exact = true
				break
			}
			if caseMatch == "" && asciiFoldEqual(col, pkColumn) {
				caseMatch = col
			}
		}
		switch {
		case exact:
			// ok
		case caseMatch != "":
			caseIssues = append(caseIssues, fmt.Sprintf(
				"%s(configured primary_key %q but the column is named %q)", table, pkColumn, caseMatch))
		default:
			missing = append(missing, fmt.Sprintf("%s(%s)", table, pkColumn))
		}
	}

	if len(caseIssues) > 0 {
		return &PreflightError{
			Check:   "PK_COLUMN_CASE_CHECK",
			Message: "Configured primary_key does not match the database column name exactly. Column names are case-sensitive (e.g. log_id, LOG_ID and Log_Id are different) — set primary_key to the exact case used in the database schema",
			Tables:  caseIssues,
		}
	}
	if len(missing) > 0 {
		return &PreflightError{
			Check:   "PK_COLUMN_CHECK",
			Message: "Configured primary key columns not found in the source table",
			Tables:  missing,
		}
	}
	return nil
}

// partitionClosureFindings splits CheckFKClosure's output into its two flavours.
//
// The library emits both under Check == IDFKClosure and distinguishes them only by the
// typed Facts payload: a ForeignKey means "an external child points into the target
// set"; a MetadataVisibility means "discovery was not complete, so closure is
// unprovable". GoArchive maps the first to FK_COVERAGE_CHECK and the second to
// FK_COVERAGE_VISIBILITY_CHECK, which have DIFFERENT per-command applicability —
// copy-only is exempt from the second and not from the first — so they must be
// separated rather than reported as one error.
//
// An unrecognised check id or payload type aborts preflight (spec §2, fail closed).
func partitionClosureFindings(findings []validations.Finding) (
	[]validations.ForeignKey,
	[]validations.MetadataVisibility,
	error,
) {
	var external []validations.ForeignKey
	var visibility []validations.MetadataVisibility

	for _, f := range findings {
		if f.Check != validations.IDFKClosure {
			return nil, nil, unexpectedFindingError("fk_closure", f)
		}
		switch facts := f.Facts.(type) {
		case validations.ForeignKey:
			external = append(external, facts)
		case validations.MetadataVisibility:
			visibility = append(visibility, facts)
		default:
			return nil, nil, unexpectedFactsError(
				"fk_closure", f, "validations.ForeignKey or validations.MetadataVisibility")
		}
	}
	return external, visibility, nil
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

// dedupeCascadeEdges formats the ON DELETE CASCADE edges from one or more finding sets,
// reporting each constraint exactly once.
//
// The deduplication is required by the three-selector design (spec §3.5): a constraint
// whose child and parent are BOTH graph tables is returned by the IncomingTo fetch AND
// by the OutgoingFrom fetch, so the naive union double-reports every in-graph cascade.
// The key is (ChildSchema, ChildTable, ConstraintName) — the child-side triple, because
// a constraint belongs to its child table and that triple is unique per constraint in
// MySQL.
//
// Order is first-seen and therefore STABLE RELATIVE TO ITS INPUTS. It is deliberately
// not sorted here: the helper stays order-preserving and testable, and the caller sorts
// for operator-facing output (the fetches themselves are map-ordered — see
// WarnCascadeRules).
func dedupeCascadeEdges(stage string, findingSets ...[]validations.Finding) ([]string, error) {
	type key struct{ schema, table, constraint string }

	seen := make(map[key]struct{})
	var out []string

	for _, findings := range findingSets {
		for _, f := range findings {
			if f.Check != validations.IDCascadeRules {
				return nil, unexpectedFindingError(stage, f)
			}
			fk, ok := f.Facts.(validations.ForeignKey)
			if !ok {
				return nil, unexpectedFactsError(stage, f, "validations.ForeignKey")
			}
			k := key{fk.ChildSchema, fk.ChildTable, fk.ConstraintName}
			if _, dup := seen[k]; dup {
				continue
			}
			seen[k] = struct{}{}

			childCol, parentCol := "", ""
			if len(fk.ChildColumns) > 0 {
				childCol = fk.ChildColumns[0]
			}
			if len(fk.ParentColumns) > 0 {
				parentCol = fk.ParentColumns[0]
			}
			out = append(out, fmt.Sprintf("%s.%s.%s->%s.%s.%s",
				fk.ChildSchema, fk.ChildTable, childCol,
				fk.ParentSchema, fk.ParentTable, parentCol))
		}
	}
	return out, nil
}

// reconcileInternalFKs compares foreign keys whose child AND parent are both graph
// tables against the relations configuration, returning one line per discrepancy in
// fact order.
//
// The caller must supply facts from ForeignKeys(ctx, Within(graphTables...)) — the
// selector is the both-endpoints-in-graph filter, so no additional filtering happens
// here. Callers that need a stable message order must sort; fact order follows the
// request order, which is Graph.AllNodes() and therefore map-ranged.
func reconcileInternalFKs(fks []validations.ForeignKey, g *graph.Graph) []string {
	var messages []string
	for _, fk := range fks {
		// Self-referencing FKs (e.g. category.parent_id -> category.id) are not graph
		// edges and never were.
		if fk.ChildTable == fk.ParentTable {
			continue
		}

		// A graph edge carries exactly one foreign_key and one reference column, so a
		// multi-column constraint between two graph tables cannot be represented. 1.8
		// rejected these too, as a side effect of reading one information_schema row per
		// constraint column; 2.0 states the reason.
		if len(fk.ChildColumns) != 1 || len(fk.ParentColumns) != 1 {
			messages = append(messages, fmt.Sprintf(
				"  - %s -> %s (constraint: %s) [multi-column foreign key: %v -> %v; GoArchive relations model a single foreign_key/primary_key pair per edge]",
				fk.ChildTable, fk.ParentTable, fk.ConstraintName, fk.ChildColumns, fk.ParentColumns))
			continue
		}
		childColumn := fk.ChildColumns[0]
		parentColumn := fk.ParentColumns[0]

		edgeMeta := g.GetEdgeMeta(fk.ParentTable, fk.ChildTable)
		if edgeMeta == nil {
			messages = append(messages, fmt.Sprintf(
				"  - %s.%s -> %s.%s (constraint: %s) [no graph edge]",
				fk.ChildTable, childColumn, fk.ParentTable, parentColumn, fk.ConstraintName))
			continue
		}
		if edgeMeta.ForeignKey != childColumn {
			messages = append(messages, fmt.Sprintf(
				"  - %s.%s -> %s.%s (constraint: %s) [FK column mismatch: config has '%s', DB has '%s']",
				fk.ChildTable, childColumn, fk.ParentTable, parentColumn, fk.ConstraintName,
				edgeMeta.ForeignKey, childColumn))
			continue
		}
		if parentPK := g.GetPK(fk.ParentTable); parentPK != parentColumn {
			messages = append(messages, fmt.Sprintf(
				"  - %s.%s -> %s.%s (constraint: %s) [reference column mismatch: config PK is '%s', DB references '%s']",
				fk.ChildTable, childColumn, fk.ParentTable, parentColumn, fk.ConstraintName,
				parentPK, parentColumn))
		}
	}
	return messages
}
