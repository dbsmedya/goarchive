package archiver

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
)

// schemaVerdict is one table's compatibility evaluation.
type schemaVerdict struct {
	Table    string
	Fatal    string   // "" when compatible; otherwise the decorated PreflightError.Tables entry
	Warnings []string // advisory lines, already suppression-filtered
}

// intDisplayWidthRe matches the deprecated integer display width — the parenthesized
// digit count following an integer type keyword. Anchored at the start so it never
// touches the genuinely-semantic precision of varchar(255) or decimal(10,2). unsigned
// and zerofill survive because they change the value range.
var intDisplayWidthRe = regexp.MustCompile(`^(tinyint|smallint|mediumint|int|integer|bigint)\(\d+\)`)

// goarchiveTypesCompatible is a deliberate GoArchive POLICY OVERRIDE. It is NOT a
// duplicate of the library's type normalization, and must not be "simplified away" by a
// later reader who notices the overlap.
//
// The library already normalizes every integer display width before comparing
// (ColumnSpec.NormalizedType; COMPAT.md entry 1), so for bigint(20) vs bigint, int(11) vs
// int, and int(10) unsigned vs int unsigned it emits NO diff at all and this function is
// never reached. The library deliberately PRESERVES tinyint(1): BOOLEAN is an alias for
// TINYINT(1), and erasing the width would report a boolean and a plain TINYINT as
// identical. That is the right call for a general schema-difference engine.
//
// GoArchive is not a schema-difference engine — it decides whether rows can be COPIED.
// MySQL does not enforce boolean values in a TINYINT(1), and the display width changes
// neither storage, range, nor whether any source value fits the destination column. So
// GoArchive accepts the difference, exactly as 1.8 did, and the published design REQUIRES
// it: spec §3.3 (`2026-07-27-dbsgomysql-integration-design.md:244`) specifies "judged from
// the raw values the diff carries". No ledger deviation — D1-D6 is closed, and this
// preserves current acceptance rather than changing it.
//
// In practice this function therefore exists for exactly ONE live case: tinyint(1) vs
// tinyint. Nothing else can reach it.
func goarchiveTypesCompatible(a, b string) bool {
	strip := func(t string) string {
		return intDisplayWidthRe.ReplaceAllString(strings.TrimSpace(t), "$1")
	}
	return strip(a) == strip(b)
}

// evaluateSchemaCompatibility applies goarchive's directional compatibility policy to one
// table's specs. The destination may be LOOSER than the source — dropped secondary
// indexes, auto_increment, defaults, ON UPDATE, relaxed NOT NULL — but never STRICTER.
//
// charsetStrict selects the charset disposition: fatal under count verification or
// --skip-verify, advisory under a sha256 verification that will actually run.
//
// Only the FIRST fatal column per table is reported, matching 1.8's `break`
// (preflight.go:1189) and the shape phase 006's characterization pins.
func evaluateSchemaCompatibility(pair specPair, charsetStrict bool) (schemaVerdict, error) {
	verdict := schemaVerdict{Table: pair.Table}
	warnedCharset := make(map[string]bool)

	for _, diff := range validations.DiffSpecs(pair.A, pair.B) {
		fatal, err := disposeDiff(diff, charsetStrict, warnedCharset)
		if err != nil {
			return schemaVerdict{}, err
		}
		switch {
		case fatal != nil && fatal.fatalReason != "":
			if verdict.Fatal == "" {
				verdict.Fatal = fmt.Sprintf("%s(column %s: %s; source=%s, destination=%s)",
					pair.Table, diff.Column, fatal.fatalReason, diff.A, diff.B)
			}
		case fatal != nil && fatal.warning != "":
			verdict.Warnings = append(verdict.Warnings, fatal.warning)
		}
	}
	return verdict, nil
}

// diffDisposition is what disposeDiff decided about one difference.
type diffDisposition struct {
	fatalReason string // non-empty means fatal
	warning     string // non-empty means advisory
}

// disposeDiff maps one SpecDiff to goarchive policy. Every SpecDiffKind the library
// publishes today has an explicit case; anything else aborts preflight (spec §2, fail
// closed).
//
// warnedCharset records which columns already produced a charset advisory, so the
// collation advisory can be suppressed for them: at most one advisory per column,
// charset first (1.8's `else if`, preflight.go:1196). Pass nil when suppression state is
// not needed.
func disposeDiff(
	diff validations.SpecDiff,
	charsetStrict bool,
	warnedCharset map[string]bool,
) (*diffDisposition, error) {
	switch diff.Kind {

	// ---- fatal, unconditionally -------------------------------------------------
	case validations.ColumnAbsent:
		return &diffDisposition{fatalReason: "column missing on one side"}, nil
	case validations.ColumnOrderMismatch:
		return &diffDisposition{fatalReason: "column order mismatch"}, nil

	// ---- fatal, with the deliberate tinyint(1) policy override --------------------
	case validations.ColumnTypeMismatch:
		if goarchiveTypesCompatible(diff.A, diff.B) {
			return nil, nil // see goarchiveTypesCompatible — policy, not normalization
		}
		return &diffDisposition{fatalReason: "column type mismatch"}, nil

	// ---- directional -------------------------------------------------------------
	case validations.ColumnNullabilityMismatch:
		// A is the source, B the destination. Fatal only when the DESTINATION is
		// stricter: the copy inserts explicit values, so a looser destination is safe.
		if strings.EqualFold(diff.A, "true") && strings.EqualFold(diff.B, "false") {
			return &diffDisposition{fatalReason: "destination is NOT NULL but source allows NULL"}, nil
		}
		return nil, nil

	// ---- verification-mode dependent ---------------------------------------------
	case validations.ColumnCharsetMismatch:
		if charsetStrict {
			return &diffDisposition{fatalReason: fmt.Sprintf(
				"character set mismatch (source=%s, destination=%s): copying can silently transliterate or truncate text and count verification cannot detect it; align charsets or use sha256 verification",
				diff.A, diff.B)}, nil
		}
		if warnedCharset != nil {
			warnedCharset[diff.Column] = true
		}
		return &diffDisposition{warning: fmt.Sprintf(
			"column %s: charset differs (source=%s destination=%s); sha256 verification will fail before delete if data is altered",
			diff.Column, diff.A, diff.B)}, nil

	case validations.ColumnCollationMismatch:
		// Suppressed for a column that already warned on charset: at most one advisory
		// per column, charset first. (Phase 030 adds D3's fatal path for collation
		// differences on a destination unique index; this is the advisory-only case.)
		if warnedCharset != nil && warnedCharset[diff.Column] {
			return nil, nil
		}
		return &diffDisposition{warning: fmt.Sprintf(
			"column %s: collation differs (source=%s destination=%s); stored bytes are identical but comparisons/sorting may differ in the archive",
			diff.Column, diff.A, diff.B)}, nil

	// ---- allowed: destination may be looser ---------------------------------------
	case validations.ColumnDefaultMismatch,
		validations.ColumnAutoIncrementMismatch,
		validations.ColumnOnUpdateMismatch:
		return nil, nil

	// ---- subsumed by the destination-generated invariant (phase 029) --------------
	case validations.ColumnGeneratedMismatch, validations.ColumnGenerationExprMismatch:
		return nil, nil

	// ---- recognized and deliberately ignored --------------------------------------
	case validations.ColumnVisibilityMismatch:
		// Invisible SOURCE columns are already fatal via INVISIBLE_COLUMN_CHECK, which
		// runs earlier. Destination-side invisibility is not checked today and stays
		// unchecked.
		return nil, nil

	case validations.IndexAbsent,
		validations.IndexPartsMismatch,
		validations.IndexUniquenessMismatch,
		validations.IndexTypeMismatch,
		validations.IndexVisibilityMismatch:
		// Index policy is computed from uniqueness signatures over the captured index
		// sections (phases 029-030), never from name-keyed index diffs — so renaming an
		// equivalent index cannot false-fail.
		return nil, nil

	case validations.EngineMismatch, validations.CharsetMismatch, validations.CollationMismatch:
		// Table-level. The compatibility check compares columns; the source engine is
		// separately enforced by STORAGE_ENGINE_CHECK.
		return nil, nil

	// ---- inspection-integrity failure: aborts here, already ----------------------
	// Phase 029 adds a SECOND, earlier guard — checkAbsoluteInvariants inspects
	// TableSpec.Captured directly, before any diff is produced — so an uncaptured index
	// section is rejected without relying on the library to emit this diff for it. This
	// arm stays as the backstop for a spec pair that reaches DiffSpecs anyway.
	case validations.IndexUnconfirmed:
		// The library emits this as SpecDiff{Kind, Side} with NO Column and NO Index
		// (spec_diff.go:339-347). Report the SIDE — diff.Column would always render "".
		return nil, fmt.Errorf(
			"PREFLIGHT_INSPECTION_INTEGRITY: DiffSpecs reported IndexUnconfirmed on side %v "+
				"even though both sides captured indexes; the schema inspection cannot be trusted",
			diff.Side)

	// ---- cannot appear: sections not captured -> fail closed if observed ----------
	case validations.CommentMismatch,
		validations.CommentUnconfirmed,
		validations.ConstraintUnconfirmed,
		validations.ConstraintAbsent,
		validations.ConstraintKindMismatch,
		validations.CheckClauseMismatch,
		validations.CheckEnforcementMismatch,
		validations.ForeignKeyColumnsMismatch,
		validations.ForeignKeyReferenceMismatch,
		validations.ForeignKeyRuleMismatch:
		return nil, fmt.Errorf(
			"PREFLIGHT_UNEXPECTED_DIFF: diff kind %v cannot occur — GoArchive captures neither "+
				"constraints nor comments — but was observed for column %q / index %q. Preflight "+
				"fails closed",
			diff.Kind, diff.Column, diff.Index)

	default:
		return nil, fmt.Errorf(
			"PREFLIGHT_UNKNOWN_DIFF: unrecognised schema difference kind %v (column %q, index %q). "+
				"This build of GoArchive does not know how to judge it, so preflight fails closed. "+
				"Upgrade GoArchive or pin the validation library version this build was released against",
			diff.Kind, diff.Column, diff.Index)
	}
}
