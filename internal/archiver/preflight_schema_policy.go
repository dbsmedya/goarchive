package archiver

import (
	"fmt"
	"regexp"
	"slices"
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
// The library normalizes every ORDINARY integer display width before comparing
// (ColumnSpec.NormalizedType; COMPAT.md entry 1) — bigint(20) vs bigint, int(11) vs int,
// int(10) unsigned vs int unsigned emit NO diff and never reach this function — with two
// deliberate exceptions it PRESERVES: tinyint(1), because BOOLEAN is an alias for
// TINYINT(1) and erasing the width would report a boolean and a plain TINYINT as
// identical; and any ZEROFILL column's width, because it changes the server's text
// rendering. That is the right call for a general schema-difference engine.
//
// GoArchive is not a schema-difference engine — it decides whether rows can be COPIED.
// MySQL does not enforce boolean values in a TINYINT(1), and the display width changes
// neither storage, range, nor whether any source value fits the destination column. So
// GoArchive accepts the difference, exactly as 1.8 did, and the published design REQUIRES
// it: spec §3.3 (`2026-07-27-dbsgomysql-integration-design.md:244`) specifies "judged from
// the raw values the diff carries". No ledger deviation — D1-D6 is closed, and this
// preserves current acceptance rather than changing it.
//
// The rule is general: EVERY ColumnTypeMismatch arrives here (disposeDiff calls this for
// the whole kind), a leading integer display width is stripped from both raw COLUMN_TYPE
// strings, and the pair is accepted iff the remainders are byte-equal. Given the
// library's normalisation, the acceptance this ADDS is exactly tinyint(1) vs tinyint and
// a ZEROFILL column's width — both pinned in
// TestSchemaCompatTypesCompatibleAcceptanceBoundary. Everything else that differs —
// decimal(10,2) vs decimal(12,2), varchar vs int, a one-sided unsigned or zerofill —
// stays fatal. History: on libraries before 1.1.3, year(4) vs year arrived here and was
// rejected; since 1.1.3 the library normalises it upstream and it never arrives. This
// regex deliberately does not claim year, so a library regression fails closed.
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

	// Absolute invariants first: they are diff-independent, cheaper than DiffSpecs, and
	// their failures are the most actionable. Running them first is also what makes the
	// Captured guard meaningful — after the diff loop it would be shadowed by
	// disposeDiff's IndexUnconfirmed arm for the one-sided case.
	reason, err := checkAbsoluteInvariants(pair)
	if err != nil {
		return schemaVerdict{}, err
	}
	if reason != "" {
		verdict.Fatal = reason
		return verdict, nil
	}

	diffs := validations.DiffSpecs(pair.A, pair.B)

	// D3 before the column dispositions: a destination unique index the source lacks is
	// a data-loss hazard, and its message is more actionable than a charset advisory on
	// the same column.
	if reason := checkDestinationUniqueness(pair, diffs); reason != "" {
		verdict.Fatal = reason
		return verdict, nil
	}

	warnedCharset := make(map[string]bool)
	for _, diff := range diffs {
		fatal, err := disposeDiff(diff, charsetStrict, warnedCharset)
		if err != nil {
			return schemaVerdict{}, err
		}
		// Per-table half of spec §3.3's declared evaluation order: only the FIRST fatal
		// diff is recorded (verdict.Fatal == "" guards re-assignment), but the loop does
		// NOT stop there — it keeps appending warnings from every later diff. That is what
		// makes a fatal table's Warnings slice non-empty, and is why the CALLER
		// (ValidateDestinationSchemaCompatibility, preflight.go) must itself discard a
		// fatal table's warnings rather than relying on this function to withhold them.
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

	// ---- advisory: the two catalogues disagree on spelling only -----------------
	case validations.ColumnNameCaseMismatch:
		// A and B are the two spellings (A source, B destination); Column is the
		// source's. MySQL resolves column names case-insensitively (dbsgomysql COMPAT
		// entry 28), the INSERT column list is built from the SOURCE only
		// (sourceColumnLists), and the verifier hashes both sides with that same list —
		// so the copy and the verification are unaffected. The operator still hears it:
		// the archive is no longer a byte-faithful copy of the source's schema, and a
		// case-sensitive consumer of the archive will notice. Deliberately NOT gated on
		// warnedCharset — that map serves the charset → collation pair only.
		return &diffDisposition{warning: fmt.Sprintf(
			"column %s: name differs only in letter case (source=%s destination=%s); MySQL resolves column names case-insensitively, so the copy and verification are unaffected",
			diff.Column, diff.A, diff.B)}, nil

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
		// Visibility is irrelevant to the copy and to verification: every row
		// read and every INSERT names its columns explicitly (issue #23), so an
		// INVISIBLE column on either side is still copied and hashed.
		return nil, nil

	case validations.IndexAbsent,
		validations.IndexPartsMismatch,
		validations.IndexUniquenessMismatch,
		validations.IndexTypeMismatch,
		validations.IndexVisibilityMismatch,
		validations.IndexNameCaseMismatch:
		// Index policy is computed from uniqueness signatures over the captured index
		// sections (phases 029-030), never from name-keyed index diffs — so renaming an
		// equivalent index cannot false-fail, and neither can re-casing its name (#77).
		return nil, nil

	case validations.EngineMismatch, validations.CharsetMismatch, validations.CollationMismatch:
		// Table-level. The compatibility check compares columns; the source engine is
		// separately enforced by STORAGE_ENGINE_CHECK.
		return nil, nil

	// ---- inspection-integrity failure: defensive backstop --------------------------
	// checkAbsoluteInvariants (phase 029) inspects TableSpec.Captured directly, BEFORE
	// any diff is produced, and rejects a one-sided capture there. This arm is therefore
	// UNREACHABLE through evaluateSchemaCompatibility. It is retained deliberately: it
	// stays correct for a direct or future caller of disposeDiff, and it fails closed if
	// the invariant is ever reordered or removed. TestDisposeDiffIndexUnconfirmedFailsClosed
	// covers it directly, since no stage-level path can.
	case validations.IndexUnconfirmed:
		// The library emits this as SpecDiff{Kind, Side} with NO Column and NO Index
		// (spec_diff.go:339-347). Report the SIDE — diff.Column would always render "".
		return nil, fmt.Errorf(
			"PREFLIGHT_INSPECTION_INTEGRITY: DiffSpecs reported IndexUnconfirmed on side %v "+
				"even though both sides captured indexes; the schema inspection cannot be trusted",
			diff.Side)

	// ---- cannot appear: sections not captured -> fail closed if observed ----------
	// ConstraintKindUnconfirmed (v1.2.0) is emitted only after BOTH sides captured the
	// constraints section, which GoArchive never requests (preflight_facts.go, WithIndexes
	// only). It carries the constraint name in SpecDiff.Index, rendered below.
	case validations.CommentMismatch,
		validations.CommentUnconfirmed,
		validations.ConstraintUnconfirmed,
		validations.ConstraintKindUnconfirmed,
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

// primaryIndex returns the PRIMARY index of a spec. In MySQL a primary key is always
// named PRIMARY (see the library's docs/COMPAT.md entry 13), so the name is the lookup.
func primaryIndex(spec validations.TableSpec) (validations.IndexSpec, bool) {
	for _, idx := range spec.Indexes {
		if idx.Name == "PRIMARY" {
			return idx, true
		}
	}
	return validations.IndexSpec{}, false
}

// primaryPartSignature renders one PRIMARY key part for comparison.
//
// Only (column identity, prefix) participate. Descending is deliberately ignored:
// direction does not affect INSERT IGNORE idempotency, and the 1.8 COLUMN_KEY-based
// policy never observed it, so comparing full IndexPart values would introduce an
// unintended new failure (spec §3.3, fourth review precision 6).
//
// The column identity is folded to ASCII lower case (asciiLower): MySQL resolves column
// names case-insensitively, and error 1060 forbids two columns differing only by case in
// one table, so the fold is injective per table — the namespace tag below still separates
// a column from an expression, and no collision the server does not already have can be
// introduced (#77, operator review R1). Expression text is never folded. The operator
// message therefore shows key columns in lower case; the surrounding preflight error
// names the table, and the advisory from disposeDiff names both spellings.
//
// The expression branch is unreachable today — MySQL does not permit functional PRIMARY
// key parts — and is kept for TOTALITY, not for reuse. Phase 030's uniquenessSignature is
// a separate function with a different encoding. Collapsing expression parts onto the
// empty identity would make two DIFFERENT expressions produce the SAME signature and
// compare equal: a silent false all-clear in exactly the case the code cannot otherwise
// check.
//
// BOTH kinds are namespace-tagged, and tagging only one is not enough: with a bare column
// identity, a column literally named `expr:email` and the expression `email` would both
// render "(expr:email,0)". MySQL permits `:` in a backtick-quoted identifier, so that
// column name is legal. Tag every branch, or the tag buys nothing.
func primaryPartSignature(part validations.IndexPart) string {
	identity := "col:" + asciiLower(part.Column)
	if part.Column == "" {
		identity = "expr:" + part.Expression
	}
	return fmt.Sprintf("(%s,%d)", identity, part.SubPart)
}

// checkAbsoluteInvariants evaluates the diff-independent rules of spec §3.3.
//
// It returns a fatal reason ("" when clean), or an ERROR when the inspection itself
// cannot be trusted — the two are different outcomes: a fatal reason says the schema is
// wrong, an error says GoArchive could not determine whether it is.
func checkAbsoluteInvariants(pair specPair) (string, error) {
	// Inspection integrity. tableSpecs captures both sides WithIndexes(), so a spec
	// reporting otherwise did not come from the capture this evaluator requires.
	//
	// This is STRICTLY STRONGER than the library's IndexUnconfirmed diff, which
	// diffIndexes emits only when EXACTLY ONE side captured (spec_diff.go:339-347).
	// When NEITHER side captured, diffIndexes returns nil and the pair would be silently
	// judged compatible. Checking Captured directly closes that.
	//
	// The sides are an ordered slice, not a map: a map range would report a random side
	// when both are uncaptured, making the message non-deterministic.
	for _, side := range []struct {
		name string
		spec validations.TableSpec
	}{{"source", pair.A}, {"destination", pair.B}} {
		if !side.spec.Captured.Has(validations.SectionIndexes) {
			return "", fmt.Errorf(
				"PREFLIGHT_INSPECTION_INTEGRITY: %s spec for table %q did not capture its index "+
					"section, so schema compatibility cannot be evaluated. Preflight fails closed",
				side.name, pair.Table)
		}
	}

	// Rule 1: any generated DESTINATION column is fatal, even when the source column is
	// identically generated. The copy inserts explicit values for every column, and MySQL
	// rejects an explicit insert into a generated column with Error 3105 — INSERT IGNORE
	// included. A SOURCE-generated column writing into a plain destination column is
	// fine: SELECT materialises the value.
	//
	// Only the DESTINATION is inspected, deliberately. A source column with an
	// unpopulated Generated state cannot make the copy fail — SELECT reads whatever the
	// server materialises either way — so it is not an integrity failure here.
	for _, col := range pair.B.Columns {
		switch col.Generated {
		case validations.GeneratedVirtual, validations.GeneratedStored:
			return fmt.Sprintf(
				"%s(column %s: destination column is generated (%s); the copy inserts explicit values "+
					"for every column and MySQL rejects them with Error 3105 even under INSERT IGNORE)",
				pair.Table, col.Name, col.Generated), nil
		case validations.GeneratedNone:
			// Plain destination column.
		default:
			// GeneratedUnknown is the ZERO value of GeneratedKind, so this arm also
			// catches a ColumnSpec whose Generated field was never populated.
			return "", fmt.Errorf(
				"PREFLIGHT_INSPECTION_INTEGRITY: destination column %s.%s has unpopulated or unknown generated state %v",
				pair.Table, col.Name, col.Generated)
		}
	}

	// Rule 2: the PRIMARY key part list must be identical, in order. INSERT IGNORE
	// crash-recovery idempotency depends on the destination rejecting a re-inserted row
	// by the same key the source identified it with.
	aPrimary, aOK := primaryIndex(pair.A)
	bPrimary, bOK := primaryIndex(pair.B)
	switch {
	case !aOK && !bOK:
		// Neither side has one. Not this check's problem: PRIMARY_KEY_CHECK (position 3)
		// already rejected a source table without a PRIMARY KEY.
	case aOK != bOK:
		return fmt.Sprintf(
			"%s(primary key mismatch: the destination must keep the source primary key for idempotent resume; "+
				"source has PRIMARY=%t, destination has PRIMARY=%t)", pair.Table, aOK, bOK), nil
	default:
		aSig := make([]string, 0, len(aPrimary.Parts))
		for _, part := range aPrimary.Parts {
			aSig = append(aSig, primaryPartSignature(part))
		}
		bSig := make([]string, 0, len(bPrimary.Parts))
		for _, part := range bPrimary.Parts {
			bSig = append(bSig, primaryPartSignature(part))
		}
		// Element-wise, not a joined string: concatenating signatures could in principle
		// let a pathological expression forge a boundary. The joins below are for the
		// operator message only.
		if !slices.Equal(aSig, bSig) {
			return fmt.Sprintf(
				"%s(primary key mismatch: the destination must keep the source primary key for idempotent resume; "+
					"source=%s, destination=%s)", pair.Table,
				strings.Join(aSig, ""), strings.Join(bSig, "")), nil
		}
	}

	return "", nil
}

// isFunctionalIndex reports whether any key part indexes an expression rather than a
// column. A part indexes either a column or an expression, never both.
func isFunctionalIndex(idx validations.IndexSpec) bool {
	for _, part := range idx.Parts {
		if part.Column == "" && part.Expression != "" {
			return true
		}
	}
	return false
}

// columnCollation returns the collation of the named column, or "" when the column is
// not a string type or is not present.
//
// It looks a key part's column up in the SAME side's spec. part.Column and col.Name come
// from one catalogue, so byte-exact is correct here; the cross-side fold lives in
// uniquenessSignature, not in this lookup.
func columnCollation(spec validations.TableSpec, column string) string {
	for _, col := range spec.Columns {
		if col.Name == column {
			return col.Collation
		}
	}
	return ""
}

// uniquenessSignature renders the uniqueness predicate one unique index enforces
// (deviation D3, spec §3.3).
//
// A part signature is (kind, identity, prefix, collation):
//
//   - identity is the column name folded to ASCII lower case — MySQL resolves column
//     names case-insensitively (dbsgomysql COMPAT entry 28; error 1060 forbids two
//     columns differing only by case in one table, so the fold is injective per table
//     and cannot forge a collision the server does not already have) — or the exact
//     server-rewritten expression text, which is NEVER folded: it carries string
//     literals where case is meaning
//   - prefix is IndexPart.SubPart; zero means the whole value, so UNIQUE(email(10)) and
//     UNIQUE(email) are DIFFERENT signatures
//   - collation is the indexed column's collation, empty for non-string columns and for
//     functional parts. Uniqueness is collation-dependent: the same UNIQUE(email)
//     topology under a case/accent-insensitive destination collation collides rows that
//     are distinct under a binary source collation, and INSERT IGNORE silently skips one
//
// Descending is ignored (direction does not change the predicate), the index name is
// ignored, and part order is ignored — UNIQUE(a,b) and UNIQUE(b,a) enforce the same
// row-uniqueness predicate — which is why the parts are sorted.
func uniquenessSignature(idx validations.IndexSpec, spec validations.TableSpec) []uniquePart {
	parts := make([]uniquePart, 0, len(idx.Parts))
	for _, part := range idx.Parts {
		if part.Column != "" {
			parts = append(parts, uniquePart{
				Identity:  asciiLower(part.Column),
				Prefix:    part.SubPart,
				Collation: columnCollation(spec, part.Column),
			})
			continue
		}
		// Functional part: no result collation is exposed, so Collation is deliberately
		// empty and the conservative rule in checkDestinationUniqueness compensates.
		parts = append(parts, uniquePart{
			Expression: true,
			Identity:   part.Expression,
			Prefix:     part.SubPart,
		})
	}
	slices.SortFunc(parts, compareUniqueParts)
	return parts
}

// uniquePart is one key part's contribution to a uniqueness predicate.
//
// It is compared BY VALUE and must never be flattened into a delimited string for
// comparison. MySQL permits any character inside a backtick-quoted identifier, including
// whatever delimiters an encoding would pick, so a column can be named to forge another
// signature's rendering. The first implementation of this file used
// "col|<name>|<prefix>|<collation>" joined by "&&", and a single column named
// `a|0|&&col|b` rendered exactly what a composite UNIQUE(a, b) rendered — so a STRICTER
// destination unique index was accepted as equivalent, failing open on the precise silent
// row loss D3 exists to prevent (found in review of PR #59; regression test
// TestD3SignatureCannotBeForgedByAnIdentifier).
//
// formatSignature renders these for OPERATOR MESSAGES ONLY.
type uniquePart struct {
	Expression bool // Identity is an expression rather than a column name
	Identity   string
	Prefix     int
	Collation  string
}

// compareUniqueParts orders parts deterministically so two equivalent indexes sort to the
// same sequence regardless of declaration order.
func compareUniqueParts(a, b uniquePart) int {
	if a.Expression != b.Expression {
		if a.Expression {
			return 1
		}
		return -1
	}
	if c := strings.Compare(a.Identity, b.Identity); c != 0 {
		return c
	}
	if a.Prefix != b.Prefix {
		if a.Prefix < b.Prefix {
			return -1
		}
		return 1
	}
	return strings.Compare(a.Collation, b.Collation)
}

// hasEquivalentSignature reports whether any source signature equals want element-wise.
// A linear scan is right here: a table has a handful of unique indexes, and the whole
// point is that no string key may stand in for the parts.
func hasEquivalentSignature(sources [][]uniquePart, want []uniquePart) bool {
	for _, candidate := range sources {
		if slices.Equal(candidate, want) {
			return true
		}
	}
	return false
}

// formatSignature renders a signature for an operator message. It is DISPLAY ONLY — never
// compare these strings; see uniquePart. Column identities render in ASCII lower case
// because that is what the signature holds; the index NAME in the surrounding message
// carries the operator's own spelling.
func formatSignature(parts []uniquePart) string {
	rendered := make([]string, 0, len(parts))
	for _, part := range parts {
		kind := "column"
		if part.Expression {
			kind = "expression"
		}
		rendered = append(rendered, fmt.Sprintf("%s %q prefix=%d collation=%q",
			kind, part.Identity, part.Prefix, part.Collation))
	}
	return strings.Join(rendered, " + ")
}

// checkDestinationUniqueness applies deviation D3: a destination unique index whose
// uniqueness signature has no exactly-equal counterpart among the source's unique
// signatures is fatal, because INSERT IGNORE would silently skip rows the source holds
// as distinct.
//
// PRIMARY is excluded — its equality is checkAbsoluteInvariants' job, and judging it
// twice would produce two different messages for one defect.
//
// Conservative functional rule: IndexPart does not expose an expression's RESULT
// collation, so a destination functional unique index is accepted only when its
// expression text matches a source functional unique EXACTLY and the table has no
// ColumnCharsetMismatch or ColumnCollationMismatch diff on any column — an identical
// column environment is the only available proof that the expression's equality
// semantics match.
//
// diffs is DiffSpecs' output for this pair; pass nil when there are none.
func checkDestinationUniqueness(pair specPair, diffs []validations.SpecDiff) string {
	var sourceSignatures [][]uniquePart
	for _, idx := range pair.A.Indexes {
		if !idx.Unique || idx.Name == "PRIMARY" {
			continue
		}
		sourceSignatures = append(sourceSignatures, uniquenessSignature(idx, pair.A))
	}

	columnEnvironmentClean := true
	for _, diff := range diffs {
		if diff.Kind == validations.ColumnCharsetMismatch || diff.Kind == validations.ColumnCollationMismatch {
			columnEnvironmentClean = false
			break
		}
	}

	for _, idx := range pair.B.Indexes {
		if !idx.Unique || idx.Name == "PRIMARY" {
			continue
		}
		signature := uniquenessSignature(idx, pair.B)
		if !hasEquivalentSignature(sourceSignatures, signature) {
			return fmt.Sprintf(
				"%s(destination unique index %q has no equivalent on the source (signature: %s); "+
					"INSERT IGNORE would silently skip rows the source holds as distinct. Drop the "+
					"destination unique index, or add an equivalent one to the source)",
				pair.Table, idx.Name, formatSignature(signature))
		}
		if isFunctionalIndex(idx) && !columnEnvironmentClean {
			return fmt.Sprintf(
				"%s(destination functional unique index %q matches a source expression textually, but "+
					"the table has a column charset or collation difference, so GoArchive cannot prove "+
					"the expression's equality semantics match. Align the column charsets and collations, "+
					"or drop the destination index)",
				pair.Table, idx.Name)
		}
	}
	return ""
}
