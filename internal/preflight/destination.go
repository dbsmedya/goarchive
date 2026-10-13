package preflight

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
)

// checkDestinationKeys refuses a destination PRIMARY KEY that is not the source's: the
// same columns in the same order, each with the same type, charset and collation (spec
// §8.3). A different collation changes which keys collide and the order verification reads
// rows in. One widening is allowed (R32): a utf8mb3 key column with collation utf8mb3_X may
// land in utf8mb4 with the twin collation utf8mb4_X. On utf8mb3's characters, which are all
// in the Basic Multilingual Plane, twins have the same weights and PAD attribute.
func checkDestinationKeys(ctx context.Context, f *facts) error {
	pks, err := f.destPKs(ctx)
	if err != nil {
		return inspectionError("destination_keys", err)
	}
	specs, err := f.destSpecs(ctx)
	if err != nil {
		return inspectionError("destination_keys", err)
	}
	byTable := make(map[string]validations.PKInfo, len(pks))
	for _, pk := range pks {
		byTable[pk.Table] = pk
	}
	dstKeys := make(map[string]key.Def, len(f.tables))
	var issues []string
	for _, t := range f.tables {
		pk, ok := byTable[t]
		if !ok {
			return fmt.Errorf("preflight destination keys: no primary-key fact for destination table %q", t)
		}
		src := f.keys[t]
		dst, err := key.FromFacts(pk, specs[t])
		var np *key.NoPrimaryKeyError
		var ue *key.UnsupportedError
		switch {
		case errors.As(err, &np):
			issues = append(issues, fmt.Sprintf("%s(the destination has no PRIMARY KEY; the source's is %s)", t, columnList(src.Names())))
			continue
		case errors.As(err, &ue):
			issues = append(issues, fmt.Sprintf("%s(destination key column %s is %s: %s)", t, ue.Column, ue.Type, ue.Reason))
			continue
		case err != nil:
			return fmt.Errorf("preflight destination keys: %w", err)
		}
		dstKeys[t] = dst
		if issue := keyIdentity(src, dst); issue != "" {
			issues = append(issues, t+"("+issue+")")
		}
	}
	if len(issues) > 0 {
		return &Error{Check: "DEST_KEY_IDENTITY_CHECK",
			Message: "The destination PRIMARY KEY must have the source's columns, in key order, with the same " +
				"type, charset and collation. The one exception is a utf8mb3 key column copied into utf8mb4 " +
				"with the twin collation, such as utf8mb3_general_ci to utf8mb4_general_ci",
			Tables: issues}
	}
	f.dstKeys = dstKeys
	return nil
}

// readDestinationKeys reads the destination keys without judging them, for
// --skip-dangerously-validate-preflight-allow-experimental: the statement caps of destination statements are computed from
// them (spec §8.3: the key definitions are always read). A destination table without a key
// the engine can address fails, because no cap could be computed for it.
func readDestinationKeys(ctx context.Context, f *facts) error {
	pks, err := f.destPKs(ctx)
	if err != nil {
		return inspectionError("destination_keys", err)
	}
	specs, err := f.destSpecs(ctx)
	if err != nil {
		return inspectionError("destination_keys", err)
	}
	byTable := make(map[string]validations.PKInfo, len(pks))
	for _, pk := range pks {
		byTable[pk.Table] = pk
	}
	dstKeys := make(map[string]key.Def, len(f.tables))
	for _, t := range f.tables {
		pk, ok := byTable[t]
		if !ok {
			return fmt.Errorf("preflight destination keys: no primary-key fact for destination table %q", t)
		}
		dst, err := key.FromFacts(pk, specs[t])
		if err != nil {
			return fmt.Errorf("preflight destination keys: %w", err)
		}
		dstKeys[t] = dst
	}
	f.dstKeys = dstKeys
	return nil
}

// keyIdentity returns how dst differs from the source key src, or "".
func keyIdentity(src, dst key.Def) string {
	if len(src.Columns) != len(dst.Columns) || !foldEqualAll(src.Names(), dst.Names()) {
		return fmt.Sprintf("the destination PRIMARY KEY is %s but the source's is %s", columnList(dst.Names()), columnList(src.Names()))
	}
	for i, s := range src.Columns {
		d := dst.Columns[i]
		if s.Family != d.Family || !typesCompatible(s.Type, d.Type) {
			return fmt.Sprintf("key column %s: source %s, destination %s", s.Name, s.Type, d.Type)
		}
		if (s.Charset != d.Charset || s.Collation != d.Collation) && !twinCollation(s, d) {
			return fmt.Sprintf("key column %s: source %s %s, destination %s %s", s.Name, s.Charset, s.Collation, d.Charset, d.Collation)
		}
	}
	return ""
}

// twinCollation reports the one allowed key widening, utf8mb3_X to utf8mb4_X (R32).
func twinCollation(s, d key.Column) bool {
	return twin(s.Charset, s.Collation, d.Charset, d.Collation)
}

// twin reports whether a source utf8mb3 column with collation utf8mb3_X lands in utf8mb4
// with the twin collation utf8mb4_X. On utf8mb3's characters, all in the Basic
// Multilingual Plane, twins have the same weights and PAD attribute, so they compare,
// order and collide alike.
func twin(srcCharset, srcCollation, dstCharset, dstCollation string) bool {
	suffix, ok := strings.CutPrefix(srcCollation, "utf8mb3_")
	return ok && srcCharset == "utf8mb3" && dstCharset == "utf8mb4" && dstCollation == "utf8mb4_"+suffix
}

// foldColumn finds a column by name, ASCII case folded, as MySQL resolves column names.
func foldColumn(t validations.TableSpec, name string) (validations.ColumnSpec, bool) {
	for _, c := range t.Columns {
		if asciiFoldEqual(c.Name, name) {
			return c, true
		}
	}
	return validations.ColumnSpec{}, false
}

// twinColumn reports whether the column called name is the R32 twin on dst of its source
// column.
func twinColumn(src, dst validations.TableSpec, name string) bool {
	s, ok := foldColumn(src, name)
	d, ok2 := foldColumn(dst, name)
	return ok && ok2 && twin(s.Charset, s.Collation, d.Charset, d.Collation)
}

// intDisplayWidth matches the deprecated integer display width, the digits after an
// integer type keyword. It never touches varchar(255) or decimal(10,2).
var intDisplayWidth = regexp.MustCompile(`^(tinyint|smallint|mediumint|int|integer|bigint)\(\d+\)`)

// typesCompatible accepts two COLUMN_TYPE strings that differ only in an integer display
// width. The library already ignores ordinary widths, but keeps tinyint(1) (BOOLEAN) and a
// ZEROFILL width; neither changes what a value can be, so a copy accepts them.
func typesCompatible(a, b string) bool {
	strip := func(t string) string { return intDisplayWidth.ReplaceAllString(strings.TrimSpace(t), "$1") }
	return strip(a) == strip(b)
}

// checkDestinationSchemaCompatibility needs every destination table able to receive
// copies of the source rows. The destination may be looser (dropped secondary indexes,
// AUTO_INCREMENT, defaults, ON UPDATE, NULL allowed) but never stricter. Every
// incompatible table is reported at once; each table's first fatal difference is shown.
// Advisory differences of compatible tables are logged.
func checkDestinationSchemaCompatibility(ctx context.Context, f *facts) error {
	srcSpecs, err := f.sourceSpecs(ctx)
	if err != nil {
		return inspectionError("destination_schema_compatibility", err)
	}
	dstSpecs, err := f.destSpecs(ctx)
	if err != nil {
		return inspectionError("destination_schema_compatibility", err)
	}
	charsetStrict := f.cfg.Verification != jobspec.VerifySHA256
	var incompatible, warnings []string
	for _, t := range f.tables {
		verdict, err := judgeSchema(t, srcSpecs[t], dstSpecs[t], f.keys[t].Names(), charsetStrict)
		if err != nil {
			return err
		}
		if verdict.fatal != "" {
			incompatible = append(incompatible, verdict.fatal)
			continue
		}
		for _, w := range verdict.warnings {
			warnings = append(warnings, "Table "+t+" "+w)
		}
	}
	for _, w := range warnings {
		f.cfg.Logger.Warn(w)
	}
	if len(incompatible) > 0 {
		return &Error{Check: "DEST_SCHEMA_COMPATIBILITY_CHECK", Message: "Source and destination schemas are incompatible", Tables: incompatible}
	}
	return nil
}

type schemaVerdict struct {
	fatal    string
	warnings []string
}

// judgeSchema applies the compatibility policy to one table. keyCols are the source key
// columns: their charset and collation were judged by checkDestinationKeys, which allows
// the R32 twin, so those differences are not judged again here.
func judgeSchema(table string, src, dst validations.TableSpec, keyCols []string, charsetStrict bool) (schemaVerdict, error) {
	var verdict schemaVerdict
	// A generated destination column is fatal even when the source column is generated
	// identically: the copy inserts a value for every column, and MySQL rejects that with
	// error 3105.
	for _, col := range dst.Columns {
		switch col.Generated {
		case validations.GeneratedVirtual, validations.GeneratedStored:
			verdict.fatal = fmt.Sprintf("%s(column %s: destination column is generated (%s); the copy inserts "+
				"a value for every column and MySQL rejects it with error 3105)", table, col.Name, col.Generated)
			return verdict, nil
		case validations.GeneratedNone:
		default:
			return verdict, fmt.Errorf("PREFLIGHT_INSPECTION_INTEGRITY: destination column %s.%s has unpopulated or unknown generated state %v",
				table, col.Name, col.Generated)
		}
	}

	diffs := validations.DiffSpecs(src, dst)
	if reason := checkDestinationUniqueness(table, src, dst, diffs); reason != "" {
		verdict.fatal = reason
		return verdict, nil
	}
	warnedCharset := map[string]bool{}
	for _, diff := range diffs {
		if (diff.Kind == validations.ColumnCharsetMismatch || diff.Kind == validations.ColumnCollationMismatch) &&
			slices.ContainsFunc(keyCols, func(c string) bool { return asciiFoldEqual(c, diff.Column) }) {
			continue
		}
		d, err := disposeDiff(diff, charsetStrict, warnedCharset)
		if err != nil {
			return schemaVerdict{}, err
		}
		switch {
		case d.fatal != "" && verdict.fatal == "":
			verdict.fatal = fmt.Sprintf("%s(column %s: %s; source=%s, destination=%s)", table, diff.Column, d.fatal, diff.A, diff.B)
		case d.warning != "":
			verdict.warnings = append(verdict.warnings, d.warning)
		}
	}
	return verdict, nil
}

type disposition struct {
	fatal   string
	warning string
}

// disposeDiff maps one SpecDiff to policy. Every kind the library publishes has a case;
// an unknown kind fails closed. warnedCharset suppresses a collation advisory on a column
// that already warned on its charset.
func disposeDiff(diff validations.SpecDiff, charsetStrict bool, warnedCharset map[string]bool) (disposition, error) {
	switch diff.Kind {
	case validations.ColumnAbsent:
		return disposition{fatal: "column missing on one side"}, nil
	case validations.ColumnOrderMismatch:
		return disposition{fatal: "column order mismatch"}, nil
	case validations.ColumnTypeMismatch:
		if typesCompatible(diff.A, diff.B) {
			return disposition{}, nil
		}
		return disposition{fatal: "column type mismatch"}, nil
	case validations.ColumnNullabilityMismatch:
		// Fatal only when the destination is stricter: A is the source, B the destination.
		if strings.EqualFold(diff.A, "true") && strings.EqualFold(diff.B, "false") {
			return disposition{fatal: "destination is NOT NULL but source allows NULL"}, nil
		}
		return disposition{}, nil
	case validations.ColumnNameCaseMismatch:
		// MySQL resolves column names case-insensitively, and the copy names the source's
		// columns on both sides, so copy and verification are unaffected.
		return disposition{warning: fmt.Sprintf(
			"column %s: name differs only in letter case (source=%s destination=%s); MySQL resolves column "+
				"names case-insensitively, so the copy and verification are unaffected", diff.Column, diff.A, diff.B)}, nil
	case validations.ColumnCharsetMismatch:
		if charsetStrict {
			return disposition{fatal: fmt.Sprintf(
				"character set mismatch (source=%s, destination=%s): copying can silently transliterate or "+
					"truncate text and count verification cannot detect it; align charsets or use sha256 verification",
				diff.A, diff.B)}, nil
		}
		warnedCharset[diff.Column] = true
		return disposition{warning: fmt.Sprintf(
			"column %s: charset differs (source=%s destination=%s); sha256 verification will fail before delete if data is altered",
			diff.Column, diff.A, diff.B)}, nil
	case validations.ColumnCollationMismatch:
		if warnedCharset[diff.Column] {
			return disposition{}, nil
		}
		return disposition{warning: fmt.Sprintf(
			"column %s: collation differs (source=%s destination=%s); stored bytes are identical but comparisons and sorting may differ in the archive",
			diff.Column, diff.A, diff.B)}, nil
	case validations.ColumnDefaultMismatch, validations.ColumnAutoIncrementMismatch, validations.ColumnOnUpdateMismatch:
		return disposition{}, nil
	case validations.ColumnGeneratedMismatch, validations.ColumnGenerationExprMismatch:
		// Judged above: any generated destination column is fatal.
		return disposition{}, nil
	case validations.ColumnVisibilityMismatch:
		// Every read and INSERT names its columns, so an INVISIBLE column is still copied.
		return disposition{}, nil
	case validations.IndexAbsent, validations.IndexPartsMismatch, validations.IndexUniquenessMismatch,
		validations.IndexTypeMismatch, validations.IndexVisibilityMismatch, validations.IndexNameCaseMismatch:
		// Index policy is the key identity and the uniqueness signatures, never name-keyed
		// index diffs, so renaming an equivalent index cannot fail.
		return disposition{}, nil
	case validations.EngineMismatch, validations.CharsetMismatch, validations.CollationMismatch:
		// Table-level defaults; the source engine is STORAGE_ENGINE_CHECK's.
		return disposition{}, nil
	case validations.IndexUnconfirmed:
		return disposition{}, fmt.Errorf("PREFLIGHT_INSPECTION_INTEGRITY: DiffSpecs reported IndexUnconfirmed on side %v "+
			"even though both sides captured indexes; the schema inspection cannot be trusted", diff.Side)
	case validations.CommentMismatch, validations.CommentUnconfirmed, validations.ConstraintUnconfirmed,
		validations.ConstraintKindUnconfirmed, validations.ConstraintAbsent, validations.ConstraintKindMismatch,
		validations.CheckClauseMismatch, validations.CheckEnforcementMismatch, validations.ForeignKeyColumnsMismatch,
		validations.ForeignKeyReferenceMismatch, validations.ForeignKeyRuleMismatch:
		return disposition{}, fmt.Errorf("PREFLIGHT_UNEXPECTED_DIFF: diff kind %v cannot occur, because GoArchive "+
			"captures neither constraints nor comments, but was observed for column %q / index %q. Preflight fails closed",
			diff.Kind, diff.Column, diff.Index)
	default:
		return disposition{}, fmt.Errorf("PREFLIGHT_UNKNOWN_DIFF: unrecognised schema difference kind %v (column %q, index %q). "+
			"This build of GoArchive does not know how to judge it, so preflight fails closed. Upgrade GoArchive or pin "+
			"the validation library version this build was released against", diff.Kind, diff.Column, diff.Index)
	}
}

// uniquePart is one part of the uniqueness predicate a unique index enforces. Parts are
// compared by value, never as a joined string: an identifier may contain any delimiter,
// so a rendering can be forged.
type uniquePart struct {
	Expression bool // Identity is an expression, not a column name
	Identity   string
	Prefix     int
	Collation  string
}

// uniquenessSignature is what a unique index makes unique: each part's column (ASCII
// folded, as MySQL resolves column names) or exact expression, its prefix length, and its
// column's collation, which decides which values collide. Part order and the index name do
// not matter, so the parts are sorted. collation names the collation a part compares
// under.
func uniquenessSignature(idx validations.IndexSpec, spec validations.TableSpec, collation func(validations.ColumnSpec) string) []uniquePart {
	parts := make([]uniquePart, 0, len(idx.Parts))
	for _, p := range idx.Parts {
		if p.Column != "" {
			c, _ := columnSpec(spec, p.Column)
			parts = append(parts, uniquePart{Identity: asciiLower(p.Column), Prefix: p.SubPart, Collation: collation(c)})
			continue
		}
		parts = append(parts, uniquePart{Expression: true, Identity: p.Expression, Prefix: p.SubPart})
	}
	slices.SortFunc(parts, func(a, b uniquePart) int {
		switch {
		case a.Expression != b.Expression:
			if a.Expression {
				return 1
			}
			return -1
		case a.Identity != b.Identity:
			return strings.Compare(a.Identity, b.Identity)
		case a.Prefix != b.Prefix:
			return a.Prefix - b.Prefix
		}
		return strings.Compare(a.Collation, b.Collation)
	})
	return parts
}

// checkDestinationUniqueness refuses a destination unique index with no equivalent on the
// source: rows the source holds as distinct would collide there, and the copy would fail
// with a duplicate-key error. PRIMARY is checkDestinationKeys'. A destination column that
// is the R32 twin of its source column compares under the source's collation, because
// twins collide the same rows. A functional unique index is accepted only when its
// expression matches a source one exactly and no column differs in charset or collation
// other than as a twin, because the expression's result collation is not exposed.
func checkDestinationUniqueness(table string, src, dst validations.TableSpec, diffs []validations.SpecDiff) string {
	own := func(c validations.ColumnSpec) string { return c.Collation }
	asSource := func(c validations.ColumnSpec) string {
		if s, ok := foldColumn(src, c.Name); ok && twinColumn(src, dst, c.Name) {
			return s.Collation
		}
		return c.Collation
	}
	var sources [][]uniquePart
	for _, idx := range src.Indexes {
		if idx.Unique && idx.Name != "PRIMARY" {
			sources = append(sources, uniquenessSignature(idx, src, own))
		}
	}
	clean := !slices.ContainsFunc(diffs, func(d validations.SpecDiff) bool {
		return (d.Kind == validations.ColumnCharsetMismatch || d.Kind == validations.ColumnCollationMismatch) &&
			!twinColumn(src, dst, d.Column)
	})
	for _, idx := range dst.Indexes {
		if !idx.Unique || idx.Name == "PRIMARY" {
			continue
		}
		sig := uniquenessSignature(idx, dst, asSource)
		if !slices.ContainsFunc(sources, func(s []uniquePart) bool { return slices.Equal(s, sig) }) {
			return fmt.Sprintf("%s(destination unique index %q has no equivalent on the source; rows the source holds "+
				"as distinct would collide and fail the copy. Drop the destination unique index, or add an equivalent "+
				"one to the source)", table, idx.Name)
		}
		functional := slices.ContainsFunc(idx.Parts, func(p validations.IndexPart) bool { return p.Column == "" })
		if functional && !clean {
			return fmt.Sprintf("%s(destination functional unique index %q matches a source expression textually, but "+
				"a column differs in charset or collation, so GoArchive cannot prove the expression's equality "+
				"semantics match. Align the column charsets and collations, or drop the destination index)", table, idx.Name)
		}
	}
	return ""
}
