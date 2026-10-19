package preflight

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"

	"github.com/dbsmedya/goarchive/v3/internal/key"
)

// readKeys reads every job table's primary key from the source. It runs even when policy
// checks are skipped, because every key-addressed statement is built from these
// definitions (spec §8.3). It refuses a table without a PRIMARY KEY and every key type
// outside the allowlist (spec §4.4).
func readKeys(ctx context.Context, f *facts) error {
	pks, err := f.sourcePKs(ctx)
	if err != nil {
		return inspectionError("keys", err)
	}
	specs, err := f.sourceSpecs(ctx)
	if err != nil {
		return inspectionError("keys", err)
	}
	keys, err := buildKeys(f.tables, pks, specs)
	if err != nil {
		return err
	}
	f.keys = keys
	return nil
}

// buildKeys builds one key.Def per table. Refusals are collected across tables, missing
// keys first, then unsupported types; a fact GoArchive cannot interpret is a plain error.
func buildKeys(tables []string, pks []validations.PKInfo, specs map[string]validations.TableSpec) (map[string]key.Def, error) {
	byTable := make(map[string]validations.PKInfo, len(pks))
	for _, pk := range pks {
		byTable[pk.Table] = pk
	}
	keys := make(map[string]key.Def, len(tables))
	var noKey, unsupported []string
	for _, t := range tables {
		pk, ok := byTable[t]
		if !ok {
			return nil, fmt.Errorf("preflight keys: no primary-key fact for table %q; the table was not found", t)
		}
		def, err := key.FromFacts(pk, specs[t])
		var np *key.NoPrimaryKeyError
		var ue *key.UnsupportedError
		switch {
		case errors.As(err, &np):
			noKey = append(noKey, t)
		case errors.As(err, &ue):
			unsupported = append(unsupported, fmt.Sprintf("%s.%s (%s): %s", ue.Table, ue.Column, ue.Type, ue.Reason))
		case err != nil:
			return nil, fmt.Errorf("preflight keys: %w", err)
		default:
			keys[t] = def
		}
	}
	if len(noKey) > 0 {
		return nil, &Error{Check: "PRIMARY_KEY_CHECK",
			Message: "Every participating table must have a PRIMARY KEY. GoArchive reads, copies, verifies " +
				"and deletes rows by the key MySQL reports, so a table without one cannot be addressed",
			Tables: noKey}
	}
	if len(unsupported) > 0 {
		return nil, &Error{Check: "KEY_TYPE_UNSUPPORTED",
			Message: "These primary-key columns cannot identify rows. Allowed key types are the integer types, " +
				"DECIMAL, CHAR, VARCHAR, BINARY, VARBINARY, DATE, DATETIME, TIMESTAMP, TIME and YEAR, " +
				"without a prefix length",
			Tables: unsupported}
	}
	return keys, nil
}

// checkPrimaryKeyColumns refuses a documented primary_key that is not exactly the key
// MySQL reports: the same columns, in key order, spelled exactly (R31). A missing, extra
// or reordered column fails PK_COLUMN_CHECK; a difference in letter case only fails
// PK_COLUMN_CASE_CHECK, reported first because its fix differs.
func checkPrimaryKeyColumns(_ context.Context, f *facts) error {
	var mismatch, caseOnly []string
	for _, n := range f.tree.CopyOrder() {
		real := f.keys[n.Table].Names()
		doc := n.PrimaryKey
		switch {
		case slices.Equal(doc, real):
		case len(doc) == len(real) && foldEqualAll(doc, real):
			caseOnly = append(caseOnly, fmt.Sprintf("%s(primary_key %s but the PRIMARY KEY is %s)",
				n.Table, columnList(doc), columnList(real)))
		default:
			mismatch = append(mismatch, fmt.Sprintf("%s(primary_key %s but the PRIMARY KEY is %s)",
				n.Table, columnList(doc), columnList(real)))
		}
	}
	if len(caseOnly) > 0 {
		return &Error{Check: "PK_COLUMN_CASE_CHECK",
			Message: "The configured primary_key differs from the PRIMARY KEY only in letter case. " +
				"Write each column exactly as the database spells it",
			Tables: caseOnly}
	}
	if len(mismatch) > 0 {
		return &Error{Check: "PK_COLUMN_CHECK",
			Message: "The configured primary_key must list the PRIMARY KEY's columns, all of them, in key order",
			Tables:  mismatch}
	}
	return nil
}

func foldEqualAll(a, b []string) bool {
	for i := range a {
		if !asciiFoldEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}
