//go:build integration

package archiver

import (
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/graph"
)

// chrSha256Verification selects the advisory (non-strict) charset path.
func chrSha256Verification() config.VerificationConfig {
	return config.VerificationConfig{Method: "sha256"}
}

// chrCountVerification selects the strict charset path (the default).
func chrCountVerification() config.VerificationConfig {
	return config.VerificationConfig{Method: "count"}
}

// chrSkipVerification selects sha256 as the Method but disables verification
// entirely (--skip-verify), so a charset mismatch is fatal for the
// SkipVerification arm of charsetMismatchFatal, not the Method arm.
func chrSkipVerification() config.VerificationConfig {
	return config.VerificationConfig{Method: "sha256", SkipVerification: true}
}

// TestCharacterizationDestTableExistence pins DEST_TABLE_EXISTENCE_CHECK and the
// purge exemption: the destination checks are skipped entirely under
// PreflightProfileSourceOnly (preflight.go:188), so purge PASSES the same fixture that
// fails for the other four commands.
func TestCharacterizationDestTableExistence(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	const ddl = "CREATE TABLE orders (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB"
	f.ExecSource(t, ctx, ddl)
	// destination deliberately empty

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			err := chrRun(t, ctx, f.Checker(t, graph.NewGraph("orders", "id")), cmd, false)
			if cmd.Name == "purge" {
				chrAssertPasses(t, err)
				return
			}
			chrAssertCheck(t, err, "DEST_TABLE_EXISTENCE_CHECK", []string{"orders"})
		})
	}
}

// TestCharacterizationDestSchemaTypeMismatch pins rule 2: a differing column type is
// fatal for every command except purge.
func TestCharacterizationDestSchemaTypeMismatch(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, amount decimal(10,2) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
	f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, amount decimal(12,2) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			err := chrRun(t, ctx, f.Checker(t, graph.NewGraph("orders", "id")), cmd, false)
			if cmd.Name == "purge" {
				chrAssertPasses(t, err)
				return
			}
			chrAssertCheck(t, err, "DEST_SCHEMA_COMPATIBILITY_CHECK", []string{"orders"})
		})
	}
}

// TestCharacterizationDestSchemaLooserDestinationPasses pins the direction matrix: the
// destination may drop secondary indexes, auto_increment, defaults and ON UPDATE, and
// may relax NOT NULL. All of that must PASS.
func TestCharacterizationDestSchemaLooserDestinationPasses(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, "CREATE TABLE orders ("+
		"id bigint NOT NULL AUTO_INCREMENT, "+
		"ref bigint NULL, "+
		"note varchar(64) NOT NULL, "+
		"touched timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "+
		"PRIMARY KEY (id), KEY idx_ref (ref)) ENGINE=InnoDB")
	f.ExecDest(t, ctx, "CREATE TABLE orders ("+
		"id bigint NOT NULL, "+
		"ref bigint NULL, "+
		"note varchar(64) NULL, "+
		"touched timestamp NOT NULL, "+
		"PRIMARY KEY (id)) ENGINE=InnoDB")

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			chrAssertPasses(t, chrRun(t, ctx, f.Checker(t, graph.NewGraph("orders", "id")), cmd, false))
		})
	}
}

// TestCharacterizationDestSchemaStricterDestinationFails pins the reverse direction:
// destination-only NOT NULL where the source allows NULL is fatal (rule 3).
func TestCharacterizationDestSchemaStricterDestinationFails(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, note varchar(64) NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
	f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, note varchar(64) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")

	err := chrRun(t, ctx, f.Checker(t, graph.NewGraph("orders", "id")), chrCommands[0], false)
	chrAssertCheck(t, err, "DEST_SCHEMA_COMPATIBILITY_CHECK", []string{"orders"})
}

// TestCharacterizationDestSchemaGeneratedDestinationFails pins rule 6: a generated
// destination column is fatal even when the source column is identically generated —
// MySQL rejects explicit inserts with Error 3105 under INSERT IGNORE too.
func TestCharacterizationDestSchemaGeneratedDestinationFails(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	const ddl = "CREATE TABLE orders (id bigint NOT NULL, amount int NOT NULL, doubled int GENERATED ALWAYS AS (amount * 2) STORED, PRIMARY KEY (id)) ENGINE=InnoDB"
	f.ExecSource(t, ctx, ddl)
	f.ExecDest(t, ctx, ddl)

	err := chrRun(t, ctx, f.Checker(t, graph.NewGraph("orders", "id")), chrCommands[0], false)
	chrAssertCheck(t, err, "DEST_SCHEMA_COMPATIBILITY_CHECK", []string{"orders"})
}

// TestCharacterizationDestSchemaMissingPKFails pins rule 4: the destination must keep
// the source primary key, because INSERT IGNORE crash-recovery idempotency depends on it.
func TestCharacterizationDestSchemaMissingPKFails(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, note varchar(64) NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
	f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, note varchar(64) NULL) ENGINE=InnoDB")

	err := chrRun(t, ctx, f.Checker(t, graph.NewGraph("orders", "id")), chrCommands[0], false)
	chrAssertCheck(t, err, "DEST_SCHEMA_COMPATIBILITY_CHECK", []string{"orders"})
}

// TestCharacterizationDestSchemaAggregatesAcrossTables pins the aggregation contract:
// TWO incompatible tables produce ONE error naming BOTH, not an abort on the first.
// Spec §3.3 preserves this exactly.
//
// DEVIATION from the plan's literal code, two-fold:
//
//  1. The plan used the child table name "lines". LINES is a MySQL reserved word
//     (confirmed against the live server, same as phase 005's finding for the
//     identical table name). Renamed to "order_lines" throughout, matching phases
//     004/005's convention of descriptive, non-reserved child table names.
//  2. The plan built the child node with `g.SetPK("lines", "id")` followed by a bare
//     `g.AddEdgeWithMeta("orders", "lines", "order_id", "id", "")` — this is THE TRAP:
//     AddEdgeWithMeta records only the edge, never calling AddNode, so "lines" would
//     be absent from g.Nodes and therefore invisible to p.graph.AllNodes(), which is
//     exactly the `tables` slice ValidateDestinationSchemaCompatibility iterates
//     (preflight.go:142,1165). The test would have PASSED while checking only
//     "orders" — silently testing nothing about aggregation. Fixed by using
//     chrAddChild, which registers the node, the edge, and the child PK together.
func TestCharacterizationDestSchemaAggregatesAcrossTables(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, amount decimal(10,2) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
	f.ExecSource(t, ctx, "CREATE TABLE order_lines (id bigint NOT NULL, order_id bigint NOT NULL, qty int NOT NULL, PRIMARY KEY (id), KEY idx_o (order_id), CONSTRAINT fk_ol_o FOREIGN KEY (order_id) REFERENCES orders (id)) ENGINE=InnoDB")
	f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, amount decimal(12,2) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
	f.ExecDest(t, ctx, "CREATE TABLE order_lines (id bigint NOT NULL, order_id bigint NOT NULL, qty bigint NOT NULL, PRIMARY KEY (id), KEY idx_o (order_id), CONSTRAINT fk_ol_o FOREIGN KEY (order_id) REFERENCES orders (id)) ENGINE=InnoDB")

	g := graph.NewGraph("orders", "id")
	chrAddChild(g, "orders", "id", "order_lines", "order_id", "id")

	err := chrRun(t, ctx, f.Checker(t, g), chrCommands[0], false)
	chrAssertCheck(t, err, "DEST_SCHEMA_COMPATIBILITY_CHECK", []string{"order_lines", "orders"})
}

// TestCharacterizationDestSchemaFirstOffenderPerTable pins the within-table rule: a
// table with TWO incompatible columns yields exactly ONE Tables entry, for the FIRST
// offending column in ordinal order.
//
// The decoration format changed in phase 028 (authorized by phase 006 Ambiguity 2 /
// spec §1.1): the 1.8 TableSpec-less comparison named the ordinal ("position 2"); the
// TableSpec/DiffSpecs evaluator names the column instead, since a SpecDiff carries the
// column name, not its ordinal position — the column name is strictly more useful anyway.
func TestCharacterizationDestSchemaFirstOffenderPerTable(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, a int NOT NULL, b int NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
	f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, a bigint NOT NULL, b bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")

	err := chrRun(t, ctx, f.Checker(t, graph.NewGraph("orders", "id")), chrCommands[0], false)
	chrAssertCheck(t, err, "DEST_SCHEMA_COMPATIBILITY_CHECK", []string{"orders"})

	pe := err.(*PreflightError)
	if len(pe.Tables) != 1 {
		t.Fatalf("expected exactly 1 Tables entry (first offender per table), got %d: %v", len(pe.Tables), pe.Tables)
	}
	if !strings.Contains(pe.Tables[0], "column a") {
		t.Fatalf("expected the FIRST offending column (a, ordinal 2) to be reported, got %q", pe.Tables[0])
	}
}

// TestCharacterizationDestSchemaCharsetFatalUnderCount pins rule 7's strict path: a
// charset mismatch is FATAL under count verification.
func TestCharacterizationDestSchemaCharsetFatalUnderCount(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, note varchar(64) CHARACTER SET utf8mb4 NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
	f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, note varchar(64) CHARACTER SET latin1 NULL, PRIMARY KEY (id)) ENGINE=InnoDB")

	checker := f.Checker(t, graph.NewGraph("orders", "id"))
	checker.SetVerification(chrCountVerification())

	err := chrRun(t, ctx, checker, chrCommands[0], false)
	chrAssertCheck(t, err, "DEST_SCHEMA_COMPATIBILITY_CHECK", []string{"orders"})
}

// TestCharacterizationDestSchemaCharsetWarnsUnderSha256 pins rule 7's advisory path:
// under a sha256 verification that will run, the same mismatch PASSES with a warning.
func TestCharacterizationDestSchemaCharsetWarnsUnderSha256(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, note varchar(64) CHARACTER SET utf8mb4 NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
	f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, note varchar(64) CHARACTER SET latin1 NULL, PRIMARY KEY (id)) ENGINE=InnoDB")

	checker := f.Checker(t, graph.NewGraph("orders", "id"))
	checker.SetVerification(chrSha256Verification())

	chrAssertPasses(t, chrRun(t, ctx, checker, chrCommands[0], false))

	warns := f.WarnMessages()
	if !chrAnyContains(warns, "charset differs") {
		t.Fatalf("expected a charset advisory warning, got: %v", warns)
	}
	if chrAnyContains(warns, "collation differs") {
		t.Fatalf("collation advisory must be SUPPRESSED for a column that already warned on charset; got: %v", warns)
	}
}

// TestCharacterizationDestSchemaCharsetFatalUnderSkipVerify pins rule 7's other
// strict path: charsetMismatchFatal() (preflight.go:1668-1670) is a DISJUNCTION —
// SkipVerification || EffectiveMethod() != "sha256" — and the two existing charset
// tests above exercise only the second arm (Method: "count" is fatal, Method:
// "sha256" is advisory). This test exercises the FIRST arm: Method is deliberately
// "sha256" (the advisory method), with SkipVerification: true the only thing that
// can make it fatal. This must stay fatal: with verification disabled there is no
// later gate to catch a transliterated column, and archive/purge DELETE the source
// rows right after copy — a charset mismatch under --skip-verify is silent,
// unrecoverable data loss, not merely an unverified copy.
func TestCharacterizationDestSchemaCharsetFatalUnderSkipVerify(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, note varchar(64) CHARACTER SET utf8mb4 NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
	f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, note varchar(64) CHARACTER SET latin1 NULL, PRIMARY KEY (id)) ENGINE=InnoDB")

	checker := f.Checker(t, graph.NewGraph("orders", "id"))
	checker.SetVerification(chrSkipVerification())

	err := chrRun(t, ctx, checker, chrCommands[0], false)
	chrAssertCheck(t, err, "DEST_SCHEMA_COMPATIBILITY_CHECK", []string{"orders"})
}

// TestCharacterizationDestSchemaCollationWarnsAlone pins the second half of the
// suppression rule: a collation-only difference (same charset) DOES warn.
func TestCharacterizationDestSchemaCollationWarnsAlone(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, note varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
	f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, note varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL, PRIMARY KEY (id)) ENGINE=InnoDB")

	checker := f.Checker(t, graph.NewGraph("orders", "id"))
	checker.SetVerification(chrCountVerification())

	chrAssertPasses(t, chrRun(t, ctx, checker, chrCommands[0], false))

	warns := f.WarnMessages()
	if !chrAnyContains(warns, "collation differs") {
		t.Fatalf("expected a collation advisory warning, got: %v", warns)
	}
}

// TestCharacterizationDestInsertTrigger pins DEST_INSERT_TRIGGER_CHECK, which — like
// the other destination checks — is skipped by purge.
func TestCharacterizationDestInsertTrigger(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	const ddl = "CREATE TABLE orders (id bigint NOT NULL, n int NOT NULL DEFAULT 0, PRIMARY KEY (id)) ENGINE=InnoDB"
	f.ExecSource(t, ctx, ddl)
	f.ExecDest(t, ctx, ddl)
	f.ExecDest(t, ctx, "CREATE TRIGGER trg_orders_ins BEFORE INSERT ON orders FOR EACH ROW SET NEW.n = NEW.n + 1")

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			err := chrRun(t, ctx, f.Checker(t, graph.NewGraph("orders", "id")), cmd, false)
			if cmd.Name == "purge" {
				chrAssertPasses(t, err)
				return
			}
			chrAssertCheck(t, err, "DEST_INSERT_TRIGGER_CHECK", []string{"orders"})
			chrAssertRawTables(t, err, []string{"orders(trg_orders_ins)"})
		})
	}
}

// chrAnyContains reports whether any string in list contains sub.
func chrAnyContains(list []string, sub string) bool {
	for _, s := range list {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
