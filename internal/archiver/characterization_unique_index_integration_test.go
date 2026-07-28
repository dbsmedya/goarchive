//go:build integration

package archiver

import (
	"testing"

	"github.com/dbsmedya/goarchive/internal/graph"
)

// ============================================================================
// D3 PRE-IMAGE.
//
// Every assertion in this file records what the per-column COLUMN_KEY policy
// does TODAY:
//
//	if d.ColumnKey == "UNI" && s.ColumnKey != "UNI" { … fatal … }   // preflight.go:1120
//
// information_schema.COLUMNS.COLUMN_KEY is a lossy per-column projection of the
// index picture: it reports 'UNI' only when the column is the FIRST column of a
// unique index and is not the primary key. Measured on the live test servers
// (MySQL 8.4.4-4, both 3305 and 3307) for
//
//	PRIMARY KEY (id), UNIQUE KEY uq_ab (a, b), UNIQUE KEY uq_email_prefix (email(10)):
//	  id → PRI      a → MUL      b → ''      email → UNI
//
// so the rule provably cannot see four destination-only uniqueness hazards.
//
// PRE-IMAGE — these four assert TODAY'S PERMISSIVE (buggy) BEHAVIOUR and MUST be
// INVERTED to fatal by deviation D3 (spec §3.3, §4), landed in phase 030:
//
//	TestCharacterizationUniqueCompositeDestOnly_PassesToday      composite UNIQUE(a,b), a → MUL
//	TestCharacterizationUniquePrefixDestOnly_PassesToday         UNIQUE(email(10)) vs UNIQUE(email), both → UNI
//	TestCharacterizationUniqueFunctionalDestOnly_PassesToday     UNIQUE((lower(email))), email → ''
//	TestCharacterizationUniqueCollationLoosened_WarnsOnlyToday   same UNIQUE, looser destination collation
//
// Each is a real INSERT-IGNORE silent-skip data-loss hazard. They are pinned as
// passing deliberately, so the amendment is visible as a diff rather than as a
// silent behaviour change. Do NOT "fix" them outside phase 030.
//
// OWNERSHIP — phase 030 amends this file; phase 031 does NOT.
//
//	Phase 030 Step 5 is the sanctioned amendment: rename each of the four above
//	from _PassesToday / _WarnsOnlyToday to _FatalUnderD3 and change each
//	chrAssertPasses to chrAssertCheck(t, err, "DEST_SCHEMA_COMPATIBILITY_CHECK",
//	[]string{"orders"}).
//
//	Phase 031's non-negotiables require this suite to stay green and UNAMENDED.
//	If you are implementing 031 and believe you need to change this file, stop —
//	either phase 030 skipped its Step 5, or you are about to weaken the baseline.
//
// NOT pre-image — these four must keep their current outcome after D3, and a
// phase-030 diff that touches them is a regression, not an amendment:
//
//	TestCharacterizationUniqueSingleColumnDestOnly_Fatal   already fatal, stays fatal
//	TestCharacterizationUniqueOverlappingDestOnly_Fatal    already fatal, stays fatal
//	TestCharacterizationUniqueRenamedEquivalent_PassesToday  index NAME is not part of the signature
//	TestCharacterizationUniqueSourceOnly_PassesToday         a looser destination is always allowed
//
// Only the `archive` command is exercised: DEST_SCHEMA_COMPATIBILITY_CHECK has
// identical applicability across archive / copy-only / dry-run / validate and is
// skipped by purge, which phase 006 already pins. Phase 010's matrix test is the
// authority on applicability.
// ============================================================================

// TestCharacterizationUniqueSingleColumnDestOnly_Fatal pins the ONE case the current
// rule does catch: a destination-only single-column UNIQUE index whose column reports
// COLUMN_KEY = UNI while the source column reports the empty string.
func TestCharacterizationUniqueSingleColumnDestOnly_Fatal(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, email varchar(64) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
	f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, email varchar(64) NOT NULL, PRIMARY KEY (id), UNIQUE KEY uq_email (email)) ENGINE=InnoDB")

	checker := f.Checker(t, graph.NewGraph("orders", "id"))
	checker.SetVerification(chrCountVerification())

	err := chrRun(t, ctx, checker, chrCommands[0], false)
	chrAssertCheck(t, err, "DEST_SCHEMA_COMPATIBILITY_CHECK", []string{"orders"})
}

// TestCharacterizationUniqueOverlappingDestOnly_Fatal pins the second caught case:
// source has UNIQUE(a,b) (so a → MUL) and destination has UNIQUE(a) (so a → UNI).
// The destination is genuinely stricter here, and the per-column rule happens to see it.
func TestCharacterizationUniqueOverlappingDestOnly_Fatal(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, a int NOT NULL, b int NOT NULL, PRIMARY KEY (id), UNIQUE KEY uq_ab (a, b)) ENGINE=InnoDB")
	f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, a int NOT NULL, b int NOT NULL, PRIMARY KEY (id), UNIQUE KEY uq_a (a)) ENGINE=InnoDB")

	checker := f.Checker(t, graph.NewGraph("orders", "id"))
	checker.SetVerification(chrCountVerification())

	err := chrRun(t, ctx, checker, chrCommands[0], false)
	chrAssertCheck(t, err, "DEST_SCHEMA_COMPATIBILITY_CHECK", []string{"orders"})
}

// GAP 1 — TestCharacterizationUniqueCompositeDestOnly_PassesToday.
// Destination-only UNIQUE(a,b). COLUMN_KEY of `a` is MUL, not UNI, so the per-column
// rule never sees it. This PASSES today and is a real silent-skip hazard: two source
// rows sharing (a,b) collide in the destination and INSERT IGNORE drops one.
// D3 (phase 030) makes it FATAL.
func TestCharacterizationUniqueCompositeDestOnly_PassesToday(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, a int NOT NULL, b int NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
	f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, a int NOT NULL, b int NOT NULL, PRIMARY KEY (id), UNIQUE KEY uq_ab (a, b)) ENGINE=InnoDB")

	checker := f.Checker(t, graph.NewGraph("orders", "id"))
	checker.SetVerification(chrCountVerification())

	chrAssertPasses(t, chrRun(t, ctx, checker, chrCommands[0], false))
}

// GAP 2 — TestCharacterizationUniquePrefixDestOnly_PassesToday.
// Source UNIQUE(email), destination UNIQUE(email(10)). Both report COLUMN_KEY = UNI,
// so the per-column rule sees no difference — but UNIQUE(email(10)) rejects two rows
// sharing a 10-character prefix that the source accepted. PASSES today.
// D3 (phase 030) makes it FATAL, because the prefix length is part of the uniqueness
// signature.
func TestCharacterizationUniquePrefixDestOnly_PassesToday(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, email varchar(64) NOT NULL, PRIMARY KEY (id), UNIQUE KEY uq_email (email)) ENGINE=InnoDB")
	f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, email varchar(64) NOT NULL, PRIMARY KEY (id), UNIQUE KEY uq_email (email(10))) ENGINE=InnoDB")

	checker := f.Checker(t, graph.NewGraph("orders", "id"))
	checker.SetVerification(chrCountVerification())

	chrAssertPasses(t, chrRun(t, ctx, checker, chrCommands[0], false))
}

// GAP 3 — TestCharacterizationUniqueFunctionalDestOnly_PassesToday.
// Destination-only functional UNIQUE ((lower(email))). A functional key part indexes an
// expression, not a column, so COLUMN_KEY for `email` stays empty. The hidden virtual
// column MySQL materialises for the expression is also absent from
// information_schema.COLUMNS (verified on 8.4.4-4: both sides report exactly two
// columns), so the column-count guard does not catch it either. PASSES today.
// D3 (phase 030) makes it FATAL under the conservative functional rule.
func TestCharacterizationUniqueFunctionalDestOnly_PassesToday(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, email varchar(64) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
	f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, email varchar(64) NOT NULL, PRIMARY KEY (id), UNIQUE KEY uq_lower_email ((lower(email)))) ENGINE=InnoDB")

	checker := f.Checker(t, graph.NewGraph("orders", "id"))
	checker.SetVerification(chrCountVerification())

	chrAssertPasses(t, chrRun(t, ctx, checker, chrCommands[0], false))
}

// GAP 4 — TestCharacterizationUniqueCollationLoosened_WarnsOnlyToday.
// Source UNIQUE(email) under utf8mb4_bin; destination UNIQUE(email) under
// utf8mb4_0900_ai_ci. Both report COLUMN_KEY = UNI so the unique rule is satisfied, the
// charsets are identical so the strict charset rule is satisfied, and the collation
// difference is only an ADVISORY warning — yet the destination index collides rows the
// source held as distinct ('A' vs 'a') and INSERT IGNORE silently skips one.
// PASSES WITH A WARNING today. D3 (phase 030) makes it FATAL regardless of verification
// mode, because collation is part of the uniqueness signature.
func TestCharacterizationUniqueCollationLoosened_WarnsOnlyToday(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, email varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, PRIMARY KEY (id), UNIQUE KEY uq_email (email)) ENGINE=InnoDB")
	f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, email varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL, PRIMARY KEY (id), UNIQUE KEY uq_email (email)) ENGINE=InnoDB")

	checker := f.Checker(t, graph.NewGraph("orders", "id"))
	checker.SetVerification(chrCountVerification())

	chrAssertPasses(t, chrRun(t, ctx, checker, chrCommands[0], false))

	if !chrAnyContains(f.WarnMessages(), "collation differs") {
		t.Fatalf("expected a collation advisory warning, got: %v", f.WarnMessages())
	}
}

// TestCharacterizationUniqueRenamedEquivalent_PassesToday pins a case D3 must NOT
// regress: source and destination both have UNIQUE(email), differing only in INDEX
// NAME. This passes today and must still pass after D3 — the signature ignores the
// index name (spec §3.3).
func TestCharacterizationUniqueRenamedEquivalent_PassesToday(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, email varchar(64) NOT NULL, PRIMARY KEY (id), UNIQUE KEY uq_email_src (email)) ENGINE=InnoDB")
	f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, email varchar(64) NOT NULL, PRIMARY KEY (id), UNIQUE KEY uq_email_dst (email)) ENGINE=InnoDB")

	checker := f.Checker(t, graph.NewGraph("orders", "id"))
	checker.SetVerification(chrCountVerification())

	chrAssertPasses(t, chrRun(t, ctx, checker, chrCommands[0], false))
}

// TestCharacterizationUniqueSourceOnly_PassesToday pins the other direction that D3
// must NOT regress: a SOURCE-only unique index is fine — the destination is looser,
// which is always allowed.
func TestCharacterizationUniqueSourceOnly_PassesToday(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, email varchar(64) NOT NULL, PRIMARY KEY (id), UNIQUE KEY uq_email (email)) ENGINE=InnoDB")
	f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, email varchar(64) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")

	checker := f.Checker(t, graph.NewGraph("orders", "id"))
	checker.SetVerification(chrCountVerification())

	chrAssertPasses(t, chrRun(t, ctx, checker, chrCommands[0], false))
}
