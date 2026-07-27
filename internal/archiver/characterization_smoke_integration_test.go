//go:build integration

package archiver

import (
	"testing"

	"github.com/dbsmedya/goarchive/internal/graph"
)

// TestCharacterizationHarness_CleanFixturePassesEveryCommand is the harness's own
// acceptance test: a minimal, entirely valid two-table graph must pass preflight for
// all five commands. If this fails, the harness is broken, not the engine.
func TestCharacterizationHarness_CleanFixturePassesEveryCommand(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	const parentDDL = "CREATE TABLE parent (id bigint NOT NULL, name varchar(64) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB"
	const childDDL = "CREATE TABLE child (id bigint NOT NULL, parent_id bigint NOT NULL, PRIMARY KEY (id), KEY idx_parent (parent_id), CONSTRAINT fk_child_parent FOREIGN KEY (parent_id) REFERENCES parent (id)) ENGINE=InnoDB"

	f.ExecSource(t, ctx, parentDDL)
	f.ExecSource(t, ctx, childDDL)
	f.ExecDest(t, ctx, parentDDL)
	f.ExecDest(t, ctx, childDDL)

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			g := graph.NewGraph("parent", "id")
			chrAddChild(g, "parent", "id", "child", "parent_id", "id")

			chrAssertPasses(t, chrRun(t, ctx, f.Checker(t, g), cmd, false))
		})
	}
}

// TestCharacterizationHarness_AssertionHelpers proves chrTableNames strips validator
// decoration but preserves dotted table.column entries.
func TestCharacterizationHarness_AssertionHelpers(t *testing.T) {
	err := &PreflightError{
		Check:  "EXAMPLE",
		Tables: []string{"zeta(3-column PRIMARY KEY)", "alpha.col_a", "beta(position 2: x)"},
	}
	got := chrTableNames(err)
	want := []string{"alpha.col_a", "beta", "zeta"}
	if len(got) != len(want) {
		t.Fatalf("chrTableNames = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("chrTableNames = %v, want %v", got, want)
		}
	}
}
