package preflight

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
)

func TestProfileFor(t *testing.T) {
	for typ, want := range map[jobspec.Type]Profile{
		jobspec.Archive: ProfileFull, jobspec.Purge: ProfileSourceOnly, jobspec.CopyOnly: ProfileNonDestructive,
	} {
		if got := ProfileFor(typ); got != want {
			t.Errorf("ProfileFor(%s) = %d, want %d", typ, got, want)
		}
	}
}

func enabledStages(cfg Config) []string {
	var out []string
	for _, st := range stages(cfg) {
		if st.enabled {
			out = append(out, st.name)
		}
	}
	return out
}

// TestStagesByJobType pins each job type's checks and their order (spec §8.1). A purge job
// has no destination-table checks but keeps the job schema permission check (R5).
func TestStagesByJobType(t *testing.T) {
	structure := []string{"TablesExist", "Keys", "PrimaryKeyColumns", "StorageEngine", "RelationKeys", "ForeignKeyIndexes", "JobSchemaPermissions"}
	destination := []string{"DestinationTablesExist", "DestinationStorageEngine", "DestinationKeys", "DestinationSchemaCompatibility",
		"DestinationWritePermissions", "DestinationInsertTriggerVisibility", "DestinationInsertTriggers"}
	join := func(parts ...[]string) []string { return slices.Concat(parts...) }
	for _, tc := range []struct {
		typ  jobspec.Type
		skip bool
		want []string
	}{
		{jobspec.Archive, false, join(structure, destination, []string{"ForeignKeyMetadataVisibility", "ForeignKeyCoverage",
			"InternalForeignKeys", "SelfForeignKeys", "SourceSelectPermissions", "SourceDeletePermissions", "DeleteTriggerVisibility", "DeleteTriggers", "CascadeRules", "StatementCaps", "MaxAllowedPacket"})},
		{jobspec.Purge, false, join(structure, []string{"ForeignKeyMetadataVisibility", "ForeignKeyCoverage",
			"InternalForeignKeys", "SelfForeignKeys", "SourceSelectPermissions", "SourceDeletePermissions", "DeleteTriggerVisibility", "DeleteTriggers", "CascadeRules", "StatementCaps",
			"MaxAllowedPacket"})},
		{jobspec.CopyOnly, false, join(structure, destination, []string{"ForeignKeyCoverage", "InternalForeignKeys",
			"SourceSelectPermissions", "StatementCaps", "MaxAllowedPacket"})},
		// Under --skip-dangerously-validate-preflight-allow-experimental the keys are still read, the destination's too:
		// the destination statement caps are computed from them (spec §8.3).
		{jobspec.Archive, true, []string{"Keys", "DestinationKeyRead", "StatementCaps"}},
		{jobspec.CopyOnly, true, []string{"Keys", "DestinationKeyRead", "StatementCaps"}},
		{jobspec.Purge, true, []string{"Keys", "StatementCaps"}},
	} {
		got := enabledStages(Config{Spec: &jobspec.Spec{Type: tc.typ}, SkipPolicy: tc.skip})
		if !slices.Equal(got, tc.want) {
			t.Errorf("%s skip=%v stages:\n got %v\nwant %v", tc.typ, tc.skip, got, tc.want)
		}
	}
}

// TestHealthyJobPasses is the baseline every other test breaks in one way: each job type
// passes, and the result holds MySQL's keys in key order.
func TestHealthyJobPasses(t *testing.T) {
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
		t.Run(string(typ), func(t *testing.T) {
			res := healthy(t, typ).mustPass()
			if got := res.Keys["order_lines"].Names(); !slices.Equal(got, []string{"order_id", "line_no"}) {
				t.Errorf("order_lines key = %v", got)
			}
			if c := res.Keys["orders"].Columns[0]; c.Family != key.FamilyUnsigned {
				t.Errorf("orders.id family = %v, want unsigned", c.Family)
			}
			if len(res.Keys) != 3 {
				t.Errorf("keys = %v, want 3 tables", res.Keys)
			}
		})
	}
}

// TestPurgeNeedsNoDestinationTables is R5: validate and dry-run use the job's profile, so a
// purge job passes with no tables on the destination and reads none of its table facts.
// It reads the job schema privileges and two server variables, which bound the tracking
// log's statements there (M7c F3).
func TestPurgeNeedsNoDestinationTables(t *testing.T) {
	w := healthy(t, jobspec.Purge)
	w.dst.specs = nil
	w.mustPass()
	for _, c := range w.dst.calls {
		if c != "Privileges" && c != "RangeOptimizerMaxMemSize" && c != "MaxAllowedPacket" {
			t.Errorf("purge read destination fact %s; only the job schema privileges and server variables are needed", c)
		}
	}
	if !slices.Contains(w.dst.calls, "Privileges") {
		t.Error("purge did not check the job schema privileges")
	}
}

// TestCopyOnlySkipsDeleteChecks: copy-only deletes nothing, so delete privileges, DELETE
// triggers, cascade rules and the visibility proof do not apply.
func TestCopyOnlySkipsDeleteChecks(t *testing.T) {
	w := healthy(t, jobspec.CopyOnly)
	w.src.lacking = map[validations.Privilege][]string{validations.PrivilegeDelete: {"orders"}}
	w.src.triggers = []validations.TriggerInfo{{Table: "orders", Name: "trg", Event: "DELETE", Timing: "BEFORE"}}
	w.src.partial = true
	w.mustPass()

	w.jobType = jobspec.Archive
	w.refused("FK_COVERAGE_VISIBILITY_CHECK")
}

func TestRunRequiresTheDestination(t *testing.T) {
	_, err := Run(context.Background(), Config{Source: nil, SourceDatabase: "src"})
	if err == nil || !strings.Contains(err.Error(), "source") {
		t.Errorf("Run without a source = %v", err)
	}
	w := healthy(t, jobspec.Purge)
	cfg := w.cfg
	cfg.Spec = w.spec()
	cfg.Source = nil
	if _, err := Run(context.Background(), cfg); err == nil {
		t.Error("Run without connections passed")
	}
}

// TestFactsAreReadOnce: each fact is read once per run, however many stages use it.
func TestFactsAreReadOnce(t *testing.T) {
	w := healthy(t, jobspec.Archive)
	w.mustPass()
	counts := map[string]int{}
	for _, c := range w.src.calls {
		counts[c]++
	}
	want := map[string]int{"Tables": 1, "PrimaryKeys": 1, "TableSpec": 3, "ForeignKeys": 3, "Privileges": 1,
		"TriggerProof": 1, "RangeOptimizerMaxMemSize": 1}
	for k, n := range want {
		if counts[k] != n {
			t.Errorf("source %s read %d times, want %d (calls %v)", k, counts[k], n, w.src.calls)
		}
	}
	dst := map[string]int{}
	for _, c := range w.dst.calls {
		dst[c]++
	}
	if dst["TriggerProof"] != 1 {
		t.Errorf("destination TriggerProof read %d times, want 1 (calls %v)", dst["TriggerProof"], w.dst.calls)
	}
}
