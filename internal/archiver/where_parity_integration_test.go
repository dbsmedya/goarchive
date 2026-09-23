//go:build integration

package archiver

import (
	"context"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/graph"
)

// TestDryRunAndRunAcceptSameWhere_Integration pins that dry-run and the run
// embed the job's `where` the same way: for each text, dry-run's first step
// (the estimate) and the run's root fetch both accept it, or both reject it
// with the same MySQL syntax error. A where the run would reject therefore
// fails in dry-run, before any count is shown.
func TestDryRunAndRunAcceptSameWhere_Integration(t *testing.T) {
	ctx := context.Background()
	f := newChrFixture(t, ctx)
	f.ExecSource(t, ctx, "CREATE TABLE parity_root (id BIGINT NOT NULL PRIMARY KEY, s VARCHAR(10) NOT NULL) ENGINE=InnoDB")
	f.ExecSource(t, ctx, "INSERT INTO parity_root (id, s) VALUES (1, 'a'), (2, 'b;c'), (3, 'c')")
	cfg, mgr := estateManager(t, ctx, f)
	g := graph.NewGraph("parity_root", "id")

	for _, tc := range []struct{ name, where string }{
		{"plain", "id < 100"},
		{"trailing_semicolon", "id < 100;"},
		{"dash_comment", "id < 100 -- note"},
		{"hash_comment", "id < 100 # note"},
		{"order_by", "id < 100 ORDER BY id"},
		{"closes_parenthesis", "id < 100) OR (1=1"},
		{"block_comment", "id < 100 /* note */"},
		{"dash_comment_newline", "id < 100 -- note\n"},
		{"or", "s = 'a' OR s = 'b'"},
		{"quoted_semicolon", "s = 'b;c'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			jobCfg := &config.JobConfig{RootTable: "parity_root", PrimaryKey: "id", Where: tc.where}
			est, estErr := NewEstimator(mgr.Source, cfg, jobCfg, g, nil).Estimate(ctx)
			ids, fetchErr := NewRootIDFetcher(mgr.Source, "parity_root", "id", tc.where, 100, nil).FetchNextBatch(ctx)

			switch {
			case estErr == nil && fetchErr != nil:
				t.Fatalf("dry-run estimate accepted (root count %d) but the run's fetch failed: %v; want the same outcome", est.RootCount, fetchErr)
			case estErr != nil && fetchErr == nil:
				t.Fatalf("dry-run estimate failed (%v) but the run's fetch accepted %d ids; want the same outcome", estErr, len(ids))
			case estErr != nil:
				if !isMySQLError(estErr, 1064) || !isMySQLError(fetchErr, 1064) {
					t.Errorf("both rejected, estimate: %v, fetch: %v; want MySQL error 1064 from each", estErr, fetchErr)
				}
				if !strings.Contains(estErr.Error(), "failed to estimate root count") {
					t.Errorf("estimate error = %v; want the root-count error (no count computed)", estErr)
				}
			default:
				if est.RootCount != int64(len(ids)) {
					t.Errorf("dry-run root count = %d, the run's fetch selected %d ids; want equal", est.RootCount, len(ids))
				}
			}
		})
	}
}
