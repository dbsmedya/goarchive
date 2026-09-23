package archiver

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/types"
)

// TestWhereSitesWrapPredicate pins that every site embedding the job's `where`
// wraps it the same way, as `WHERE (<where>)`, so dry-run and the run parse the
// operator's text identically. The where carries an OR: unwrapped, it would bind
// looser than any predicate appended after it.
func TestWhereSitesWrapPredicate(t *testing.T) {
	const w = "status = 'x' OR status = 'y'"
	want := "WHERE (" + w + ")"

	// recordingDB returns a mock whose matcher accepts every query and records
	// its SQL text, so each subtest reads back the query its site issued.
	recordingDB := func(t *testing.T) (*sql.DB, sqlmock.Sqlmock, *[]string) {
		t.Helper()
		var seen []string
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(
			func(_, actual string) error {
				seen = append(seen, actual)
				return nil
			})))
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db, mock, &seen
	}
	check := func(t *testing.T, site string, queries []string) {
		t.Helper()
		if len(queries) != 1 {
			t.Fatalf("%s issued %d queries, want 1: %q", site, len(queries), queries)
		}
		if strings.Count(queries[0], want) != 1 {
			t.Errorf("%s query = %s; want %s", site, queries[0], want)
		}
	}

	g, err := graph.NewBuilder(&config.JobConfig{
		RootTable:  "t",
		PrimaryKey: "id",
		Relations: []config.Relation{
			{Table: "c", PrimaryKey: "id", ForeignKey: "t_id", DependencyType: "1-N"},
		},
	}).Build()
	if err != nil {
		t.Fatalf("build graph: %v", err)
	}
	jobCfg := &config.JobConfig{RootTable: "t", PrimaryKey: "id", Where: w}
	cfg := &config.Config{Processing: config.ProcessingConfig{BatchSize: 10}}
	ctx := context.Background()

	t.Run("root_fetch", func(t *testing.T) {
		db, mock, seen := recordingDB(t)
		mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
		if _, err := NewRootIDFetcher(db, "t", "id", w, 10, nil).FetchNextBatch(ctx); err != nil {
			t.Fatalf("FetchNextBatch: %v", err)
		}
		check(t, "root fetch", *seen)
	})

	for _, tc := range []struct {
		name       string
		checkpoint interface{}
	}{{"progress_count_nil_checkpoint", nil}, {"progress_count_checkpoint", int64(5)}} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, seen := recordingDB(t)
			mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
			if _, err := NewRootIDFetcher(db, "t", "id", w, 10, tc.checkpoint).CountRemaining(ctx); err != nil {
				t.Fatalf("CountRemaining: %v", err)
			}
			check(t, "progress count", *seen)
		})
	}

	t.Run("dryrun_root_count", func(t *testing.T) {
		db, mock, seen := recordingDB(t)
		mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
		if _, err := NewEstimator(db, cfg, jobCfg, g, nil).estimateRootCount(ctx); err != nil {
			t.Fatalf("estimateRootCount: %v", err)
		}
		check(t, "dry-run root count", *seen)
	})

	t.Run("dryrun_child_count", func(t *testing.T) {
		db, mock, seen := recordingDB(t)
		mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
		if _, err := NewEstimator(db, cfg, jobCfg, g, nil).estimateChildCount(ctx, "c"); err != nil {
			t.Fatalf("estimateChildCount: %v", err)
		}
		check(t, "dry-run child count", *seen)
	})

	t.Run("payload_sample", func(t *testing.T) {
		q, err := buildSampleQuery("t", "id", types.ColumnMetadata{Names: []string{"id"}, Temporal: map[string]types.TemporalKind{}}, w, 10)
		if err != nil {
			t.Fatalf("buildSampleQuery: %v", err)
		}
		check(t, "payload sample", []string{q})
	})
}
