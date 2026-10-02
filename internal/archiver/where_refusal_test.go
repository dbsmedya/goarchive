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

// TestWhereSitesRefuseEmptyWhere pins that every site embedding the job's
// `where` refuses an empty or whitespace-only one before sending any statement.
// Config validation refuses such a job, so reaching a site with one is a broken
// invariant; selecting every row in that case would fail open.
func TestWhereSitesRefuseEmptyWhere(t *testing.T) {
	const wantErr = "where is empty"

	// recordingDB returns a mock whose matcher accepts every query and records
	// its SQL text, so each subtest counts the statements its site sent.
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
	cfg := &config.Config{Processing: config.ProcessingConfig{BatchSize: 10}}
	meta := types.ColumnMetadata{Names: []string{"id"}, Temporal: map[string]types.TemporalKind{}}
	ctx := context.Background()

	// Each site reads the source database; the payload check alone also has a
	// destination, which gets no expectation.
	sites := []struct {
		name string
		call string
		run  func(src, dest *sql.DB, where string) error
	}{
		{"root_fetch", "FetchNextBatch", func(src, _ *sql.DB, where string) error {
			_, err := NewRootIDFetcher(src, "t", "id", where, 10, nil).FetchNextBatch(ctx)
			return err
		}},
		{"progress_count", "CountRemaining", func(src, _ *sql.DB, where string) error {
			_, err := NewRootIDFetcher(src, "t", "id", where, 10, nil).CountRemaining(ctx)
			return err
		}},
		{"dryrun_root_count", "estimateRootCount", func(src, _ *sql.DB, where string) error {
			jobCfg := &config.JobConfig{RootTable: "t", PrimaryKey: "id", Where: where}
			_, err := NewEstimator(src, cfg, jobCfg, g, nil).estimateRootCount(ctx)
			return err
		}},
		{"dryrun_child_count", "estimateChildCount", func(src, _ *sql.DB, where string) error {
			jobCfg := &config.JobConfig{RootTable: "t", PrimaryKey: "id", Where: where}
			_, err := NewEstimator(src, cfg, jobCfg, g, nil).estimateChildCount(ctx, "c")
			return err
		}},
		{"payload_root_sample", "measureSample", func(src, dest *sql.DB, where string) error {
			jobCfg := &config.JobConfig{RootTable: "t", PrimaryKey: "id", Where: where}
			p := NewPayloadValidator(src, dest, g, "src", jobCfg, config.SafetyConfig{}, 10, config.VerificationConfig{}, nil)
			_, _, err := p.measureSample(ctx, "t", meta)
			return err
		}},
	}
	values := []struct{ name, where string }{{"empty", ""}, {"whitespace", " \t\n"}}

	for _, site := range sites {
		t.Run(site.name, func(t *testing.T) {
			for _, v := range values {
				t.Run(v.name, func(t *testing.T) {
					src, srcMock, srcSeen := recordingDB(t)
					dest, _, destSeen := recordingDB(t)
					srcMock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))

					err := site.run(src, dest, v.where)
					sent := len(*srcSeen) + len(*destSeen)
					if err == nil || !strings.Contains(err.Error(), wantErr) || sent != 0 {
						t.Errorf("%s(where=%q): err = %v, statements sent = %d; want an error containing %q and 0 statements",
							site.call, v.where, err, sent, wantErr)
					}
				})
			}
		})
	}

	t.Run("payload_child_sample_unfiltered", func(t *testing.T) {
		src, srcMock, srcSeen := recordingDB(t)
		dest, _, _ := recordingDB(t)
		srcMock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))

		jobCfg := &config.JobConfig{RootTable: "t", PrimaryKey: "id", Where: "id > 0"}
		p := NewPayloadValidator(src, dest, g, "src", jobCfg, config.SafetyConfig{}, 10, config.VerificationConfig{}, nil)
		// The sample's later destination steps fail on the bare mock; only the
		// source statement is under test.
		_, _, _ = p.measureSample(ctx, "c", meta)

		const want = "SELECT `id` FROM `c` LIMIT 10"
		if len(*srcSeen) == 0 || (*srcSeen)[0] != want {
			t.Errorf("child sample statements = %q; want first %q", *srcSeen, want)
		}
	})
}
