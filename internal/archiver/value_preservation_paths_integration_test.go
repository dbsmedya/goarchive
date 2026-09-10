//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/internal/archiver/testsupport"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
)

func vpMarkerCount(t *testing.T, db *sql.DB, name string, status LogStatus) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow("SELECT id FROM archiver_job WHERE job_name=?", name).Scan(&id); err != nil {
		t.Fatal(err)
	}
	var n int64
	if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM archiver_job_log_%d WHERE log_status=?", id), status).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func TestTemporalDirtyDestinationSHA256_Integration(t *testing.T) {
	cfg, mgr := vpFixture(t)
	cfg.Verification.Method = "sha256"
	const table, name = "vp_dirty", "vp_107_dirty"
	vpTable(t, mgr, table, "(id BIGINT PRIMARY KEY,d DATE) ENGINE=InnoDB")
	vpExec(t, mgr.Source, "INSERT INTO vp_dirty VALUES(1,'2020-03-02')")
	vpSeed(t, cfg.Destination, "ALLOW_INVALID_DATES", "INSERT INTO vp_dirty VALUES(1,'2020-02-31')")
	var before string
	if err := mgr.Destination.QueryRow("SELECT CAST(d AS CHAR) FROM vp_dirty WHERE id=1").Scan(&before); err != nil || before != "2020-02-31" {
		t.Fatalf("dirty fixture=%q err=%v", before, err)
	}
	job := &config.JobConfig{RootTable: table, PrimaryKey: "id", Where: "id=1"}
	orch, err := NewOrchestrator(cfg, name, job, mgr)
	if err != nil {
		t.Fatal(err)
	}
	if err = orch.Initialize(); err != nil {
		t.Fatal(err)
	}
	testsupport.CleanupArchiverState(t, mgr.Destination, name)
	_, runErr := orch.Execute(context.Background(), nil)
	var n int64
	var raw string
	if err := mgr.Source.QueryRow("SELECT COUNT(*) FROM vp_dirty").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Destination.QueryRow("SELECT CAST(d AS CHAR) FROM vp_dirty WHERE id=1").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	copied, completed := vpMarkerCount(t, mgr.Destination, name, LogStatusCopied), vpMarkerCount(t, mgr.Destination, name, LogStatusCompleted)
	t.Logf("source=%d raw=%s copied=%d completed=%d run=%v", n, raw, copied, completed, runErr)
	if runErr == nil || !strings.Contains(runErr.Error(), "verification failed: 1 tables had mismatches") || n != 1 || raw != before || copied != 0 || completed != 0 {
		t.Fatalf("dirty destination authorized deletion: expected SHA256 mismatch and retained source: %v", runErr)
	}
}

func vpValidKey(t *testing.T, kind, key string) (*config.Config, *database.Manager, *config.JobConfig) {
	t.Helper()
	cfg, mgr := vpFixture(t)
	vpTable(t, mgr, "vp_valid_roots", "(id BIGINT PRIMARY KEY) ENGINE=InnoDB")
	vpTable(t, mgr, "vp_valid_keys", fmt.Sprintf("(d %s PRIMARY KEY,root_id BIGINT NOT NULL,KEY(root_id),FOREIGN KEY(root_id) REFERENCES vp_valid_roots(id)) ENGINE=InnoDB", kind))
	vpTable(t, mgr, "vp_valid_leaves", fmt.Sprintf("(id BIGINT PRIMARY KEY,parent_d %s NOT NULL,KEY(parent_d),FOREIGN KEY(parent_d) REFERENCES vp_valid_keys(d)) ENGINE=InnoDB", kind))
	vpExec(t, mgr.Source, "INSERT INTO vp_valid_roots VALUES(1)")
	vpExec(t, mgr.Source, "INSERT INTO vp_valid_keys VALUES(?,1)", key)
	vpExec(t, mgr.Source, "INSERT INTO vp_valid_leaves VALUES(11,?)", key)
	var raw string
	if err := mgr.Source.QueryRow("SELECT CAST(d AS CHAR) FROM vp_valid_keys").Scan(&raw); err != nil || raw != key {
		t.Fatalf("valid fixture raw=%q err=%v", raw, err)
	}
	t.Logf("fixture kind=%s key=%s", kind, raw)
	return cfg, mgr, &config.JobConfig{RootTable: "vp_valid_roots", PrimaryKey: "id", Where: "id=1", Relations: []config.Relation{{Table: "vp_valid_keys", PrimaryKey: "d", ForeignKey: "root_id", DependencyType: "1-N", Relations: []config.Relation{{Table: "vp_valid_leaves", PrimaryKey: "id", ForeignKey: "parent_d", DependencyType: "1-N"}}}}}
}
func TestTemporalValidKeyArchive_Integration(t *testing.T) {
	for _, tc := range []struct{ kind, key string }{{"DATE", "2020-02-29"}, {"DATETIME(6)", "2020-02-29 12:34:56.123456"}} {
		for _, method := range []string{"count", "sha256"} {
			t.Run(tc.kind+"/"+method, func(t *testing.T) {
				cfg, mgr, job := vpValidKey(t, tc.kind, tc.key)
				cfg.Verification.Method = method
				const name = "vp_107_valid_archive"
				orch, err := NewOrchestrator(cfg, name, job, mgr)
				if err != nil {
					t.Fatal(err)
				}
				if err = orch.Initialize(); err != nil {
					t.Fatal(err)
				}
				testsupport.CleanupArchiverState(t, mgr.Destination, name)
				_, runErr := orch.Execute(context.Background(), nil)
				for _, table := range []string{"vp_valid_roots", "vp_valid_keys", "vp_valid_leaves"} {
					var n int
					if err := mgr.Source.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
						t.Fatal(err)
					}
					t.Logf("source %s count=%d run=%v", table, n, runErr)
					if n != 0 {
						t.Errorf("valid temporal archive refused or changed key: %s source=%d run=%v", table, n, runErr)
					}
				}
				for _, q := range []string{"SELECT CAST(d AS CHAR) FROM vp_valid_keys", "SELECT CAST(parent_d AS CHAR) FROM vp_valid_leaves"} {
					var raw string
					err := mgr.Destination.QueryRow(q).Scan(&raw)
					t.Logf("destination raw=%q read=%v", raw, err)
					if err != nil || raw != tc.key {
						t.Errorf("valid temporal archive refused or changed key: raw=%q err=%v", raw, err)
					}
				}
				if runErr != nil || vpMarkerCount(t, mgr.Destination, name, LogStatusCompleted) != 1 {
					t.Errorf("valid temporal archive refused or changed key: run=%v", runErr)
				}
			})
		}
	}
}
func TestTemporalValidKeyPurge_Integration(t *testing.T) {
	for _, tc := range []struct{ kind, key string }{{"DATE", "2020-02-29"}, {"DATETIME(6)", "2020-02-29 12:34:56.123456"}} {
		t.Run(tc.kind, func(t *testing.T) {
			cfg, mgr, job := vpValidKey(t, tc.kind, tc.key)
			const name = "vp_107_valid_purge"
			before := map[string]int{}
			for _, table := range []string{"vp_valid_roots", "vp_valid_keys", "vp_valid_leaves"} {
				var n int
				if err := mgr.Destination.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
					t.Fatal(err)
				}
				before[table] = n
			}
			orch, err := NewPurgeOrchestrator(cfg, name, job, mgr)
			if err != nil {
				t.Fatal(err)
			}
			if err = orch.Initialize(); err != nil {
				t.Fatal(err)
			}
			testsupport.CleanupArchiverState(t, mgr.Destination, name)
			_, runErr := orch.Execute(context.Background())
			for table, want := range before {
				var src, dst int
				if err := mgr.Source.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&src); err != nil {
					t.Fatal(err)
				}
				if err := mgr.Destination.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&dst); err != nil {
					t.Fatal(err)
				}
				t.Logf("%s source=%d destination=%d run=%v", table, src, dst, runErr)
				if src != 0 || dst != want {
					t.Errorf("valid temporal purge refused or incomplete: %s %d/%d want 0/%d", table, src, dst, want)
				}
			}
			if runErr != nil {
				t.Errorf("valid temporal purge refused or incomplete: %v", runErr)
			}
		})
	}
}
