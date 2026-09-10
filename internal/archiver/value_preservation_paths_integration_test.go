//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/dbsmedya/goarchive/internal/graph"
	"os"
	"reflect"
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

func vpRawRows(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var values []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		values = append(values, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return values
}
func TestTemporalPayloadRawRoundTrip_Integration(t *testing.T) {
	vpRequireProfile(t, "raw")
	for _, method := range []string{"count", "sha256"} {
		t.Run(method, func(t *testing.T) {
			cfg, mgr := vpFixture(t)
			vpAssertProfileFacts(t, mgr.Destination)
			cfg.Verification.Method = method
			const table, name = "vp_raw", "vp_107_raw"
			vpTable(t, mgr, table, "(id BIGINT PRIMARY KEY,d DATE,dt DATETIME(6),dt0 DATETIME) ENGINE=InnoDB")
			vpSeed(t, cfg.Source, "ALLOW_INVALID_DATES", "INSERT INTO vp_raw VALUES(1,'2020-02-31','2020-02-31 12:00:00.123456','2020-02-31 12:00:00'),(2,'2020-00-15','2020-00-15 12:00:00','2020-00-15 12:00:00'),(3,'2020-01-00','2020-01-00 12:00:00','2020-01-00 12:00:00'),(4,'0000-00-00','0000-00-00 00:00:00','0000-00-00 00:00:00'),(5,NULL,NULL,NULL),(6,'2020-02-29','2020-02-29 12:00:00.123456','2020-02-29 12:00:00'),(7,'0000-03-02','0000-03-02 12:00:00','0000-03-02 12:00:00')")
			query := "SELECT CONCAT(id,'/',COALESCE(CAST(d AS CHAR),'NULL'),'/',COALESCE(CAST(dt AS CHAR),'NULL'),'/',COALESCE(CAST(dt0 AS CHAR),'NULL')) FROM vp_raw ORDER BY id"
			before := vpRawRows(t, mgr.Source, query)
			t.Logf("raw source=%v", before)
			if len(before) != 7 || !strings.Contains(before[0], "2020-02-31") {
				t.Fatal("raw fixture missing")
			}
			orch, err := NewOrchestrator(cfg, name, &config.JobConfig{RootTable: table, PrimaryKey: "id", Where: "id<=7"}, mgr)
			if err != nil {
				t.Fatal(err)
			}
			if err = orch.Initialize(); err != nil {
				t.Fatal(err)
			}
			testsupport.CleanupArchiverState(t, mgr.Destination, name)
			_, runErr := orch.Execute(context.Background(), nil)
			after := vpRawRows(t, mgr.Destination, query)
			remaining := vpRawRows(t, mgr.Source, query)
			t.Logf("raw destination=%v remaining=%v error=%v", after, remaining, runErr)
			if runErr != nil || len(remaining) != 0 || !reflect.DeepEqual(before, after) {
				t.Fatalf("raw temporal payload changed: before=%v after=%v error=%v", before, after, runErr)
			}
		})
	}
}
func TestTemporalTimestampUTC_Integration(t *testing.T) {
	vpRequireProfile(t, "raw")
	cfg, mgr := vpFixture(t)
	for _, db := range []*sql.DB{mgr.Source, mgr.Destination} {
		vpAssertProfileFacts(t, db)
		var zone string
		if err := db.QueryRow("SELECT @@GLOBAL.time_zone").Scan(&zone); err != nil || zone != "+03:00" {
			t.Fatalf("timestamp startup zone=%s err=%v", zone, err)
		}
	}
	vpTable(t, mgr, "vp_utc", "(id BIGINT PRIMARY KEY,ts TIMESTAMP(6),tm TIME(6),yr YEAR) ENGINE=InnoDB")
	vpExec(t, mgr.Source, "INSERT INTO vp_utc VALUES(1,'2020-02-29 12:34:56.123456','12:34:56.123456',2020)")
	q := "SELECT CONCAT(CAST(ts AS CHAR),'/',UNIX_TIMESTAMP(ts),'/',CAST(tm AS CHAR),'/',yr) FROM vp_utc"
	before := vpRawRows(t, mgr.Source, q)
	cfg.Verification.Method = "sha256"
	const name = "vp_107_utc"
	orch, err := NewOrchestrator(cfg, name, &config.JobConfig{RootTable: "vp_utc", PrimaryKey: "id", Where: "id=1"}, mgr)
	if err != nil {
		t.Fatal(err)
	}
	if err = orch.Initialize(); err != nil {
		t.Fatal(err)
	}
	testsupport.CleanupArchiverState(t, mgr.Destination, name)
	_, err = orch.Execute(context.Background(), nil)
	after := vpRawRows(t, mgr.Destination, q)
	t.Logf("timestamp before=%v after=%v error=%v", before, after, err)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal(before, after, err)
	}
}
func TestTemporalCopyOnlyAndSample_Integration(t *testing.T) {
	profile := os.Getenv("VP_MATRIX_PROFILE")
	if profile != "" && profile != "default" && profile != "raw" {
		t.Fatalf("unexpected sample profile %s", profile)
	}
	cases := []string{"default_refusal"}
	if profile == "raw" {
		cases = []string{"copy_only", "sample_empty", "sample_duplicate"}
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			cfg, mgr := vpFixture(t)
			vpAssertProfileFacts(t, mgr.Destination)
			const table = "vp_sample"
			vpTable(t, mgr, table, "(id BIGINT PRIMARY KEY,d DATE,dt DATETIME(6)) ENGINE=InnoDB")
			seed := "INSERT INTO vp_sample VALUES(1,'2020-02-31','2020-02-31 12:00:00.123456')"
			vpSeed(t, cfg.Source, "ALLOW_INVALID_DATES", seed)
			if name == "sample_duplicate" {
				vpSeed(t, cfg.Destination, "ALLOW_INVALID_DATES", seed)
			}
			q := "SELECT CONCAT(id,'/',CAST(d AS CHAR),'/',CAST(dt AS CHAR)) FROM vp_sample"
			srcBefore, dstBefore := vpRawRows(t, mgr.Source, q), vpRawRows(t, mgr.Destination, q)
			job := &config.JobConfig{RootTable: table, PrimaryKey: "id", Where: "id=1"}
			var runErr error
			if name == "copy_only" {
				cfg.Verification.Method = "sha256"
				orch, err := NewCopyOnlyOrchestrator(cfg, "vp_107_sample_copy", job, mgr)
				if err != nil {
					t.Fatal(err)
				}
				if err = orch.Initialize(); err != nil {
					t.Fatal(err)
				}
				testsupport.CleanupArchiverState(t, mgr.Destination, "vp_107_sample_copy")
				_, runErr = orch.Execute(context.Background(), false)
			} else {
				p := NewPayloadValidator(mgr.Source, mgr.Destination, graph.NewGraph(table, "id"), cfg.Source.Database, job, cfg.Safety, 100, cfg.Verification, nil)
				runErr = p.Validate(context.Background())
			}
			srcAfter, dstAfter := vpRawRows(t, mgr.Source, q), vpRawRows(t, mgr.Destination, q)
			t.Logf("sample=%s source=%v destination=%v error=%v", name, srcAfter, dstAfter, runErr)
			if !reflect.DeepEqual(srcBefore, srcAfter) {
				t.Fatal("sample/copy-only changed source")
			}
			if name == "copy_only" {
				if runErr != nil || !reflect.DeepEqual(srcBefore, dstAfter) {
					t.Fatal(runErr, dstAfter)
				}
			} else {
				if !reflect.DeepEqual(dstBefore, dstAfter) {
					t.Fatal("sample persisted rows")
				}
				if name == "default_refusal" {
					if runErr == nil || !strings.Contains(runErr.Error(), "INSERT_DIAGNOSTIC_REJECTED") {
						t.Fatal(runErr)
					}
				} else if runErr != nil {
					t.Fatal(runErr)
				}
			}
		})
	}
}
func TestValuePreservationPendingReplay_Integration(t *testing.T) {
	vpRequireProfile(t, "raw")
	cfg, mgr := vpFixture(t)
	vpAssertProfileFacts(t, mgr.Destination)
	cfg.Verification.Method = "sha256"
	const table, name = "vp_pending", "vp_107_pending"
	vpTable(t, mgr, table, "(id BIGINT PRIMARY KEY,d DATE) ENGINE=InnoDB")
	vpSeed(t, cfg.Source, "ALLOW_INVALID_DATES", "INSERT INTO vp_pending VALUES(1,'2020-02-31')")
	logTable := bootstrapJobTracking(t, mgr.Destination, cfg.Destination.EffectiveJobSchema(), name, table, "archive")
	seedLogStatus(t, mgr.Destination, logTable, LogStatusPending, "1")
	orch, err := NewOrchestrator(cfg, name, &config.JobConfig{RootTable: table, PrimaryKey: "id", Where: "id=1"}, mgr)
	if err != nil {
		t.Fatal(err)
	}
	if err = orch.Initialize(); err != nil {
		t.Fatal(err)
	}
	_, err = orch.Execute(context.Background(), nil)
	src := vpRawRows(t, mgr.Source, "SELECT CAST(d AS CHAR) FROM vp_pending")
	dst := vpRawRows(t, mgr.Destination, "SELECT CAST(d AS CHAR) FROM vp_pending")
	t.Logf("pending src=%v dst=%v error=%v", src, dst, err)
	if err != nil || len(src) != 0 || !reflect.DeepEqual(dst, []string{"2020-02-31"}) || vpMarkerCount(t, mgr.Destination, name, LogStatusCompleted) != 1 {
		t.Fatal(src, dst, err)
	}
}
func TestValuePreservationCopiedReplay_Integration(t *testing.T) {
	vpRequireProfile(t, "raw")
	for _, invalid := range []bool{false, true} {
		t.Run(fmt.Sprint(invalid), func(t *testing.T) {
			var cfg *config.Config
			var mgr *database.Manager
			var job *config.JobConfig
			if invalid {
				cfg, mgr, job = vpCollision(t)
			} else {
				cfg, mgr, job = vpValidKey(t, "DATE", "2020-02-29")
			}
			cfg.Verification.Method = "sha256"
			const name = "vp_107_copied"
			logTable := bootstrapJobTracking(t, mgr.Destination, cfg.Destination.EffectiveJobSchema(), name, job.RootTable, "archive")
			seedLogStatus(t, mgr.Destination, logTable, LogStatusCopied, "1")
			orch, err := NewOrchestrator(cfg, name, job, mgr)
			if err != nil {
				t.Fatal(err)
			}
			if err = orch.Initialize(); err != nil {
				t.Fatal(err)
			}
			_, err = orch.Execute(context.Background(), nil)
			if invalid {
				vpAssertCollisionRetained(t, mgr, err)
				return
			}
			var src, dst int
			if e := mgr.Source.QueryRow("SELECT COUNT(*) FROM vp_valid_roots").Scan(&src); e != nil {
				t.Fatal(e)
			}
			if e := mgr.Destination.QueryRow("SELECT COUNT(*) FROM vp_valid_roots").Scan(&dst); e != nil {
				t.Fatal(e)
			}
			t.Logf("copied replay source=%d destination=%d error=%v", src, dst, err)
			if err != nil || src != 0 || dst != 0 || vpMarkerCount(t, mgr.Destination, name, LogStatusCompleted) != 1 {
				t.Fatal(src, dst, err)
			}
		})
	}
}
func TestValuePreservationBatchPromotion_Integration(t *testing.T) {
	vpRequireProfile(t, "raw")
	cfg, mgr, job := vpCollision(t)
	cfg.Verification.Method = "sha256"
	const name = "vp_107_promote"
	logTable := bootstrapJobTracking(t, mgr.Destination, cfg.Destination.EffectiveJobSchema(), name, job.RootTable, "copy-only")
	seedLogStatus(t, mgr.Destination, logTable, LogStatusCopied, "1")
	orch, err := NewCopyOnlyOrchestrator(cfg, name, job, mgr)
	if err != nil {
		t.Fatal(err)
	}
	if err = orch.Initialize(); err != nil {
		t.Fatal(err)
	}
	_, err = orch.Execute(context.Background(), false)
	rows := vpRawRows(t, mgr.Source, "SELECT CONCAT(CAST(d AS CHAR),'/',x) FROM vp_keys ORDER BY d")
	t.Logf("promotion rows=%v error=%v", rows, err)
	if err != nil || !reflect.DeepEqual(rows, []string{"2020-02-31/1", "2020-03-02/2"}) || vpMarkerCount(t, mgr.Destination, name, LogStatusCompleted) != 1 {
		t.Fatal(rows, err)
	}
	var dst int
	if err := mgr.Destination.QueryRow("SELECT COUNT(*) FROM vp_roots").Scan(&dst); err != nil || dst != 0 {
		t.Fatal(dst, err)
	}
}
func TestDiagnosticLateTableFailure_Integration(t *testing.T) {
	cfg, mgr := vpFixture(t)
	vpAssertProfileFacts(t, mgr.Destination)
	cfg.Verification.Method = "sha256"
	vpTable(t, mgr, "vp_late_root", "(id BIGINT PRIMARY KEY) ENGINE=InnoDB")
	vpTable(t, mgr, "vp_late_child", "(id BIGINT PRIMARY KEY,root_id BIGINT NOT NULL,d DATE,KEY(root_id),FOREIGN KEY(root_id) REFERENCES vp_late_root(id)) ENGINE=InnoDB")
	vpExec(t, mgr.Source, "INSERT INTO vp_late_root VALUES(1)")
	vpSeed(t, cfg.Source, "ALLOW_INVALID_DATES", "INSERT INTO vp_late_child VALUES(2,1,'2020-02-31')")
	job := &config.JobConfig{RootTable: "vp_late_root", PrimaryKey: "id", Where: "id=1", Relations: []config.Relation{{Table: "vp_late_child", PrimaryKey: "id", ForeignKey: "root_id"}}}
	const name = "vp_107_late"
	orch, err := NewOrchestrator(cfg, name, job, mgr)
	if err != nil {
		t.Fatal(err)
	}
	if err = orch.Initialize(); err != nil {
		t.Fatal(err)
	}
	testsupport.CleanupArchiverState(t, mgr.Destination, name)
	_, runErr := orch.Execute(context.Background(), nil)
	for _, table := range []string{"vp_late_root", "vp_late_child"} {
		var src, dst int
		if err := mgr.Source.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&src); err != nil {
			t.Fatal(err)
		}
		if err := mgr.Destination.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&dst); err != nil {
			t.Fatal(err)
		}
		t.Logf("late table=%s source=%d destination=%d error=%v", table, src, dst, runErr)
		if src != 1 || dst != 0 {
			t.Fatal(src, dst)
		}
	}
	if runErr == nil || !strings.Contains(runErr.Error(), "INSERT_DIAGNOSTIC_REJECTED") || !strings.Contains(runErr.Error(), "1264") || vpMarkerCount(t, mgr.Destination, name, LogStatusCopied) != 0 || vpMarkerCount(t, mgr.Destination, name, LogStatusCompleted) != 0 {
		t.Fatal(runErr)
	}
}
