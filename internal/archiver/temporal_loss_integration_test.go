//go:build integration

package archiver

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/internal/archiver/testsupport"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
	"github.com/dbsmedya/goarchive/internal/graph"
)

func TestTemporalPayloadStrictIgnore_Integration(t *testing.T) {
	cfg, mgr := vpFixture(t)
	cfg.Verification.Method = "sha256"
	const table, name = "vp_dates", "vp_107_dates"
	vpTable(t, mgr, table, "(id BIGINT PRIMARY KEY,d DATE,dt DATETIME(6)) ENGINE=InnoDB")
	vpSeed(t, cfg.Source, "ALLOW_INVALID_DATES", "INSERT INTO vp_dates VALUES(1,'2020-02-31','2020-02-31 12:00:00.123456')")
	var mode, raw string
	if err := mgr.Destination.QueryRow("SELECT @@SESSION.sql_mode").Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(mode, "STRICT_TRANS_TABLES") {
		t.Fatalf("strict fixture unavailable: mode=%s", mode)
	}
	if err := mgr.Source.QueryRow("SELECT CAST(d AS CHAR) FROM vp_dates WHERE id=1").Scan(&raw); err != nil || raw != "2020-02-31" {
		t.Fatalf("fixture raw=%q err=%v", raw, err)
	}
	t.Logf("fixture raw=%s destination mode=%s", raw, mode)
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
	var sourceCount, destCount int64
	if err := mgr.Source.QueryRow("SELECT COUNT(*) FROM vp_dates").Scan(&sourceCount); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Destination.QueryRow("SELECT COUNT(*) FROM vp_dates").Scan(&destCount); err != nil {
		t.Fatal(err)
	}
	if destCount > 0 {
		if err := mgr.Destination.QueryRow("SELECT CAST(d AS CHAR) FROM vp_dates WHERE id=1").Scan(&raw); err != nil {
			t.Fatal(err)
		}
		t.Logf("destination raw=%s", raw)
	}
	t.Logf("source=%d destination=%d run=%v", sourceCount, destCount, runErr)
	if runErr == nil || !strings.Contains(runErr.Error(), "INSERT_DIAGNOSTIC_REJECTED") {
		t.Errorf("missing pre-commit diagnostic refusal: %v", runErr)
	}
	if sourceCount != 1 || destCount != 0 {
		t.Errorf("TEMPORAL_PAYLOAD_LOST: source=%d destination=%d, want 1/0", sourceCount, destCount)
	}
}

func vpCollision(t *testing.T) (*config.Config, *database.Manager, *config.JobConfig) {
	t.Helper()
	cfg, mgr := vpFixture(t)
	vpTable(t, mgr, "vp_roots", "(id BIGINT PRIMARY KEY) ENGINE=InnoDB")
	vpTable(t, mgr, "vp_keys", "(d DATE PRIMARY KEY,root_id BIGINT NOT NULL,x INT,KEY(root_id),FOREIGN KEY(root_id) REFERENCES vp_roots(id)) ENGINE=InnoDB")
	vpTable(t, mgr, "vp_leaves", "(id BIGINT PRIMARY KEY,parent_d DATE NOT NULL,KEY(parent_d),FOREIGN KEY(parent_d) REFERENCES vp_keys(d)) ENGINE=InnoDB")
	vpSeed(t, cfg.Source, "ALLOW_INVALID_DATES", "INSERT INTO vp_roots VALUES(1),(2)", "INSERT INTO vp_keys VALUES('2020-02-31',1,1),('2020-03-02',2,2)", "INSERT INTO vp_leaves VALUES(11,'2020-02-31'),(22,'2020-03-02')")
	return cfg, mgr, &config.JobConfig{RootTable: "vp_roots", PrimaryKey: "id", Where: "id=1", Relations: []config.Relation{{Table: "vp_keys", PrimaryKey: "d", ForeignKey: "root_id", DependencyType: "1-N", Relations: []config.Relation{{Table: "vp_leaves", PrimaryKey: "id", ForeignKey: "parent_d", DependencyType: "1-N"}}}}}
}
func vpAssertCollisionRetained(t *testing.T, mgr *database.Manager, runErr error) {
	t.Helper()
	queries := []struct {
		sql  string
		want []string
	}{
		{"SELECT CAST(id AS CHAR) FROM vp_roots ORDER BY id", []string{"1", "2"}},
		{"SELECT CONCAT(CAST(d AS CHAR),'/',x) FROM vp_keys ORDER BY d", []string{"2020-02-31/1", "2020-03-02/2"}},
		{"SELECT CAST(id AS CHAR) FROM vp_leaves ORDER BY id", []string{"11", "22"}},
	}
	for _, q := range queries {
		rows, err := mgr.Source.Query(q.sql)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for rows.Next() {
			var v string
			if err := rows.Scan(&v); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			got = append(got, v)
		}
		iterErr, closeErr := rows.Err(), rows.Close()
		if iterErr != nil || closeErr != nil {
			t.Fatalf("read evidence: %v/%v", iterErr, closeErr)
		}
		t.Logf("source rows=%v run=%v", got, runErr)
		if !reflect.DeepEqual(got, q.want) {
			t.Errorf("TEMPORAL_KEY_REDIRECTED: got=%v want=%v", got, q.want)
		}
	}
	if runErr == nil || !strings.Contains(runErr.Error(), "TEMPORAL_READ_CONTRACT") {
		t.Errorf("expected TEMPORAL_READ_CONTRACT, got %v", runErr)
	}
}
func TestTemporalKeyArchiveCollision_Integration(t *testing.T) {
	cfg, mgr, job := vpCollision(t)
	const name = "vp_107_key_archive"
	orch, err := NewOrchestrator(cfg, name, job, mgr)
	if err != nil {
		t.Fatal(err)
	}
	if err = orch.Initialize(); err != nil {
		t.Fatal(err)
	}
	testsupport.CleanupArchiverState(t, mgr.Destination, name)
	_, err = orch.Execute(context.Background(), nil)
	vpAssertCollisionRetained(t, mgr, err)
}
func TestTemporalKeyPurgeCollision_Integration(t *testing.T) {
	cfg, mgr, job := vpCollision(t)
	const name = "vp_107_key_purge"
	orch, err := NewPurgeOrchestrator(cfg, name, job, mgr)
	if err != nil {
		t.Fatal(err)
	}
	if err = orch.Initialize(); err != nil {
		t.Fatal(err)
	}
	testsupport.CleanupArchiverState(t, mgr.Destination, name)
	_, err = orch.Execute(context.Background())
	vpAssertCollisionRetained(t, mgr, err)
}
func TestTemporalKeyCopyOnlyCollision_Integration(t *testing.T) {
	cfg, mgr, job := vpCollision(t)
	const name = "vp_107_key_copy"
	orch, err := NewCopyOnlyOrchestrator(cfg, name, job, mgr)
	if err != nil {
		t.Fatal(err)
	}
	if err = orch.Initialize(); err != nil {
		t.Fatal(err)
	}
	testsupport.CleanupArchiverState(t, mgr.Destination, name)
	_, err = orch.Execute(context.Background(), false)
	vpAssertCollisionRetained(t, mgr, err)
}
func TestTemporalDiscoveryRawCollision_Integration(t *testing.T) {
	cfg, mgr, job := vpCollision(t)
	g, err := graph.BuildFromJob(job)
	if err != nil {
		t.Fatal(err)
	}
	d, err := NewRecordDiscovery(g, mgr.Source, 100)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := sourceColumnMetadata(context.Background(), mgr.Source, cfg.Source.Database, g.AllNodes())
	if err != nil {
		t.Fatal(err)
	}
	d.SetColumnMetadata(metadata)
	rs, err := d.Discover(context.Background(), []interface{}{int64(1)})
	if err == nil || !strings.Contains(err.Error(), "TEMPORAL_READ_CONTRACT") {
		t.Fatalf("expected TEMPORAL_READ_CONTRACT, got normalized key: records=%#v error=%v", rs, err)
	}
}
