//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"testing"

	"github.com/dbsmedya/goarchive/internal/archiver/testsupport"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
)

func TestArchiveAutoIncrementZeroCount_Integration(t *testing.T) {
	cfg, mgr := vpFixture(t)
	const table, jobName = "vp_auto", "vp_108_archive"
	vpTable(t, mgr, table, "(id BIGINT PRIMARY KEY, seq BIGINT NOT NULL AUTO_INCREMENT, payload VARCHAR(20), KEY(seq)) ENGINE=InnoDB")
	vpSeed(t, cfg.Source, "NO_AUTO_VALUE_ON_ZERO",
		"INSERT INTO vp_auto(id,seq,payload) VALUES(1,0,'original')")
	var original int64
	if err := mgr.Source.QueryRow("SELECT seq FROM vp_auto WHERE id=1").Scan(&original); err != nil || original != 0 {
		t.Fatalf("fixture zero missing: seq=%d err=%v", original, err)
	}
	job := &config.JobConfig{RootTable: table, PrimaryKey: "id", Where: "id=1"}
	orch, err := NewOrchestrator(cfg, jobName, job, mgr)
	if err != nil {
		t.Fatal(err)
	}
	if err = orch.Initialize(); err != nil {
		t.Fatal(err)
	}
	testsupport.CleanupArchiverState(t, mgr.Destination, jobName)
	_, runErr := orch.Execute(context.Background(), nil)
	var got, remaining int64
	readErr := mgr.Destination.QueryRow("SELECT seq FROM vp_auto WHERE id=1").Scan(&got)
	countErr := mgr.Source.QueryRow("SELECT COUNT(*) FROM vp_auto WHERE id=1").Scan(&remaining)
	t.Logf("evidence: archived seq=%d source remaining=%d run=%v read=%v count=%v", got, remaining, runErr, readErr, countErr)
	if runErr != nil || readErr != nil || countErr != nil {
		t.Fatalf("archive failed: %v / %v / %v", runErr, readErr, countErr)
	}
	if got != 0 {
		t.Errorf("AUTO_ZERO_LOST: archived seq=%d, want 0; source remaining=%d", got, remaining)
	}
	if remaining != 0 {
		t.Errorf("archive retained %d source rows, want 0 after successful copy", remaining)
	}
}

func TestCopyOnlyAutoIncrementZero_Integration(t *testing.T) {
	cfg, mgr := vpFixture(t)
	const table, name = "vp_auto_copy", "vp_108_copy"
	vpTable(t, mgr, table, "(id BIGINT PRIMARY KEY,seq BIGINT NOT NULL AUTO_INCREMENT,KEY(seq)) ENGINE=InnoDB")
	vpSeed(t, cfg.Source, "NO_AUTO_VALUE_ON_ZERO", "INSERT INTO vp_auto_copy VALUES(1,0)")
	job := &config.JobConfig{RootTable: table, PrimaryKey: "id", Where: "id=1"}
	orch, err := NewCopyOnlyOrchestrator(cfg, name, job, mgr)
	if err != nil {
		t.Fatal(err)
	}
	if err = orch.Initialize(); err != nil {
		t.Fatal(err)
	}
	testsupport.CleanupArchiverState(t, mgr.Destination, name)
	if _, err = orch.Execute(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	for _, db := range []*sql.DB{mgr.Source, mgr.Destination} {
		var got int64
		if err := db.QueryRow("SELECT seq FROM vp_auto_copy WHERE id=1").Scan(&got); err != nil || got != 0 {
			t.Fatalf("zero missing: got=%d err=%v", got, err)
		}
	}
}
func TestPayloadAutoIncrementZero_Integration(t *testing.T) {
	cfg, mgr := vpFixture(t)
	const table = "vp_auto_sample"
	vpTable(t, mgr, table, "(id BIGINT PRIMARY KEY,seq BIGINT NOT NULL AUTO_INCREMENT,KEY(seq)) ENGINE=InnoDB")
	vpSeed(t, cfg.Source, "NO_AUTO_VALUE_ON_ZERO", "INSERT INTO vp_auto_sample VALUES(1,0)")
	job := &config.JobConfig{RootTable: table, PrimaryKey: "id", Where: "id=1"}
	p := NewPayloadValidator(mgr.Source, mgr.Destination, graph.NewGraph(table, "id"), cfg.Source.Database, job, cfg.Safety, 100, cfg.Verification, logger.NewDefault())
	if err := p.Validate(context.Background()); err != nil {
		t.Fatal(err)
	}
	var sourceZero, destCount int
	if err := mgr.Source.QueryRow("SELECT COUNT(*) FROM vp_auto_sample WHERE id=1 AND seq=0").Scan(&sourceZero); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Destination.QueryRow("SELECT COUNT(*) FROM vp_auto_sample").Scan(&destCount); err != nil {
		t.Fatal(err)
	}
	if sourceZero != 1 || destCount != 0 {
		t.Fatalf("sample changed rows: source zero=%d dest=%d", sourceZero, destCount)
	}
}
