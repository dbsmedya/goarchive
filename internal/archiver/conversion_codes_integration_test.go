//go:build integration

package archiver

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/internal/archiver/testsupport"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/go-sql-driver/mysql"
)

func TestD9StrictStatementError_Integration(t *testing.T) {
	for _, tc := range []struct {
		name, method string
		skip, unique bool
	}{{"count", "count", false, false}, {"skip", "count", true, false}, {"secondary_unique", "sha256", false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, mgr := vpFixture(t)
			cfg.Verification.Method = tc.method
			cfg.Verification.SkipVerification = tc.skip
			const table, name = "vp_strict", "vp_107_strict"
			extra := ""
			if tc.unique {
				extra = ",UNIQUE KEY(u)"
			}
			vpTable(t, mgr, table, "(id BIGINT PRIMARY KEY,d DATE,dt DATETIME(6),u BIGINT NOT NULL"+extra+") ENGINE=InnoDB")
			vpSeed(t, cfg.Source, "ALLOW_INVALID_DATES", "INSERT INTO vp_strict VALUES(1,'2020-02-31','2020-02-31 12:00:00.123456',9)")
			var raw, mode string
			if err := mgr.Source.QueryRow("SELECT CONCAT(CAST(d AS CHAR),'/',CAST(dt AS CHAR)) FROM vp_strict WHERE id=1").Scan(&raw); err != nil {
				t.Fatal(err)
			}
			if err := mgr.Destination.QueryRow("SELECT @@SESSION.sql_mode").Scan(&mode); err != nil {
				t.Fatal(err)
			}
			t.Logf("fixture raw=%s mode=%s", raw, mode)
			if raw != "2020-02-31/2020-02-31 12:00:00.123456" || !strings.Contains(mode, "STRICT_TRANS_TABLES") {
				t.Fatal("strict fixture precondition missing")
			}
			orch, err := NewOrchestrator(cfg, name, &config.JobConfig{RootTable: table, PrimaryKey: "id", Where: "id=1"}, mgr)
			if err != nil {
				t.Fatal(err)
			}
			if err = orch.Initialize(); err != nil {
				t.Fatal(err)
			}
			testsupport.CleanupArchiverState(t, mgr.Destination, name)
			_, runErr := orch.Execute(context.Background(), nil)
			var src, dst int
			if err := mgr.Source.QueryRow("SELECT COUNT(*) FROM vp_strict").Scan(&src); err != nil {
				t.Fatal(err)
			}
			if err := mgr.Destination.QueryRow("SELECT COUNT(*) FROM vp_strict").Scan(&dst); err != nil {
				t.Fatal(err)
			}
			copied, completed := vpMarkerCount(t, mgr.Destination, name, LogStatusCopied), vpMarkerCount(t, mgr.Destination, name, LogStatusCompleted)
			t.Logf("source=%d destination=%d copied=%d completed=%d run=%v", src, dst, copied, completed, runErr)
			var sqlErr *mysql.MySQLError
			if !errors.As(runErr, &sqlErr) || sqlErr.Number != 1292 || src != 1 || dst != 0 || copied != 0 || completed != 0 {
				t.Errorf("strict plain INSERT accepted invalid temporal payload: run=%v", runErr)
			}
		})
	}
}
