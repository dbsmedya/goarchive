//go:build integration

package archiver

import (
	"context"
	"errors"
	"fmt"
	"github.com/dbsmedya/dbsgomysql/pkg/sqlutil"
	"github.com/dbsmedya/goarchive/internal/graph"
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

func vpD9CopyCase(t *testing.T, table, sourceDefinition, destDefinition, seed, want string, code uint16) {
	t.Helper()
	vpRequireProfile(t, "empty")
	for _, skip := range []bool{false, true} {
		t.Run(fmt.Sprintf("skip_%v", skip), func(t *testing.T) {
			cfg, mgr := vpFixture(t)
			vpAssertProfileFacts(t, mgr.Destination)
			vpTable(t, mgr, table, sourceDefinition)
			vpExec(t, mgr.Destination, "DROP TABLE "+sqlutil.QuoteIdentifier(table))
			vpExec(t, mgr.Destination, "CREATE TABLE "+sqlutil.QuoteIdentifier(table)+" "+destDefinition)
			vpExec(t, mgr.Source, seed)
			cp := vpCopier(t, cfg, mgr, graph.NewGraph(table, "id"), "count", skip)
			_, err := cp.Copy(context.Background(), &RecordSet{Records: map[string][]interface{}{table: {int64(1)}}})
			var count int64
			if e := mgr.Destination.QueryRow("SELECT COUNT(*) FROM " + sqlutil.QuoteIdentifier(table)).Scan(&count); e != nil {
				t.Fatal(e)
			}
			if !skip {
				t.Logf("code=%d count=%d error=%v", code, count, err)
				if err == nil || !strings.Contains(err.Error(), "INSERT_DIAGNOSTIC_REJECTED") || !strings.Contains(err.Error(), fmt.Sprint(code)) || count != 0 {
					t.Fatalf("default accepted conversion: count=%d err=%v", count, err)
				}
				return
			}
			var actual string
			if e := mgr.Destination.QueryRow("SELECT CAST(n AS CHAR) FROM " + sqlutil.QuoteIdentifier(table) + " WHERE id=1").Scan(&actual); e != nil {
				t.Fatal(e)
			}
			t.Logf("code=%d raw=%s totals=%v error=%v", code, actual, cp.ConversionTotals(), err)
			if err != nil || count != 1 || actual != want || cp.ConversionTotals()[table][code] != 1 {
				t.Fatalf("code %d: raw=%q totals=%v, want %q err=%v", code, actual, cp.ConversionTotals(), want, err)
			}
		})
	}
}
func TestD9Code1264CopyBoundary_Integration(t *testing.T) {
	vpD9CopyCase(t, "vp_d9_1264", "(id BIGINT PRIMARY KEY,n INT) ENGINE=InnoDB", "(id BIGINT PRIMARY KEY,n TINYINT) ENGINE=InnoDB", "INSERT INTO vp_d9_1264 VALUES(1,999)", "127", 1264)
}
func TestD9Code1265CopyBoundary_Integration(t *testing.T) {
	vpD9CopyCase(t, "vp_d9_1265", "(id BIGINT PRIMARY KEY,n VARCHAR(10)) ENGINE=InnoDB", "(id BIGINT PRIMARY KEY,n VARCHAR(3)) ENGINE=InnoDB", "INSERT INTO vp_d9_1265 VALUES(1,'abcdef')", "abc", 1265)
}
func TestD9Code1366CopyBoundary_Integration(t *testing.T) {
	vpD9CopyCase(t, "vp_d9_1366", "(id BIGINT PRIMARY KEY,n VARCHAR(10)) ENGINE=InnoDB", "(id BIGINT PRIMARY KEY,n INT) ENGINE=InnoDB", "INSERT INTO vp_d9_1366 VALUES(1,'abc')", "0", 1366)
}
func TestD9Code1292CollectorBoundary_Integration(t *testing.T) {
	vpRequireProfile(t, "empty")
	_, mgr := vpFixture(t)
	vpAssertProfileFacts(t, mgr.Destination)
	conn, err := mgr.Destination.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(context.Background(), "CREATE TEMPORARY TABLE vp_cast(id BIGINT PRIMARY KEY,n BIGINT UNSIGNED)"); err != nil {
		t.Fatal(err)
	}
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec("INSERT INTO vp_cast VALUES(?,CAST(? AS UNSIGNED))", 1, "abc"); err != nil {
		t.Fatal(err)
	}
	d, err := collectInsertDiagnostics(context.Background(), tx, "vp_cast")
	t.Logf("cast diagnostics=%+v error=%v", d, err)
	if err != nil || d.ByKind[diagnosticKey{"Warning", 1292}] != 1 {
		t.Fatal(d, err)
	}
	for _, skip := range []bool{false, true} {
		totals, err := classifyInsertDiagnostics(insertDiagnosticContext{SkipVerification: skip, VerificationMethod: "count"}, d)
		if skip {
			if err != nil || totals[1292] != 1 {
				t.Fatal(totals, err)
			}
		} else if err == nil {
			t.Fatal("default accepted1292")
		}
	}
	var raw string
	if err := tx.QueryRow("SELECT CAST(n AS CHAR) FROM vp_cast WHERE id=1").Scan(&raw); err != nil || raw != "0" {
		t.Fatalf("raw=%s err=%v", raw, err)
	}
	t.Logf("cast raw=%s", raw)
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}
func TestTemporalOverridePlainInsert_Integration(t *testing.T) {
	vpRequireProfile(t, "empty")
	for _, skip := range []bool{false, true} {
		t.Run(fmt.Sprint(skip), func(t *testing.T) {
			cfg, mgr := vpFixture(t)
			vpAssertProfileFacts(t, mgr.Destination)
			cfg.Verification.Method = "count"
			cfg.Verification.SkipVerification = skip
			const table, name = "vp_override", "vp_107_override"
			vpTable(t, mgr, table, "(id BIGINT PRIMARY KEY,d DATE,dt DATETIME(6)) ENGINE=InnoDB")
			vpSeed(t, cfg.Source, "ALLOW_INVALID_DATES", "INSERT INTO vp_override VALUES(1,'2020-02-31','2020-02-31 12:00:00.123456')")
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
			if err := mgr.Source.QueryRow("SELECT COUNT(*) FROM vp_override").Scan(&src); err != nil {
				t.Fatal(err)
			}
			if err := mgr.Destination.QueryRow("SELECT COUNT(*) FROM vp_override").Scan(&dst); err != nil {
				t.Fatal(err)
			}
			t.Logf("override=%v src=%d dst=%d error=%v", skip, src, dst, runErr)
			if !skip {
				if runErr == nil || !strings.Contains(runErr.Error(), "INSERT_DIAGNOSTIC_REJECTED") || src != 1 || dst != 0 {
					t.Fatal(runErr)
				}
				return
			}
			var raw string
			if err := mgr.Destination.QueryRow("SELECT CONCAT(CAST(d AS CHAR),'/',CAST(dt AS CHAR)) FROM vp_override WHERE id=1").Scan(&raw); err != nil {
				t.Fatal(err)
			}
			t.Logf("converted raw=%s", raw)
			if runErr != nil || src != 0 || dst != 1 || raw != "0000-00-00/0000-00-00 00:00:00.000000" {
				t.Fatalf("override raw/result=%s %v", raw, runErr)
			}
		})
	}
}
