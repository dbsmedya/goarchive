//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/types"
	"github.com/dbsmedya/goarchive/internal/verifier"
	"os"
	"strings"
	"testing"
)

func vpRequireProfile(t *testing.T, profiles ...string) string {
	t.Helper()
	profile := os.Getenv("VP_MATRIX_PROFILE")
	if profile == "" {
		t.Skip("matrix-only: server-startup profile required: " + strings.Join(profiles, ","))
	}
	for _, want := range profiles {
		if profile == want {
			return profile
		}
	}
	t.Fatalf("wrong matrix profile %q, want %v", profile, profiles)
	return ""
}
func vpAssertProfileFacts(t *testing.T, db *sql.DB) {
	t.Helper()
	var mode, zone string
	var cap, notes int
	if err := db.QueryRow("SELECT @@SESSION.sql_mode,@@SESSION.max_error_count,@@SESSION.sql_notes,@@SESSION.time_zone").Scan(&mode, &cap, &notes, &zone); err != nil {
		t.Fatal(err)
	}
	t.Logf("session mode=%s capacity=%d notes=%d timezone=%s", mode, cap, notes, zone)
	if notes != 1 || zone != "+00:00" {
		t.Fatal("production session initialization missing")
	}
	switch os.Getenv("VP_MATRIX_PROFILE") {
	case "empty":
		if mode != "NO_AUTO_VALUE_ON_ZERO" {
			t.Fatalf("empty profile mode=%s", mode)
		}
	case "raw":
		if !strings.Contains(mode, "ALLOW_INVALID_DATES") || strings.Contains(mode, "STRICT") {
			t.Fatalf("raw profile mode=%s", mode)
		}
	case "cap1":
		if cap != 1 {
			t.Fatalf("capacity=%d want1", cap)
		}
	case "cap0":
		if cap != 0 {
			t.Fatalf("capacity=%d want0", cap)
		}
	case "default", "":
		if !strings.Contains(mode, "STRICT_TRANS_TABLES") {
			t.Fatalf("strict profile mode=%s", mode)
		}
	}
}
func vpCopier(t testing.TB, cfg *config.Config, mgr *database.Manager, g *graph.Graph, method string, skip bool) *CopyPhase {
	t.Helper()
	cp, err := NewCopyPhase(mgr.Source, mgr.Destination, g, cfg.Safety, nil)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := sourceColumnMetadata(context.Background(), mgr.Source, cfg.Source.Database, g.AllNodes())
	if err != nil {
		t.Fatal(err)
	}
	cp.SetColumnMetadata(meta)
	cp.SetStrictInsert(method != "sha256" || skip)
	cp.SetDiagnosticPolicy(config.VerificationConfig{Method: method, SkipVerification: skip}, false)
	return cp
}
func TestDiagnosticNotesInitialized_Integration(t *testing.T) {
	if os.Getenv("VP_NOTES_OFF") != "1" {
		t.Skip("matrix-only: server initialized with global sql_notes=0 required")
	}
	vpRequireProfile(t, "notes-off")
	_, mgr := vpFixture(t)
	ctx := context.Background()
	db := mgr.Destination
	db.SetMaxOpenConns(3)
	first, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	facts := func(conn *sql.Conn) int64 {
		var global, session int
		var id int64
		if err := conn.QueryRowContext(ctx, "SELECT @@GLOBAL.sql_notes,@@SESSION.sql_notes,CONNECTION_ID()").Scan(&global, &session, &id); err != nil {
			t.Fatal(err)
		}
		t.Logf("global_sql_notes=%d session_sql_notes=%d connection_id=%d", global, session, id)
		if global != 0 || session != 1 {
			t.Fatalf("nonvacuous note initialization failed: global=%d session=%d", global, session)
		}
		return id
	}
	id1, id2 := facts(first), facts(second)
	if id1 == id2 {
		t.Fatal("expected independent physical connections")
	}
	if _, err := first.ExecContext(ctx, "DROP TEMPORARY TABLE IF EXISTS vp_missing_note"); err != nil {
		t.Fatal(err)
	}
	var count uint64
	if err := first.QueryRowContext(ctx, diagnosticCountSQL).Scan(&count); err != nil {
		t.Fatal(err)
	}
	var level, message string
	var code uint16
	if err := first.QueryRowContext(ctx, diagnosticDetailsSQL).Scan(&level, &code, &message); err != nil {
		t.Fatal(err)
	}
	if count != 1 || level != "Note" || code != 1051 {
		t.Fatalf("missing visible Note: count=%d level=%s code=%d", count, level, code)
	}
	db.SetMaxIdleConns(0)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	replacement, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	id3 := facts(replacement)
	if id3 == id1 || id3 == id2 {
		t.Fatal("replacement reused a closed physical connection")
	}
}
func vpDriverDiagnosticClose(t *testing.T, explicit bool) {
	t.Helper()
	_, mgr := vpFixture(t)
	vpAssertProfileFacts(t, mgr.Destination)
	vpTable(t, mgr, "vp_warn", "(id BIGINT PRIMARY KEY,n TINYINT) ENGINE=InnoDB")
	ctx := context.Background()
	a, err := mgr.Destination.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := mgr.Destination.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if _, err := b.ExecContext(ctx, "CREATE TEMPORARY TABLE vp_unrelated(id INT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ExecContext(ctx, "INSERT IGNORE INTO vp_unrelated VALUES(1),(1)"); err != nil {
		t.Fatal(err)
	}
	tx, err := a.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	query := "INSERT IGNORE INTO vp_warn(id,n) VALUES(?,?)"
	if explicit {
		stmt, err := tx.PrepareContext(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = stmt.ExecContext(ctx, 1, 999); err != nil {
			t.Fatal(err)
		}
		if err := stmt.Close(); err != nil {
			t.Fatal(err)
		}
	} else if _, err = tx.ExecContext(ctx, query, 1, 999); err != nil {
		t.Fatal(err)
	}
	d, err := collectInsertDiagnostics(ctx, tx, "vp_warn")
	t.Logf("driver explicit=%v diagnostics=%+v error=%v", explicit, d, err)
	if err != nil || d.ByKind[diagnosticKey{"Warning", 1264}] != 1 || d.ByKind[diagnosticKey{"Warning", 1062}] != 0 {
		t.Fatal("INSERT diagnostics did not survive driver lifecycle")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := mgr.Destination.QueryRow("SELECT COUNT(*) FROM vp_warn").Scan(&n); err != nil || n != 0 {
		t.Fatalf("rollback count=%d err=%v", n, err)
	}
}
func TestDiagnosticImplicitClose_Integration(t *testing.T) { vpDriverDiagnosticClose(t, false) }
func TestDiagnosticExplicitClose_Integration(t *testing.T) { vpDriverDiagnosticClose(t, true) }
func vpSessionOwnership(t *testing.T, clean bool) {
	t.Helper()
	cfg, mgr := vpFixture(t)
	vpAssertProfileFacts(t, mgr.Destination)
	vpTable(t, mgr, "vp_warn", "(id BIGINT PRIMARY KEY,n TINYINT) ENGINE=InnoDB")
	db := mgr.Destination
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)
	ctx := context.Background()
	a, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	session, err := readDiagnosticSession(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	b, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var idA, idB int64
	if err := a.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&idA); err != nil {
		t.Fatal(err)
	}
	if err := b.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&idB); err != nil {
		t.Fatal(err)
	}
	var count uint64
	if err := b.QueryRowContext(ctx, diagnosticCountSQL).Scan(&count); err != nil || count != 0 || idA == idB {
		t.Fatalf("ownership precondition: ids=%d/%d count=%d err=%v", idA, idB, count, err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	cp := vpCopier(t, cfg, mgr, graph.NewGraph("vp_warn", "id"), "sha256", false)
	tx, err := a.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	n := int64(999)
	if clean {
		n = 1
	}
	affected, runErr := cp.execInsertBatch(ctx, tx, "vp_warn", []string{"id", "n"}, 1, []interface{}{int64(1), n}, &copyOperation{Session: session})
	if clean {
		if runErr != nil || affected != 1 {
			t.Fatalf("clean ownership control: expected one inserted row without diagnostics: %v", runErr)
		}
	} else if runErr == nil || !strings.Contains(runErr.Error(), "INSERT_DIAGNOSTIC_REJECTED") || !strings.Contains(runErr.Error(), "1264") {
		t.Fatalf("diagnostic ownership: expected INSERT_DIAGNOSTIC_REJECTED containing 1264: %v", runErr)
	}
	t.Logf("owner=%d idle=%d affected=%d error=%v", idA, idB, affected, runErr)
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var remaining int
	if err := db.QueryRow("SELECT COUNT(*) FROM vp_warn").Scan(&remaining); err != nil || remaining != 0 {
		t.Fatalf("rollback remaining=%d err=%v", remaining, err)
	}
}
func TestDiagnosticSessionOwnership_Integration(t *testing.T)      { vpSessionOwnership(t, false) }
func TestDiagnosticSessionOwnershipClean_Integration(t *testing.T) { vpSessionOwnership(t, true) }
func TestDiagnosticRetention_Integration(t *testing.T) {
	profile := vpRequireProfile(t, "cap1", "cap0")
	_, mgr := vpFixture(t)
	vpAssertProfileFacts(t, mgr.Destination)
	vpTable(t, mgr, "vp_retain", "(id BIGINT PRIMARY KEY) ENGINE=InnoDB")
	vpExec(t, mgr.Destination, "INSERT INTO vp_retain VALUES(1),(2),(3)")
	tx, err := mgr.Destination.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec("INSERT IGNORE INTO vp_retain VALUES(?),(?),(?)", 1, 2, 3); err != nil {
		t.Fatal(err)
	}
	d, err := collectInsertDiagnostics(context.Background(), tx, "vp_retain")
	t.Logf("retention diagnostics=%+v error=%v", d, err)
	want := uint64(1)
	if profile == "cap0" {
		want = 0
	}
	if err != nil || d.Count != 3 || d.Retained != want {
		t.Fatalf("retention fixture failed: %+v %v", d, err)
	}
	_, err = classifyInsertDiagnostics(insertDiagnosticContext{UsesIgnore: true, VerificationMethod: "sha256"}, d)
	if err == nil || !strings.Contains(err.Error(), "INSERT_DIAGNOSTICS_UNPROVEN") {
		t.Fatal(err)
	}
}
func TestDiagnosticDuplicateSubdivision_Integration(t *testing.T) {
	vpRequireProfile(t, "cap1")
	cfg, mgr := vpFixture(t)
	vpAssertProfileFacts(t, mgr.Destination)
	vpTable(t, mgr, "vp_subdivide", "(id BIGINT PRIMARY KEY,n INT) ENGINE=InnoDB")
	for _, db := range []*sql.DB{mgr.Source, mgr.Destination} {
		vpExec(t, db, "INSERT INTO vp_subdivide VALUES(1,1),(2,2),(3,3),(4,4),(5,5)")
	}
	g := graph.NewGraph("vp_subdivide", "id")
	cp := vpCopier(t, cfg, mgr, g, "sha256", false)
	rs := &RecordSet{Records: map[string][]interface{}{"vp_subdivide": {int64(1), int64(2), int64(3), int64(4), int64(5)}}}
	if _, err := cp.Copy(context.Background(), rs); err != nil {
		t.Fatal(err)
	}
	v, err := verifier.NewVerifier(mgr.Source, mgr.Destination, g, verifier.MethodSHA256, nil)
	if err != nil {
		t.Fatal(err)
	}
	v.SetColumnMetadata(cp.columnMetadata)
	if _, err := v.Verify(context.Background(), rs); err != nil {
		t.Fatal(err)
	}
	if len(cp.ConversionTotals()) != 0 {
		t.Fatal(cp.ConversionTotals())
	}
}
func TestDiagnosticCapacityZeroClean_Integration(t *testing.T) {
	vpRequireProfile(t, "cap0")
	cfg, mgr := vpFixture(t)
	vpAssertProfileFacts(t, mgr.Destination)
	vpTable(t, mgr, "vp_capzero", "(id BIGINT PRIMARY KEY,n INT) ENGINE=InnoDB")
	vpExec(t, mgr.Source, "INSERT INTO vp_capzero VALUES(1,1)")
	cp := vpCopier(t, cfg, mgr, graph.NewGraph("vp_capzero", "id"), "count", false)
	if _, err := cp.Copy(context.Background(), &RecordSet{Records: map[string][]interface{}{"vp_capzero": {int64(1)}}}); err != nil {
		t.Fatal(err)
	}
}
func TestDiagnosticMixedOrder_Integration(t *testing.T) {
	_, mgr := vpFixture(t)
	vpAssertProfileFacts(t, mgr.Destination)
	vpTable(t, mgr, "vp_mixed", "(id BIGINT PRIMARY KEY,d DATE,dt DATETIME(6)) ENGINE=InnoDB")
	vpExec(t, mgr.Destination, "INSERT INTO vp_mixed VALUES(2,'2020-03-02','2020-03-02 12:00:00')")
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint(reverse), func(t *testing.T) {
			tx, err := mgr.Destination.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			args := []interface{}{1, "2020-02-31", "2020-02-31 12:00:00.123456", 2, "2020-03-02", "2020-03-02 12:00:00"}
			if reverse {
				args = append(args[3:6:6], args[:3]...)
			}
			if _, err := tx.Exec("INSERT IGNORE INTO vp_mixed VALUES(?,?,?),(?,?,?)", args...); err != nil {
				t.Fatal(err)
			}
			d, err := collectInsertDiagnostics(context.Background(), tx, "vp_mixed")
			t.Logf("reverse=%v diagnostics=%+v error=%v", reverse, d, err)
			if err != nil || d.Count != 3 || d.ByKind[diagnosticKey{"Warning", 1264}] != 2 || d.ByKind[diagnosticKey{"Warning", 1062}] != 1 {
				t.Fatal(d, err)
			}
			for _, skip := range []bool{false, true} {
				c := insertDiagnosticContext{UsesIgnore: !skip, SkipVerification: skip, VerificationMethod: "sha256"}
				if _, err := classifyInsertDiagnostics(c, d); err == nil {
					t.Fatal("runtime mixture accepted")
				}
			}
		})
	}
}
func TestDiagnosticLimitedUser_Integration(t *testing.T) {
	vpRequireProfile(t, "default")
	cfg, mgr := vpFixture(t)
	vpTable(t, mgr, "vp_worker_copy", "(id BIGINT PRIMARY KEY,n INT) ENGINE=InnoDB")
	vpExec(t, mgr.Source, "INSERT INTO vp_worker_copy VALUES(1,1)")
	srcCfg, dstCfg := cfg.Source, cfg.Destination
	srcCfg.User, dstCfg.User = "vp_worker", "vp_worker"
	srcCfg.Password, dstCfg.Password = "vp-worker-disposable", "vp-worker-disposable"
	src, err := sql.Open("mysql", database.BuildDSN(&srcCfg))
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	dst, err := sql.Open("mysql", database.BuildDSN(&dstCfg))
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	for _, db := range []*sql.DB{src, dst} {
		var user, mode string
		var notes int
		if err := db.QueryRow("SELECT CURRENT_USER(),@@SESSION.sql_mode,@@SESSION.sql_notes").Scan(&user, &mode, &notes); err != nil {
			t.Fatal(err)
		}
		t.Logf("worker identity=%s mode=%s notes=%d", user, mode, notes)
		if !strings.HasPrefix(user, "vp_worker@") || !strings.Contains(mode, "NO_AUTO_VALUE_ON_ZERO") || notes != 1 {
			t.Fatal("worker session identity/settings wrong")
		}
	}
	cp, err := NewCopyPhase(src, dst, graph.NewGraph("vp_worker_copy", "id"), config.SafetyConfig{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	cp.SetColumnMetadata(map[string]types.ColumnMetadata{"vp_worker_copy": {Names: []string{"id", "n"}, Temporal: map[string]types.TemporalKind{}}})
	cp.SetStrictInsert(true)
	cp.SetDiagnosticPolicy(config.VerificationConfig{Method: "count"}, false)
	stats, err := cp.Copy(context.Background(), &RecordSet{Records: map[string][]interface{}{"vp_worker_copy": {int64(1)}}})
	if err != nil || stats.RowsCopied != 1 {
		t.Fatal(stats, err)
	}
}
