//go:build integration

package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
)

// TestMySQLColumnCharsetRefusal (CHS-1, Codex's finding): GoArchive reads and binds every
// value through its utf8mb4 session, so a character set whose conversion to utf8mb4 and back
// is not one-to-one merges keys and changes text. cp932 maps its NEC and IBM duplicates to
// one character each: two child keys read as one, the copy moved one, and the parent's
// ON DELETE CASCADE deleted the other, uncopied, while the run reported success. sjis writes
// its backslash 0x5C back as 0x815F. Both are refused before any row moves.
func TestMySQLColumnCharsetRefusal(t *testing.T) {
	t.Run("cp932 child keys", func(t *testing.T) {
		e := newEnv(t, "chscol")
		e.both(t, "CREATE TABLE parents (id INT PRIMARY KEY) ENGINE=InnoDB",
			"CREATE TABLE children (code VARCHAR(4) CHARACTER SET cp932 COLLATE cp932_bin PRIMARY KEY, parent_id INT NOT NULL, "+
				"KEY idx_parent (parent_id), CONSTRAINT fk_children_parent FOREIGN KEY (parent_id) REFERENCES parents (id) ON DELETE CASCADE) ENGINE=InnoDB")
		execAll(t, e.src, "INSERT INTO parents VALUES (1)",
			"INSERT INTO children VALUES (CONVERT(X'8790' USING cp932), 1), (CONVERT(X'81E0' USING cp932), 1)")
		cfg := e.config(t, job(t, jobspec.Archive, "\n    root_table: parents\n    primary_key: id\n    where: 'id = 1'\n"+
			"    relations:\n      - table: children\n        primary_key: code\n        foreign_key: parent_id\n"))
		_, err := e.run(t, cfg)
		left := hexes(t, e.src, "SELECT HEX(code) FROM children ORDER BY code")
		moved := count(t, e.dst, "SELECT COUNT(*) FROM children")
		t.Logf("run error %v; source children %v, destination children %d", err, left, moved)
		wantCharsetRefusal(t, err)
		if !equal(left, []string{"81E0", "8790"}) || moved != 0 {
			t.Errorf("source children %v, destination %d; want both left [81E0 8790] and none copied", left, moved)
		}
		if n := registered(t, e); n != 0 {
			t.Errorf("the refused run registered %d job(s), want 0", n)
		}
	})

	t.Run("sjis payload", func(t *testing.T) {
		e := newEnv(t, "chscol")
		e.both(t, "CREATE TABLE files (id INT PRIMARY KEY, path VARCHAR(20) CHARACTER SET sjis) ENGINE=InnoDB")
		execAll(t, e.src, "INSERT INTO files VALUES (1, CONVERT(X'433A5C746D70' USING sjis))")
		cfg := e.config(t, job(t, jobspec.Archive, "\n    root_table: files\n    primary_key: id\n    where: '1=1'\n"))
		_, err := e.run(t, cfg)
		left, moved := hexes(t, e.src, "SELECT HEX(path) FROM files"), hexes(t, e.dst, "SELECT HEX(path) FROM files")
		t.Logf("run error %v; source %v, destination %v", err, left, moved)
		wantCharsetRefusal(t, err)
		if !equal(left, []string{"433A5C746D70"}) || len(moved) != 0 {
			t.Errorf("source %v, destination %v; want C:\\tmp left as 433A5C746D70 and nothing copied", left, moved)
		}
	})
}

func wantCharsetRefusal(t *testing.T, err error) {
	t.Helper()
	var pe *preflight.Error
	if !errors.As(err, &pe) || pe.Check != "COLUMN_CHARSET_CHECK" {
		t.Errorf("run error = %v, want preflight's COLUMN_CHARSET_CHECK", err)
	}
}

// registered counts job j in the env's job schema; a schema the run never created holds none.
func registered(t *testing.T, e env) int64 {
	t.Helper()
	var n int64
	err := e.dst.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM archiver_job WHERE job_name = 'j'").Scan(&n)
	var me *mysql.MySQLError
	if errors.As(err, &me) && me.Number == 1146 {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	return n
}
