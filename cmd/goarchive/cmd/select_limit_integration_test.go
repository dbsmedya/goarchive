//go:build integration

package cmd

import (
	"fmt"
	"strings"
	"testing"
)

// TestMySQLArchiveUnderAFiniteSelectLimit (beta audit A1): a server's global
// sql_select_limit must not reach GoArchive's sessions. Every read GoArchive makes without
// its own LIMIT — the library's column lists, discovery, copy, verification — returns at
// most sql_select_limit rows, so an inherited limit silently drops columns and children.
// Environment pair (R56): on a server with the default limit it passes either way; under a
// matrix row that sets the global limit to 20 it fails until every session pins the limit.
func TestMySQLArchiveUnderAFiniteSelectLimit(t *testing.T) {
	t.Run("a table wider than the limit", func(t *testing.T) {
		f := newFixture(t, "limitwide", `
  archive_wide:
    job_type: archive
    root_table: wide
    primary_key: id
    where: "id < 100"
`)
		logLimits(t, f)
		cols, values := []string{"id INT NOT NULL"}, []string{"1"}
		for i := 1; i <= 24; i++ {
			cols = append(cols, fmt.Sprintf("c%d VARCHAR(20) NULL", i))
			values = append(values, fmt.Sprintf("'value-%d'", i))
		}
		ddl := "CREATE TABLE wide (" + strings.Join(cols, ", ") + ", PRIMARY KEY (id)) ENGINE=InnoDB"
		mustExec(t, f.src, ddl)
		mustExec(t, f.dst, ddl)
		mustExec(t, f.src, "INSERT INTO wide VALUES ("+strings.Join(values, ", ")+")")

		out, err := execute(t, "archive", "-c", f.config, "--job", "archive_wide")
		if err != nil {
			if scalar(t, f.src, "SELECT COUNT(*) FROM wide") != 1 {
				t.Fatalf("archive refused (%v) but changed the source\n%s", err, out)
			}
			return
		}
		if got := scalar(t, f.src, "SELECT COUNT(*) FROM wide"); got != 0 {
			t.Errorf("source rows after the archive = %d, want 0", got)
		}
		if got := scalar(t, f.dst, "SELECT COUNT(*) FROM wide WHERE c24 = 'value-24' AND c21 = 'value-21'"); got != 1 {
			t.Errorf("the archive deleted the source row and the destination lacks its last columns (c21..c24)\n%s", out)
		}
	})

	t.Run("more children than the limit", func(t *testing.T) {
		// The relation is declared in the job only, with no server foreign key, as in
		// schemas that never declared one: nothing but discovery finds the order's lines.
		f := newFixture(t, "limitfan", `
  archive_orders:
    job_type: archive
    root_table: orders
    primary_key: id
    where: "id = 1"
    relations:
      - table: order_lines
        primary_key: id
        foreign_key: order_id
`)
		logLimits(t, f)
		ddl := []string{
			"CREATE TABLE orders (id INT NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB",
			"CREATE TABLE order_lines (id INT NOT NULL, order_id INT NOT NULL, PRIMARY KEY (id), KEY (order_id)) ENGINE=InnoDB",
		}
		mustExec(t, f.src, ddl...)
		mustExec(t, f.dst, ddl...)
		mustExec(t, f.src, "INSERT INTO orders VALUES (1)")
		for i := 1; i <= 30; i++ {
			mustExec(t, f.src, fmt.Sprintf("INSERT INTO order_lines VALUES (%d, 1)", i))
		}

		out, err := execute(t, "archive", "-c", f.config, "--job", "archive_orders")
		if err != nil {
			if scalar(t, f.src, "SELECT COUNT(*) FROM order_lines") != 30 {
				t.Fatalf("archive refused (%v) but changed the source\n%s", err, out)
			}
			return
		}
		src, dst := scalar(t, f.src, "SELECT COUNT(*) FROM order_lines"), scalar(t, f.dst, "SELECT COUNT(*) FROM order_lines")
		if src != 0 || dst != 30 {
			t.Errorf("after archiving order 1: source lines %d, destination lines %d; want 0 and 30 (lines left "+
				"behind lose their order)\n%s", src, dst, out)
		}
	})
}

// logLimits names the environment: the default limit is 2^64-1, which an int64 cannot hold.
func logLimits(t *testing.T, f fixture) {
	t.Helper()
	var src, dst string
	if err := f.src.QueryRow("SELECT CAST(@@GLOBAL.sql_select_limit AS CHAR)").Scan(&src); err != nil {
		t.Fatal(err)
	}
	if err := f.dst.QueryRow("SELECT CAST(@@GLOBAL.sql_select_limit AS CHAR)").Scan(&dst); err != nil {
		t.Fatal(err)
	}
	t.Logf("@@GLOBAL.sql_select_limit: source %s, destination %s", src, dst)
}
