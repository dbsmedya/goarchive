//go:build integration

package preflight

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// TestMySQLMixedCaseNames (M7c-2, register M2-C9): tables created as Orders and OrderLines,
// and a copy-only job spelling them either way. Under lower_case_table_names=1 MySQL
// stores and reports every table name in lower case (probe PR-4).
// Environment pair (R56, P8), across servers:
//   - lower_case_table_names=0 (the estate): the mixed-case job passes; the lower-case
//     spelling names tables that do not exist and is refused with TABLE_EXISTENCE_CHECK.
//   - lower_case_table_names=1 (the environment matrix's init row): the mixed-case job is
//     refused with TABLE_EXISTENCE_CHECK naming both tables, before any check could look a
//     table up by MySQL's lower-case spelling (M2-C9's exact-case Tree.Node), and the
//     lower-case spelling passes.
func TestMySQLMixedCaseNames(t *testing.T) {
	src, dst := scratch(t, "TEST_SOURCE", "mixcase"), scratch(t, "TEST_DEST", "mixcase")
	for _, db := range []*sql.DB{src, dst} {
		execAll(t, db,
			"CREATE TABLE Orders (Id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB",
			"CREATE TABLE OrderLines (Id BIGINT NOT NULL PRIMARY KEY, Order_Id BIGINT NOT NULL, KEY ix_order (Order_Id), "+
				"CONSTRAINT fk_lines FOREIGN KEY (Order_Id) REFERENCES Orders (Id)) ENGINE=InnoDB")
	}
	var lctn int
	if err := src.QueryRowContext(context.Background(), "SELECT @@lower_case_table_names").Scan(&lctn); err != nil {
		t.Fatal(err)
	}
	spelled := func(root, child string) *jobspec.Spec {
		return job(t, jobspec.CopyOnly, `
    root_table: `+root+`
    primary_key: Id
    where: "1=1"
    relations:
      - table: `+child+`
        primary_key: Id
        foreign_key: Order_Id
`)
	}
	refused := func(t *testing.T, err error, tables ...string) {
		t.Helper()
		var pe *Error
		if !errors.As(err, &pe) || pe.Check != "TABLE_EXISTENCE_CHECK" {
			t.Fatalf("run = %v, want TABLE_EXISTENCE_CHECK", err)
		}
		got := slices.Clone(pe.Tables)
		slices.Sort(got)
		if !slices.Equal(got, tables) {
			t.Errorf("TABLE_EXISTENCE_CHECK tables = %v, want %v", got, tables)
		}
	}
	mixed, lower := spelled("Orders", "OrderLines"), spelled("orders", "orderlines")
	t.Logf("@@lower_case_table_names = %d", lctn)
	switch lctn {
	case 0:
		if _, err := runOn(t, src, dst, mixed); err != nil {
			t.Errorf("mixed-case job on a case-sensitive server: %v, want it to pass", err)
		}
		_, err := runOn(t, src, dst, lower)
		refused(t, err, "orderlines", "orders")
	case 1:
		_, err := runOn(t, src, dst, mixed)
		refused(t, err, "OrderLines", "Orders")
		if _, err := runOn(t, src, dst, lower); err != nil {
			t.Errorf("lower-case job under lower_case_table_names=1: %v, want it to pass", err)
		}
	default:
		t.Skipf("lower_case_table_names = %d: only 0 and 1 run on Linux servers", lctn)
	}
}
