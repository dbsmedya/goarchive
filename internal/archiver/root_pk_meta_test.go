package archiver

import (
	"errors"
	"strings"
	"testing"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
	"github.com/dbsmedya/goarchive/internal/graph"
)

func tableColumns(table string, columns ...validations.ColumnInfo) []validations.TableColumns {
	return []validations.TableColumns{{Table: table, Columns: columns}}
}

// The configured column, rather than the table's actual PRIMARY KEY, owns the
// checkpoint type. The exact match must also beat an earlier case-fold match.
func TestApplyRootPKMetaConfiguredColumnExactMatchWins(t *testing.T) {
	g := graph.NewGraph("orders", "ref_no")
	facts := tableColumns("orders",
		validations.ColumnInfo{Name: "REF_NO", DataType: "bigint"},
		validations.ColumnInfo{Name: "ref_no", DataType: "varchar"},
	)

	err := applyRootPKMeta(g, facts)
	if err == nil || !strings.Contains(err.Error(), "ROOT_PK_TYPE_UNSUPPORTED") {
		t.Fatalf("applyRootPKMeta error = %v, want exact varchar match rejection", err)
	}
	if !strings.Contains(err.Error(), "ref_no") {
		t.Fatalf("error must name the configured column: %v", err)
	}
}

// Column facts intentionally carry no key-shape information. An unrelated second
// column therefore cannot make this skip-preflight path reject a configured integer.
func TestApplyRootPKMetaIgnoresActualPrimaryKeyShape(t *testing.T) {
	g := graph.NewGraph("orders", "id")
	facts := tableColumns("orders",
		validations.ColumnInfo{Name: "id", DataType: "BIGINT"},
		validations.ColumnInfo{Name: "tenant_id", DataType: "bigint"},
	)

	if err := applyRootPKMeta(g, facts); err != nil {
		t.Fatalf("applyRootPKMeta: %v", err)
	}
	dataType, unsigned, ok := g.GetRootPKMeta()
	if !ok || dataType != "bigint" || unsigned {
		t.Fatalf("metadata = (%q, %v, %v), want (bigint, false, true)", dataType, unsigned, ok)
	}
}

func TestApplyRootPKMetaRecordsUnsigned(t *testing.T) {
	g := graph.NewGraph("orders", "id")
	facts := tableColumns("orders",
		validations.ColumnInfo{Name: "id", DataType: "bigint", Unsigned: true},
	)

	if err := applyRootPKMeta(g, facts); err != nil {
		t.Fatalf("applyRootPKMeta: %v", err)
	}
	dataType, unsigned, ok := g.GetRootPKMeta()
	if !ok || dataType != "bigint" || !unsigned {
		t.Fatalf("metadata = (%q, %v, %v), want (bigint, true, true)", dataType, unsigned, ok)
	}
}

func TestApplyRootPKMetaMissingColumnIsPlainError(t *testing.T) {
	g := graph.NewGraph("orders", "LOG_ID")
	err := applyRootPKMeta(g, tableColumns("orders",
		validations.ColumnInfo{Name: "id", DataType: "bigint"},
	))
	if err == nil {
		t.Fatal("expected missing-column error")
	}
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatalf("expected plain error, got *PreflightError: %v", pe)
	}
	if !strings.Contains(err.Error(), "orders") || !strings.Contains(err.Error(), "LOG_ID") {
		t.Fatalf("error must name table and configured column: %v", err)
	}
}

func TestApplyRootPKMetaCaseFoldFallback(t *testing.T) {
	g := graph.NewGraph("events", "log_id")
	facts := tableColumns("events",
		validations.ColumnInfo{Name: "LOG_ID", DataType: "bigint"},
	)

	if err := applyRootPKMeta(g, facts); err != nil {
		t.Fatalf("applyRootPKMeta: %v", err)
	}
	dataType, unsigned, ok := g.GetRootPKMeta()
	if !ok || dataType != "bigint" || unsigned {
		t.Fatalf("metadata = (%q, %v, %v), want (bigint, false, true)", dataType, unsigned, ok)
	}
}
