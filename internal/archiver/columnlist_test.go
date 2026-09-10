package archiver

import (
	"reflect"
	"strings"
	"testing"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
	"github.com/dbsmedya/goarchive/internal/types"
)

// The projection must include every column — invisible and generated alike —
// in ordinal order. Filtering here is exactly the defect issue #23 fixes.
func TestColumnMetadataFromFactsIncludesInvisibleAndGenerated(t *testing.T) {
	facts := []validations.TableColumns{
		{Table: "orders", Columns: []validations.ColumnInfo{
			{Name: "id", DataType: "bigint", Ordinal: 1},
			{Name: "payload", DataType: "bigint", Ordinal: 2, Invisible: true},
			{Name: "doubled", DataType: "bigint", Ordinal: 3, Invisible: true, Generated: true},
		}},
		{Table: "items", Columns: []validations.ColumnInfo{
			{Name: "id", DataType: "bigint", Ordinal: 1},
		}},
	}
	lists, err := columnMetadataFromFacts([]string{"orders", "items"}, facts)
	if err != nil {
		t.Fatalf("columnMetadataFromFacts: %v", err)
	}
	if want := []string{"id", "payload", "doubled"}; !reflect.DeepEqual(lists["orders"].Names, want) {
		t.Fatalf("orders columns = %v, want %v", lists["orders"].Names, want)
	}
	if want := []string{"id"}; !reflect.DeepEqual(lists["items"].Names, want) {
		t.Fatalf("items columns = %v, want %v", lists["items"].Names, want)
	}
}

// A graph table absent from the facts must fail closed, naming the table,
// before any data moves.
func TestColumnMetadataFromFactsMissingTableFailsClosed(t *testing.T) {
	facts := []validations.TableColumns{
		{Table: "orders", Columns: []validations.ColumnInfo{{Name: "id", DataType: "bigint", Ordinal: 1}}},
	}
	_, err := columnMetadataFromFacts([]string{"orders", "ghost"}, facts)
	if err == nil || !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("want fail-closed error naming ghost, got %v", err)
	}
}

// A present table with zero columns is equally fail-closed (cannot occur on a
// real server; guards a broken facts source).
func TestColumnMetadataFromFactsEmptyColumnsFailsClosed(t *testing.T) {
	facts := []validations.TableColumns{{Table: "orders"}}
	_, err := columnMetadataFromFacts([]string{"orders"}, facts)
	if err == nil || !strings.Contains(err.Error(), "orders") {
		t.Fatalf("want fail-closed error naming orders, got %v", err)
	}
}

func TestQuotedColumnList(t *testing.T) {
	got := quotedColumnList([]string{"id", "payload"})
	if want := "`id`, `payload`"; got != want {
		t.Fatalf("quotedColumnList = %q, want %q", got, want)
	}
}

func TestColumnMetadataFromFacts(t *testing.T) {
	facts := []validations.TableColumns{{Table: "t", Columns: []validations.ColumnInfo{{Name: "id", DataType: "bigint"}, {Name: "D", DataType: "DATE", Invisible: true}, {Name: "dt", DataType: "datetime"}, {Name: "ts", DataType: "timestamp"}, {Name: "tm", DataType: "time"}, {Name: "yr", DataType: "year"}}}}
	got, err := columnMetadataFromFacts([]string{"t"}, facts)
	if err != nil {
		t.Fatal(err)
	}
	want := types.ColumnMetadata{Names: []string{"id", "D", "dt", "ts", "tm", "yr"}, Temporal: map[string]types.TemporalKind{"D": types.TemporalDate, "dt": types.TemporalDateTime, "ts": types.TemporalTimestamp}}
	if !reflect.DeepEqual(got["t"], want) {
		t.Fatalf("metadata=%+v want=%+v", got["t"], want)
	}
	facts[0].Columns[0].DataType = ""
	if _, err := columnMetadataFromFacts([]string{"t"}, facts); err == nil || !strings.Contains(err.Error(), "TEMPORAL_READ_CONTRACT") {
		t.Fatalf("incomplete datatype accepted: %v", err)
	}
}
