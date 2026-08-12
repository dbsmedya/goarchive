package archiver

import (
	"reflect"
	"strings"
	"testing"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
)

// The projection must include every column — invisible and generated alike —
// in ordinal order. Filtering here is exactly the defect issue #23 fixes.
func TestColumnListsFromFactsIncludesInvisibleAndGenerated(t *testing.T) {
	facts := []validations.TableColumns{
		{Table: "orders", Columns: []validations.ColumnInfo{
			{Name: "id", Ordinal: 1},
			{Name: "payload", Ordinal: 2, Invisible: true},
			{Name: "doubled", Ordinal: 3, Invisible: true, Generated: true},
		}},
		{Table: "items", Columns: []validations.ColumnInfo{
			{Name: "id", Ordinal: 1},
		}},
	}
	lists, err := columnListsFromFacts([]string{"orders", "items"}, facts)
	if err != nil {
		t.Fatalf("columnListsFromFacts: %v", err)
	}
	if want := []string{"id", "payload", "doubled"}; !reflect.DeepEqual(lists["orders"], want) {
		t.Fatalf("orders columns = %v, want %v", lists["orders"], want)
	}
	if want := []string{"id"}; !reflect.DeepEqual(lists["items"], want) {
		t.Fatalf("items columns = %v, want %v", lists["items"], want)
	}
}

// A graph table absent from the facts must fail closed, naming the table,
// before any data moves.
func TestColumnListsFromFactsMissingTableFailsClosed(t *testing.T) {
	facts := []validations.TableColumns{
		{Table: "orders", Columns: []validations.ColumnInfo{{Name: "id", Ordinal: 1}}},
	}
	_, err := columnListsFromFacts([]string{"orders", "ghost"}, facts)
	if err == nil || !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("want fail-closed error naming ghost, got %v", err)
	}
}

// A present table with zero columns is equally fail-closed (cannot occur on a
// real server; guards a broken facts source).
func TestColumnListsFromFactsEmptyColumnsFailsClosed(t *testing.T) {
	facts := []validations.TableColumns{{Table: "orders"}}
	_, err := columnListsFromFacts([]string{"orders"}, facts)
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
