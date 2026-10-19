package engine

import (
	"slices"
	"testing"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"

	"github.com/dbsmedya/goarchive/v3/internal/types"
)

// TestColumnMetadataMarksJSON (JSN-1): the library's DATA_TYPE marks a JSON column, in select
// order, whatever its case; a text column is not one, and JSON is not temporal.
func TestColumnMetadataMarksJSON(t *testing.T) {
	meta, err := columnMetadataFromFacts([]string{"jt"}, []validations.TableColumns{{Table: "jt", Columns: []validations.ColumnInfo{
		{Name: "id", DataType: "int"}, {Name: "j", DataType: "json"}, {Name: "note", DataType: "text"}, {Name: "J2", DataType: "JSON"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if got := meta["jt"].JSON; !slices.Equal(got, []string{"j", "J2"}) {
		t.Errorf("JSON columns = %v, want [j J2]", got)
	}
	if k := meta["jt"].Kind("j"); k != types.TemporalNone {
		t.Errorf("Kind(j) = %v, want TemporalNone", k)
	}
}
