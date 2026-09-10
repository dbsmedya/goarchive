package archiver

import (
	"github.com/dbsmedya/goarchive/internal/types"
	"testing"
)

func TestBuildSelectColumnsQuery(t *testing.T) {
	got, err := buildSelectColumnsQuery("orders", "id", types.ColumnMetadata{Names: []string{"id", "payload"}, Temporal: map[string]types.TemporalKind{}}, 2)
	want := "SELECT `id`, `payload` FROM `orders` WHERE `id` IN (?, ?)"
	if err != nil || got != want {
		t.Fatalf("buildSelectColumnsQuery = %q, want %q", got, want)
	}
}
