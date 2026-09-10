package verifier

import (
	"github.com/dbsmedya/goarchive/internal/types"
	"testing"
)

// Preserves the existing query shape exactly (comma-joined placeholders, ORDER
// BY pk) with the * replaced by explicit names — the same list is used against
// source AND destination, so an INVISIBLE column on either side is still read.
func TestBuildHashQuery(t *testing.T) {
	got, err := buildHashQuery("orders", "id", types.ColumnMetadata{Names: []string{"id", "payload"}, Temporal: map[string]types.TemporalKind{}}, 2)
	want := "SELECT `id`, `payload` FROM `orders` WHERE `id` IN (?,?) ORDER BY `id`"
	if err != nil || got != want {
		t.Fatalf("buildHashQuery = %q, want %q", got, want)
	}
}
