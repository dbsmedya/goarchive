package archiver

import (
	"github.com/dbsmedya/goarchive/internal/types"
	"testing"
)

// Both sampling branches must produce explicit, quoted lists in the order the
// caller supplies (ordinal order from columnMetadataFromFacts). An invisible
// column is just another name in the list — "hidden" here stands in for one;
// inclusion itself is the projection's job, proven in Task 1's tests and the
// Task 7 round-trips.
func TestBuildSampleQueryRootWithWhere(t *testing.T) {
	got, err := buildSampleQuery("orders", "id", types.ColumnMetadata{Names: []string{"id", "payload", "hidden"}, Temporal: map[string]types.TemporalKind{}},
		"created_at < '2020-01-01'", 100)
	want := "SELECT `id`, `payload`, `hidden` FROM `orders` WHERE (created_at < '2020-01-01') ORDER BY `id` ASC LIMIT 100"
	if err != nil || got != want {
		t.Fatalf("buildSampleQuery(root) = %q, want %q", got, want)
	}
}

func TestBuildSampleQueryUnfiltered(t *testing.T) {
	got, err := buildSampleQuery("orders", "id", types.ColumnMetadata{Names: []string{"id", "payload", "hidden"}, Temporal: map[string]types.TemporalKind{}}, "", 50)
	want := "SELECT `id`, `payload`, `hidden` FROM `orders` LIMIT 50"
	if err != nil || got != want {
		t.Fatalf("buildSampleQuery(unfiltered) = %q, want %q", got, want)
	}
}
