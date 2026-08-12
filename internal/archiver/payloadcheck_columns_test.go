package archiver

import "testing"

// Both sampling branches must produce explicit, quoted lists in the order the
// caller supplies (ordinal order from columnListsFromFacts). An invisible
// column is just another name in the list — "hidden" here stands in for one;
// inclusion itself is the projection's job, proven in Task 1's tests and the
// Task 7 round-trips.
func TestBuildSampleQueryRootWithWhere(t *testing.T) {
	got := buildSampleQuery("orders", "id", []string{"id", "payload", "hidden"},
		"created_at < '2020-01-01'", 100)
	want := "SELECT `id`, `payload`, `hidden` FROM `orders` WHERE (created_at < '2020-01-01') ORDER BY `id` ASC LIMIT 100"
	if got != want {
		t.Fatalf("buildSampleQuery(root) = %q, want %q", got, want)
	}
}

func TestBuildSampleQueryUnfiltered(t *testing.T) {
	got := buildSampleQuery("orders", "id", []string{"id", "payload", "hidden"}, "", 50)
	want := "SELECT `id`, `payload`, `hidden` FROM `orders` LIMIT 50"
	if got != want {
		t.Fatalf("buildSampleQuery(unfiltered) = %q, want %q", got, want)
	}
}
