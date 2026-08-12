package archiver

import (
	"testing"
)

func TestBuildSelectColumnsQuery(t *testing.T) {
	got := buildSelectColumnsQuery("orders", "id", []string{"id", "payload"}, 2)
	want := "SELECT `id`, `payload` FROM `orders` WHERE `id` IN (?, ?)"
	if got != want {
		t.Fatalf("buildSelectColumnsQuery = %q, want %q", got, want)
	}
}
