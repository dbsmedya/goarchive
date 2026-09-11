//go:build integration

package archiver

import (
	"testing"
	"time"
)

func TestIntegrationHarnessTemporalDecode_Integration(t *testing.T) {
	setup, _ := SetupIntegrationTest(t)
	t.Cleanup(setup.Close)

	for _, name := range []string{"source", "destination"} {
		db, ok := setup.GetDB(name)
		if !ok { t.Fatalf("%s database missing from integration setup", name) }
		var got any
		if err := db.QueryRow("SELECT CAST('2024-01-02 03:04:05' AS DATETIME)").Scan(&got); err != nil {
			t.Fatalf("%s datetime query: %v", name, err)
		}
		value, ok := got.(time.Time)
		if !ok {
			t.Fatalf("HARNESS_PARSE_TIME_DISABLED: %s DATETIME decoded as %T, want time.Time", name, got)
		}
		want := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
		if !value.Equal(want) { t.Fatalf("%s DATETIME = %s, want %s", name, value, want) }
	}
}
