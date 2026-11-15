package harness

import (
	"os"
	"path/filepath"
	"testing"
)

// Fixture helpers shared by the harness tests. They use nothing Unix-only, so they live apart from
// the gate tests (which run scripts in their own process group and are Unix-only): a test of another
// kind may use them on any platform that compiles it.

func writeFixture(t *testing.T, path, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), 0o755); err != nil {
		t.Fatal(err)
	}
}

func readFixture(t *testing.T, path string) string {
	t.Helper()
	b, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	return string(b)
}
