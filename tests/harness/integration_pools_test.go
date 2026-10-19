package harness

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// rawPoolOpens are the calls that open a pool or a connector without database.OpenPool.
var rawPoolOpens = []string{"mysql.NewConnector(", "OpenConnector(", "sql.Open(", "sql.OpenDB("}

// allowedRawPools are the integration test files that open a raw pool on purpose, each with
// the reason. Removing a file from the list is a test change, not a change to the guard.
var allowedRawPools = map[string]string{
	"internal/database/auto_zero_integration_test.go":        "tests the AUTO_INCREMENT zero pin itself",
	"internal/database/sql_notes_integration_test.go":        "tests the sql_notes pin itself",
	"internal/database/session_pins_integration_test.go":     "tests the session pins themselves",
	"internal/database/wait_timeout_integration_test.go":     "tests the wait_timeout pin itself (rawPool)",
	"internal/engine/focus_integration_test.go":              "a Berlin-time pool for the engine's own UTC refusal",
	"internal/engine/diagnostics_integration_test.go":        "a session at max_error_count 0, which the connector would raise",
	"internal/engine/charset_session_integration_test.go":    "probes sessions the charset check would refuse",
	"internal/engine/trigger_visibility_integration_test.go": "a BuildDSN pool without the checks, on purpose",
	"internal/preflight/charsets_integration_test.go":        "probes sessions the charset check would refuse",
}

// TestIntegrationPoolsUseOpenPool (M7c-F6): every pool the package integration suites hand to
// product code is opened by database.OpenPool, the code goarchive opens its own with, so the
// matrix certifies the sessions the binary opens and the suites cannot drift from the pins.
// A file that opens a raw pool must be in allowedRawPools, and a listed file must still open
// one, so the list cannot rot. The list is by file: a raw open added to a listed file is not
// seen. The match is textual, so a comment or string naming one of rawPoolOpens counts too.
func TestIntegrationPoolsUseOpenPool(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	list, err := exec.Command("git", "-C", root, "ls-files", "*_integration_test.go").Output()
	if err != nil {
		t.Fatalf("git ls-files: %v", err)
	}
	files := strings.Fields(string(list))
	if len(files) == 0 {
		t.Fatal("no tracked integration test files")
	}
	for _, f := range files {
		body, err := os.ReadFile(filepath.Join(root, f))
		if err != nil {
			t.Fatal(err)
		}
		raw := slices.ContainsFunc(rawPoolOpens, func(n string) bool { return strings.Contains(string(body), n) })
		_, allowed := allowedRawPools[f]
		switch {
		case raw && !allowed:
			t.Errorf("%s opens a raw pool; open it through database.OpenPool, or add the file to allowedRawPools with its reason", f)
		case !raw && allowed:
			t.Errorf("%s is in allowedRawPools but opens no raw pool; remove it from the list", f)
		}
	}
	for f := range allowedRawPools {
		if !slices.Contains(files, f) {
			t.Errorf("%s is in allowedRawPools but is not a tracked integration test file", f)
		}
	}
}
