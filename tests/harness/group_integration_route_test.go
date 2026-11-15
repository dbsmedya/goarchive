package harness

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The group estate's integration packages run one at a time (tests/README.md, The group estate): a primary switch, a lowered
// size limit or a read-only destination changes what every other package's tests write to, and the base check at a test's
// entry cannot protect it from another package's change afterwards. So every runner path that launches the Go integration
// packages adds `-p 1` for TOPOLOGY=group, and the plain route's invocation stays as it was: the gate runs through it
// (Codex batch 1, finding 1; GR review fixes).

const (
	plainRoute = "go test -v -tags=integration -count=1 ./internal/... ./cmd/..."
	groupRoute = "go test -v -p 1 -tags=integration -count=1 ./internal/... ./cmd/..."
)

// generatedGoCommand sources the real golayer.sh with the Go layer's runner and `go` replaced by functions that print what
// they are called with, as Codex probed it, and returns the invocation run_integration_tests makes.
func generatedGoCommand(t *testing.T, topology, goTestArgs string) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	script := `set -u
PROJECT_ROOT="$DIR"; TESTS_DIR="$DIR"; VERBOSE=false; GO_TEST_ARGS="$GOARGS"
log_step() { :; }; log_info() { :; }; log_error() { :; }
. "$ROOT/tests/e2e/lib/golayer.sh"
# after the source: golayer.sh defines run_go_layer, which counts passes in a real run's output
run_go_layer() { shift; "$@"; }
go() { printf 'go'; printf ' %s' "$@"; printf '\n'; }
run_integration_tests
`
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", "-c", script)
	env := []string{"PATH=" + os.Getenv("PATH"), "ROOT=" + root, "DIR=" + dir, "GOARGS=" + goTestArgs}
	if topology != "" {
		env = append(env, "TOPOLOGY="+topology)
	}
	cmd.Env = env
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		t.Fatalf("run_integration_tests: %v\n%s", err, errb.String())
	}
	return strings.TrimSpace(out.String())
}

// TestTheRunnersGroupRouteRunsOnePackageAtATime: golayer.sh's run_integration_tests, which tests/scripts/run-tests.sh
// reaches, adds -p 1 for the group topology, whichever packages it is given, and leaves the plain route byte for byte as
// it was, with the topology named or not.
func TestTheRunnersGroupRouteRunsOnePackageAtATime(t *testing.T) {
	for _, tc := range []struct {
		name, topology, args, want string
	}{
		{"plain, named", "plain", "", plainRoute},
		{"plain, not named", "", "", plainRoute},
		{"group", "group", "", groupRoute},
		{"group, with the packages given", "group", "./internal/engine ./cmd/goarchive/cmd", "go test -v -p 1 -tags=integration -count=1 ./internal/engine ./cmd/goarchive/cmd"},
		{"plain, with the packages given", "plain", "./internal/engine", "go test -v -tags=integration -count=1 ./internal/engine"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := generatedGoCommand(t, tc.topology, tc.args); got != tc.want {
				t.Errorf("run_integration_tests ran\n got %q\nwant %q", got, tc.want)
			}
		})
	}
}

// TestMakeTestIntegrationRunsTheGroupEstateOnePackageAtATime: the same rule for `make test-integration`, which takes the
// topology from the environment or the command line as the other targets do. Printed with -n, so nothing runs.
func TestMakeTestIntegrationRunsTheGroupEstateOnePackageAtATime(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	const (
		plain = "go test -tags=integration ./internal/... ./cmd/... -count=1"
		group = "go test -p 1 -tags=integration ./internal/... ./cmd/... -count=1"
	)
	for _, tc := range []struct {
		name string
		args []string
		env  []string
		want string
	}{
		{"plain by default", nil, nil, plain},
		{"plain, named on the command line", []string{"TOPOLOGY=plain"}, nil, plain},
		{"group, on the command line", []string{"TOPOLOGY=group"}, nil, group},
		{"group, from the environment", nil, []string{"TOPOLOGY=group"}, group},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "make", append([]string{"-n", "-C", root, "test-integration"}, tc.args...)...)
			cmd.Env = append([]string{"PATH=" + os.Getenv("PATH"), "TEST_SOURCE_HOST=h", "TEST_DEST_HOST=h"}, tc.env...)
			var out, errb bytes.Buffer
			cmd.Stdout, cmd.Stderr = &out, &errb
			if err := cmd.Run(); err != nil {
				t.Fatalf("make -n test-integration: %v\n%s", err, errb.String())
			}
			var goLine string
			for _, line := range strings.Split(out.String(), "\n") {
				if strings.HasPrefix(strings.TrimSpace(line), "go test") {
					goLine = strings.TrimSpace(line)
				}
			}
			if goLine != tc.want {
				t.Errorf("make test-integration would run\n got %q\nwant %q\n(output:\n%s)", goLine, tc.want, out.String())
			}
		})
	}
}
