package harness

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// The tests Matrix verify skips (tests/envmatrix/gh-skip.txt, MX_SKIP_FILE in matrix-verify.yml). A matrix row runs only its
// source and destination pair, so a test that opens the estate's replica (db3, TEST_REPLICA on 3308) fails there with
// `dial tcp 127.0.0.1:3308: connect: connection refused`. Matrix verify run 44005058337 (3.1's first run: the 3.1 tests had
// never run in it) failed 34 of 35 rows on the same 19 tests. They still run in the local matrix and in every gate job.

// replicaTests are the 19, from that run's artifacts: each failed in every failing row with the 3308 dial and nothing else.
var replicaTests = []string{
	"TestMySQLReplicaLockIsExclusive", "TestMySQLReplicaLockIsHeldOncePerTopLevelTest", "TestMySQLReplicaStateNamesWhatDiffers",
	"TestMySQLControlDeleteSizeDuringHold", "TestMySQLControlDeleteSizeRecovery", "TestMySQLControlToleranceLowered",
	"TestMySQLControlToleranceDuringSleep", "TestMySQLControlToleranceDuringSleepRecovery", "TestMySQLControlToleranceRaised",
	"TestMySQLDeleteSleepStop", "TestMySQLAReadOnlySourceIsRefusedBeforeRecovery",
	"TestMySQLADestinationThatIsReadOnlyRefusesEveryJobType", "TestMySQLAReadOnlyReplicaAsSourceRefusesAnArchiveAndAPurge",
	"TestMySQLControlStatus", "TestMySQLControlAnAsynchronousGateHasNoGroupBounds", "TestMySQLControlStopDuringHold",
	"TestMySQLControlRecoveringStatus", "TestMySQLControlToleranceRaisedSocket", "TestMySQLAPlainSourceWithReplicationGetsNoBanner",
}

// skipEntries reads gh-skip.txt as run-matrix.sh does: the first word of every line that is not a comment is a name, and
// what follows its # is the reason.
func skipEntries(t *testing.T) map[string]string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "envmatrix", "gh-skip.txt"))
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, rest, _ := strings.Cut(line, " ")
		out[name] = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(rest), "#"))
	}
	return out
}

// testNames is every top-level test function in the repository's Go test files.
func testNames(t *testing.T) []string {
	t.Helper()
	root := filepath.Join("..", "..")
	re := regexp.MustCompile(`(?m)^func (Test\w+)\(t \*testing\.T\)`)
	var names []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", ".ayder", "bin", "results", "sakila-db", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		for _, m := range re.FindAllStringSubmatch(string(b), -1) {
			names = append(names, m[1])
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return names
}

// TestMatrixSkipsTheTestsThatNeedTheEstateReplica: every one of the 19 is listed with a reason, and the list holds nothing
// that is not a real test. The names are regular expressions in `go test -skip`, matched anywhere in a test's name, so a name
// that is part of another test's name skips that test too: the only such pairs allowed are inside the list.
func TestMatrixSkipsTheTestsThatNeedTheEstateReplica(t *testing.T) {
	entries := skipEntries(t)
	names := testNames(t)
	for _, want := range replicaTests {
		reason, ok := entries[want]
		if !ok {
			t.Errorf("gh-skip.txt does not skip %s, which opens the estate's replica", want)
			continue
		}
		if !strings.Contains(reason, "3308") && !strings.Contains(reason, "replica") {
			t.Errorf("%s: the reason %q does not say that it needs the estate's replica", want, reason)
		}
	}
	for name, reason := range entries {
		if !slices.Contains(names, name) {
			t.Errorf("gh-skip.txt names %s, which is no test of the repository", name)
		}
		if reason == "" {
			t.Errorf("gh-skip.txt names %s without a reason", name)
		}
		re := regexp.MustCompile(name)
		for _, other := range names {
			if other != name && re.MatchString(other) {
				if _, listed := entries[other]; !listed {
					t.Errorf("the skip %s also skips %s, which is not listed", name, other)
				}
			}
		}
	}
}

// TestOnlyMatrixVerifyReadsTheSkipList: the gate, CI and the local runners never read gh-skip.txt, so the gate's counts are
// whatever they are without it. It is read through MX_SKIP_FILE, which only matrix-verify.yml sets and only run-matrix.sh reads.
func TestOnlyMatrixVerifyReadsTheSkipList(t *testing.T) {
	root := filepath.Join("..", "..")
	for _, f := range []string{
		".github/workflows/gate-verify.yml", ".github/workflows/ci.yml", ".github/workflows/docker.yml", ".github/workflows/release.yml",
		"tests/scripts/run-gate.sh", "tests/scripts/run-tests.sh", "tests/scripts/gate-start.sh", "Makefile",
	} {
		b, err := os.ReadFile(filepath.Join(root, f))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), "MX_SKIP_FILE") || strings.Contains(string(b), "gh-skip") {
			t.Errorf("%s names the matrix's skip list", f)
		}
	}
	for f, want := range map[string]bool{".github/workflows/matrix-verify.yml": true, "tests/envmatrix/run-matrix.sh": true} {
		b, err := os.ReadFile(filepath.Join(root, f))
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Contains(string(b), "MX_SKIP_FILE"); got != want {
			t.Errorf("%s names MX_SKIP_FILE = %v, want %v", f, got, want)
		}
	}
}
