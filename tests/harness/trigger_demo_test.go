package harness

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// demoResult is one run of run_e2e_test on a validation-demo declaration, with the
// goarchive binary and the account helpers replaced by recorders.
type demoResult struct {
	rc            int
	stderr, calls string // calls: goarchive's arguments, then account events, one per line
}

// runDemo runs run_e2e_test 18 on decl, against a fake goarchive that prints out and exits
// with code, and a config whose source user is user.
func runDemo(t *testing.T, decl, user, out string, code int) demoResult {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	writeFixture(t, filepath.Join(dir, "declaration.sh"), decl)
	writeFixture(t, filepath.Join(dir, "configs/demo.yaml"), "source:\n  host: 127.0.0.1\n  user: "+user+"\ndestination:\n  user: root\n")
	writeFixture(t, filepath.Join(dir, "goarchive"), fmt.Sprintf("#!/bin/bash\nprintf 'goarchive %%s\\n' \"$*\" >> %q\nprintf '%%s\\n' %q\nexit %d\n",
		filepath.Join(dir, "calls"), out, code))
	if err := os.Chmod(filepath.Join(dir, "goarchive"), 0o755); err != nil {
		t.Fatal(err)
	}
	script := `source "$DEMO_ROOT/tests/e2e/lib/engine.sh"
TESTS_DIR="$DEMO_DIR"
mkdir -p "$TESTS_DIR/results"
SOURCE_HOST=source SOURCE_PORT=3305 SOURCE_USER=root SOURCE_DB=sakila
ARCHIVE_HOST=dest ARCHIVE_PORT=3307 ARCHIVE_USER=root ARCHIVE_DB=sakila_archive
GOARCHIVE_BIN="$DEMO_DIR/goarchive"
registry_file_for() { printf '%s\n' "$TESTS_DIR/declaration.sh"; }
registry_category_for_file() { echo validation; }
log_error() { printf '%s\n' "$*" >&2; }
log_info() { :; }
log_header() { :; }
reset_source_database() { echo reset >> "$TESTS_DIR/calls"; }
get_row_count() { echo 1; }
ensure_goarchive_bin() { return 0; }
create_source_account() { echo "create $1" >> "$TESTS_DIR/calls"; }
drop_source_account() { echo drop >> "$TESTS_DIR/calls"; }
run_e2e_test 18
rc=$?
if [ -n "${source_account:-}${force_triggers:-}${expected_detail:-}" ]; then echo DEMO_VARIABLE_LEAK >&2; exit 91; fi
exit "$rc"
`
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", "-c", script)
	cmd.Env = append(os.Environ(), "DEMO_ROOT="+root, "DEMO_DIR="+dir)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err = cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("demo harness timed out: %v", ctx.Err())
	}
	rc := 0
	if e, ok := err.(*exec.ExitError); ok {
		rc = e.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	calls, _ := os.ReadFile(filepath.Join(dir, "calls"))
	return demoResult{rc, stderr.String(), string(calls)}
}

const demoDecl = "test_name=TestDemo\nconfig_file=demo.yaml\ntables=film\nmode=example\nexpected_error=THE_CHECK\n"

// TestE2EDemoTriggerFlag (TRG-1): a validation demo's validate takes --force-triggers
// unless the demo declares force_triggers="no".
func TestE2EDemoTriggerFlag(t *testing.T) {
	for _, tc := range []struct {
		name, decl string
		force      bool
	}{
		{"default", demoDecl, true},
		{"yes", demoDecl + "force_triggers=yes\n", true},
		{"no", demoDecl + "force_triggers=no\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := runDemo(t, tc.decl, "root", "FAILED: THE_CHECK", 1)
			if r.rc != 0 {
				t.Fatalf("rc=%d\n%s\n%s", r.rc, r.calls, r.stderr)
			}
			if got := strings.Contains(r.calls, "--force-triggers"); got != tc.force {
				t.Errorf("validate %q: --force-triggers=%t, want %t", r.calls, got, tc.force)
			}
		})
	}
}

// TestE2EDemoSourceAccount (TRG-1): a demo that declares source_account has the account
// created before validate and dropped after it, whether the demo passes or fails.
func TestE2EDemoSourceAccount(t *testing.T) {
	decl := demoDecl + "source_account=recipe-trigger\n"
	for _, tc := range []struct {
		name, out string
		code, rc  int
	}{
		{"refused as expected", "FAILED: THE_CHECK", 1, 0},
		{"validate passed", "PASSED", 0, 1},
		{"wrong category", "FAILED: OTHER_CHECK", 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := runDemo(t, decl, "ga3e2e_recipe", tc.out, tc.code)
			if r.rc != tc.rc {
				t.Fatalf("rc=%d, want %d\n%s\n%s", r.rc, tc.rc, r.calls, r.stderr)
			}
			create := strings.Index(r.calls, "create recipe-trigger\n")
			validate := strings.Index(r.calls, "goarchive validate")
			drop := strings.Index(r.calls, "drop\n")
			if create < 0 || validate < create || drop < validate {
				t.Errorf("calls = %q, want create recipe-trigger, validate, drop in that order", r.calls)
			}
		})
	}
}

// TestE2EDemoExpectedDetail (TRG-1): expected_detail is a second, literal substring the
// failure must hold.
func TestE2EDemoExpectedDetail(t *testing.T) {
	decl := demoDecl + "expected_detail='film(del_film)'\n"
	if r := runDemo(t, decl, "root", "FAILED: THE_CHECK (tables: [film(del_film)])", 1); r.rc != 0 {
		t.Errorf("detail present: rc=%d\n%s", r.rc, r.stderr)
	}
	if r := runDemo(t, decl, "root", "FAILED: THE_CHECK (tables: [film(unconfirmed)])", 1); r.rc == 0 {
		t.Error("detail absent: the demo passed")
	}
}

// TestE2EDemoDeclarationGuards (TRG-1): the demo declarations fail closed before anything
// runs: outside mode=example, with an unknown value, or with a config whose source user is
// not the account the demo creates.
func TestE2EDemoDeclarationGuards(t *testing.T) {
	working := "test_name=TestDemo\nconfig_file=demo.yaml\ntables=film\nmode=working\ncommand=archive\nverify_method=none\nmin_duration=0.0\nexpected_rows=1\n"
	for _, tc := range []struct{ name, decl, user string }{
		{"source_account on a working test", working + "source_account=recipe\n", "ga3e2e_recipe"},
		{"force_triggers on a working test", working + "force_triggers=no\n", "root"},
		{"expected_detail on a working test", working + "expected_detail=x\n", "root"},
		{"unknown account", demoDecl + "source_account=admin\n", "ga3e2e_recipe"},
		{"unknown force value", demoDecl + "force_triggers=maybe\n", "root"},
		{"config still root", demoDecl + "source_account=recipe\n", "root"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := runDemo(t, tc.decl, tc.user, "FAILED: THE_CHECK", 1)
			if r.rc == 0 {
				t.Fatalf("the declaration was accepted\n%s", r.calls)
			}
			if r.calls != "" {
				t.Errorf("ran before refusing: %q", r.calls)
			}
		})
	}
}

// TestE2ESourceAccountSQL (TRG-1): the account helpers run, as root on the source, exactly
// the documented source recipe, TRIGGER only for recipe-trigger, and drop the account.
func TestE2ESourceAccountSQL(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, filepath.Join(dir, "mysql-query.sh"), "#!/bin/bash\nprintf '%s|%s\\n' \"$1\" \"$2\" >> \""+filepath.Join(dir, "sql")+"\"\n")
	if err := os.Chmod(filepath.Join(dir, "mysql-query.sh"), 0o755); err != nil {
		t.Fatal(err)
	}
	r := runLib(t, dir, `MYSQL_ROOT_PASSWORD=pw
create_source_account recipe && create_source_account recipe-trigger && drop_source_account`)
	if r.rc != 0 {
		t.Fatalf("rc=%d\n%s%s", r.rc, r.stdout, r.stderr)
	}
	lines := strings.Split(strings.TrimSpace(readFixture(t, filepath.Join(dir, "sql"))), "\n")
	if len(lines) != 3 {
		t.Fatalf("statements = %q, want three calls", lines)
	}
	for i, want := range []struct {
		has     []string
		trigger bool
	}{
		{[]string{"3305|", "DROP USER IF EXISTS 'ga3e2e_recipe'@'%'", "CREATE USER 'ga3e2e_recipe'@'%' IDENTIFIED BY 'pw'",
			"GRANT SELECT, DELETE ON `sakila`.* TO 'ga3e2e_recipe'@'%'", "GRANT PROCESS ON *.* TO 'ga3e2e_recipe'@'%'"}, false},
		{[]string{"GRANT SELECT, DELETE, TRIGGER ON `sakila`.*"}, true},
	} {
		for _, h := range want.has {
			if !strings.Contains(lines[i], h) {
				t.Errorf("call %d %q lacks %q", i, lines[i], h)
			}
		}
		if strings.Contains(lines[i], "TRIGGER ON") != want.trigger {
			t.Errorf("call %d %q: TRIGGER granted = %t, want %t", i, lines[i], !want.trigger, want.trigger)
		}
	}
	if !strings.Contains(lines[2], "DROP USER IF EXISTS 'ga3e2e_recipe'@'%'") {
		t.Errorf("drop = %q", lines[2])
	}
}
