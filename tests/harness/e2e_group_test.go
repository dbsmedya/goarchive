//go:build !windows

package harness

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The E2E suite runs on both estates (GRG-1, GR C10): the configs name their servers' ports by
// the estate's own variables, and the limited source accounts of the validation demos can read the
// group's view. These tests run the harness's shell functions with no estate.

// renderConfigs renders every template of tests/configs in a temporary tests directory under env and
// returns the rendered file contents by name.
func renderConfigs(t *testing.T, env ...string) map[string]string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	templates, err := filepath.Glob(filepath.Join(root, "tests", "configs", "*.yaml.template"))
	if err != nil || len(templates) == 0 {
		t.Fatalf("no config templates: %v", err)
	}
	for _, p := range templates {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		writeFixture(t, filepath.Join(dir, "configs", filepath.Base(p)), string(b))
	}
	cmd := exec.Command("bash", "-c", `. "$E2E_ROOT/tests/e2e/lib/estate.sh"; TESTS_DIR="$E2E_DIR"; render_test_configs`)
	cmd.Env = append([]string{"PATH=" + os.Getenv("PATH"), "E2E_ROOT=" + root, "E2E_DIR=" + dir, "MYSQL_ROOT_PASSWORD=pw"}, env...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		t.Fatalf("render_test_configs: %v\n%s", err, out.String())
	}
	rendered := map[string]string{}
	for _, p := range templates {
		name := strings.TrimSuffix(filepath.Base(p), ".template")
		rendered[name] = readFixture(t, filepath.Join(dir, "configs", name))
	}
	return rendered
}

// TestE2EConfigsTakeTheirPortsFromTheEstate: no config template carries a server's port as a literal,
// so the same templates run on the plain estate (3305, 3307, 3308) and on the group estate (the ports of
// tests/.env.group). Rendered with no port variable a template is what it always was; rendered with the
// group's, every port is the group's and nothing else changed (the rendered files differ only in the
// three ports).
func TestE2EConfigsTakeTheirPortsFromTheEstate(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	templates, _ := filepath.Glob(filepath.Join(root, "tests", "configs", "*.yaml.template"))
	literal := regexp.MustCompile(`(?m)^[^#\n]*\bport:\s*33\d\d\b`)
	for _, p := range templates {
		if m := literal.FindString(readFixture(t, p)); m != "" {
			t.Errorf("%s names a port as a literal: %q", filepath.Base(p), strings.TrimSpace(m))
		}
	}

	plain := renderConfigs(t)
	group := renderConfigs(t, "TEST_SOURCE_PORT=3410", "TEST_DEST_PORT=3413", "TEST_REPLICA_PORT=3414")
	port := regexp.MustCompile(`(?m)^\s*port:\s*(\d+)\b`)
	ports := func(m map[string]string, allowed ...string) {
		t.Helper()
		for name, text := range m {
			found := port.FindAllStringSubmatch(text, -1)
			if len(found) == 0 {
				t.Errorf("%s has no port", name)
			}
			for _, f := range found {
				ok := false
				for _, a := range allowed {
					ok = ok || f[1] == a
				}
				if !ok {
					t.Errorf("%s rendered with the port %s, want one of %v", name, f[1], allowed)
				}
			}
		}
	}
	ports(plain, "3305", "3307", "3308")
	ports(group, "3410", "3413", "3414")
	back := strings.NewReplacer("port: 3410", "port: 3305", "port: 3413", "port: 3307", "port: 3414", "port: 3308")
	for name, text := range group {
		if got := back.Replace(text); got != plain[name] {
			t.Errorf("%s: the group's rendering differs from the plain one in more than its ports", name)
		}
	}
}

// TestE2ECreateSourceAccountGrantsTheGroupView: the validation demos run as a limited source account
// (docs/README_PERMISSIONS.md, the source recipe). On a group's member that account also needs SELECT on
// the view of the group, or preflight refuses it for the view before the check the demo is about
// (SOURCE_GROUP_VIEW_PERMISSION_CHECK). The grant is the group estate's only: the plain source is no
// member, and its recipe is the documented one.
func TestE2ECreateSourceAccountGrantsTheGroupView(t *testing.T) {
	const grant = "GRANT SELECT ON performance_schema.replication_group_members TO 'ga3e2e_recipe'@'%'"
	for _, tc := range []struct {
		topology string
		want     bool
	}{{"group", true}, {"plain", false}, {"", false}} {
		dir := t.TempDir()
		fakeQuery(t, dir)
		r := runLib(t, dir, "MYSQL_ROOT_PASSWORD=pw; SOURCE_DB=sakila; TOPOLOGY="+tc.topology+"; create_source_account recipe")
		if r.rc != 0 {
			t.Fatalf("TOPOLOGY=%q: rc=%d\n%s", tc.topology, r.rc, r.stderr)
		}
		sql := readFixture(t, filepath.Join(dir, "sql"))
		if got := strings.Contains(sql, grant); got != tc.want {
			t.Errorf("TOPOLOGY=%q: the grant of the group's view is present = %v, want %v:\n%s", tc.topology, got, tc.want, sql)
		}
		for _, w := range []string{"GRANT SELECT, DELETE ON `sakila`.*", "GRANT PROCESS ON *.*"} {
			if !strings.Contains(sql, w) {
				t.Errorf("TOPOLOGY=%q: the recipe lacks %q:\n%s", tc.topology, w, sql)
			}
		}
	}
}
