//go:build !windows

package harness

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The test estate has two topologies (tests/README.md → The group estate). The plain one is db1 to db3
// and is what every gate has run on; the group one is gr1 to gr3, a destination and a replica. Both are
// started, stopped and reset through tests/scripts/estate.sh, which `make test-up`, `test-down`,
// `test-status` and `test-reset` call. These tests run the script against a fake `docker` on the PATH that
// records each call and answers `docker ps` from a list, so no container is started or touched.

// estateTree copies the named files of tests/ into a new temporary directory called tests, and returns it.
func estateTree(t *testing.T, root string, files ...string) string {
	t.Helper()
	tests := filepath.Join(t.TempDir(), "tests")
	for _, f := range files {
		b, err := os.ReadFile(filepath.Join(root, "tests", f))
		if err != nil {
			t.Fatal(err)
		}
		dst := filepath.Join(tests, f)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dst, b, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return tests
}

// estateRun runs estate.sh with the given topology ("" leaves TOPOLOGY unset) and running containers. It
// returns the recorded docker calls, one "<directory>|<arguments>" line each, the script's output and its
// exit code.
func estateRun(t *testing.T, topology, running string, args ...string) (calls []string, output string, code int) {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Fatal("bash is needed to run the estate script")
	}
	// The scripts run from a copy of tests/ made here: estate.sh creates tests/.env.group from its example
	// when it is missing, and a test must not leave that in the checkout.
	tests := estateTree(t, root, "scripts/estate.sh", "scripts/topology.sh", "dot.env.group")
	bin := t.TempDir()
	log := filepath.Join(t.TempDir(), "docker.log")
	fake := `#!/usr/bin/env bash
printf '%s|%s\n' "$(basename "$PWD")" "$*" >> "$FAKE_DOCKER_LOG"
case "$1" in
  ps) printf '%s\n' $FAKE_DOCKER_RUNNING ;;
  compose) exit 0 ;;
  *) echo "fake docker: unexpected call: $*" >&2; exit 99 ;;
esac
`
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte(fake), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bash, append([]string{filepath.Join(tests, "scripts/estate.sh")}, args...)...)
	cmd.Env = []string{
		"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"),
		"HOME=" + t.TempDir(),
		"FAKE_DOCKER_LOG=" + log,
		"FAKE_DOCKER_RUNNING=" + running,
	}
	if topology != "" {
		cmd.Env = append(cmd.Env, "TOPOLOGY="+topology)
	}
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err = cmd.Run()
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("estate.sh did not run: %v", err)
	}
	if b, rerr := os.ReadFile(log); rerr == nil {
		calls = strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	}
	return calls, out.String(), code
}

// composeCalls are the recorded docker calls that are not the `docker ps` that looks for the other estate.
func composeCalls(calls []string) []string {
	var out []string
	for _, c := range calls {
		if !strings.Contains(c, "|ps --format") {
			out = append(out, c)
		}
	}
	return out
}

// TestEstatePlainIsWhatItWas: with TOPOLOGY unset the script does what the Makefile's four targets did
// before it existed. Each is one `docker compose` call run in tests/, with no file, project or env-file
// argument, so the plain estate keeps its project name and reads tests/.env as it always did.
func TestEstatePlainIsWhatItWas(t *testing.T) {
	for _, tc := range []struct {
		action string
		want   []string
	}{
		{"up", []string{"tests|compose up -d"}},
		{"down", []string{"tests|compose down"}},
		{"status", []string{"tests|compose ps"}},
		{"reset", []string{"tests|compose down -v", "tests|compose up -d"}},
	} {
		t.Run(tc.action, func(t *testing.T) {
			for _, topology := range []string{"", "plain"} {
				calls, out, code := estateRun(t, topology, "", tc.action)
				if code != 0 {
					t.Fatalf("TOPOLOGY=%q %s exited %d\n%s", topology, tc.action, code, out)
				}
				if got := composeCalls(calls); strings.Join(got, "\n") != strings.Join(tc.want, "\n") {
					t.Errorf("TOPOLOGY=%q %s ran %q, want %q", topology, tc.action, got, tc.want)
				}
			}
		})
	}
}

// TestEstateGroupHasItsOwnFileAndEnvironment: the group topology runs `docker compose` on its own file with
// its own env file, never on the plain estate's, so `down -v` on one cannot reach the other's containers or
// volumes. `up` waits for the members' health checks, which is how the bootstrap is waited for.
func TestEstateGroupHasItsOwnFileAndEnvironment(t *testing.T) {
	const base = "tests|compose -f compose.group.yml --env-file "
	for _, tc := range []struct {
		action string
		want   []string
	}{
		{"up", []string{base + ".env.group up -d --wait --wait-timeout 300"}},
		{"down", []string{base + ".env.group down"}},
		{"status", []string{base + ".env.group ps"}},
		{"reset", []string{base + ".env.group down -v", base + ".env.group up -d --wait --wait-timeout 300"}},
	} {
		t.Run(tc.action, func(t *testing.T) {
			calls, out, code := estateRun(t, "group", "", tc.action)
			if code != 0 {
				t.Fatalf("TOPOLOGY=group %s exited %d\n%s", tc.action, code, out)
			}
			if got := composeCalls(calls); strings.Join(got, "\n") != strings.Join(tc.want, "\n") {
				t.Errorf("TOPOLOGY=group %s ran %q, want %q", tc.action, got, tc.want)
			}
		})
	}
}

// TestEstateRefusesWhileTheOtherIsUp (plan §2, review finding 8): bringing one estate up, or resetting it,
// is refused while a container of the other is running, found by the container's name and not by a port
// that happens to collide. Nothing is run against the compose files then. Stopping and listing an estate
// are never refused, and a container that only contains a name (db10, xgr1) is not that estate's.
func TestEstateRefusesWhileTheOtherIsUp(t *testing.T) {
	for _, tc := range []struct {
		topology, running, mentions string
	}{
		{"group", "db1", "db1"},
		{"group", "other db3 more", "db3"},
		{"plain", "gr1", "gr1"},
		{"plain", "gr-replica", "gr-replica"},
	} {
		for _, action := range []string{"up", "reset"} {
			calls, out, code := estateRun(t, tc.topology, tc.running, action)
			if code == 0 {
				t.Errorf("TOPOLOGY=%s %s with %q running was not refused\n%s", tc.topology, action, tc.running, out)
			}
			if !strings.Contains(out, tc.mentions) {
				t.Errorf("TOPOLOGY=%s %s: the refusal does not name the container %s\n%s", tc.topology, action, tc.mentions, out)
			}
			if got := composeCalls(calls); len(got) != 0 {
				t.Errorf("TOPOLOGY=%s %s ran compose after refusing: %q", tc.topology, action, got)
			}
		}
		for _, action := range []string{"down", "status"} {
			if _, out, code := estateRun(t, tc.topology, tc.running, action); code != 0 {
				t.Errorf("TOPOLOGY=%s %s with %q running exited %d, want it never refused\n%s", tc.topology, action, tc.running, code, out)
			}
		}
	}
	for _, tc := range []struct{ topology, running string }{
		{"group", "db10 xdb1 db1-old"},
		{"plain", "gr10 xgr1 gr1-old"},
		{"group", "gr1 gr2 gr3"},
		{"plain", "db1 db2 db3"},
	} {
		if _, out, code := estateRun(t, tc.topology, tc.running, "up"); code != 0 {
			t.Errorf("TOPOLOGY=%s up with %q running was refused, want it allowed\n%s", tc.topology, tc.running, out)
		}
	}
}

// TestEstateRejectsWhatItDoesNotKnow: an unknown topology or action is exit 2 and runs nothing.
func TestEstateRejectsWhatItDoesNotKnow(t *testing.T) {
	for _, tc := range []struct{ topology, action string }{{"galera", "up"}, {"", "destroy"}, {"group", ""}} {
		var args []string
		if tc.action != "" {
			args = []string{tc.action}
		}
		calls, out, code := estateRun(t, tc.topology, "", args...)
		if code != 2 {
			t.Errorf("TOPOLOGY=%q %q exited %d, want 2\n%s", tc.topology, tc.action, code, out)
		}
		if len(calls) != 0 {
			t.Errorf("TOPOLOGY=%q %q ran docker: %q", tc.topology, tc.action, calls)
		}
	}
}

// groupComposeFile reads tests/compose.group.yml.
func groupComposeFile(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("../compose.group.yml")
	if err != nil {
		t.Fatalf("the group estate's compose file: %v", err)
	}
	return string(b)
}

// envNames reads the variable names an env file defines.
func envNames(t *testing.T, path string) map[string]string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, line := range strings.Split(string(b), "\n") {
		if m := regexp.MustCompile(`^([A-Z][A-Z0-9_]*)=(.*)$`).FindStringSubmatch(line); m != nil {
			out[m[1]] = m[2]
		}
	}
	return out
}

// TestGroupEstateFilesAgree ties the group estate's four files together, so none can drift from another:
//   - the compose file has a project name of its own, and not the plain estate's (the directory, tests);
//   - its container names are the ones topology.sh lists, which estate.sh looks for when it refuses, and none is a plain estate name;
//   - its published ports are the ones the env example hands the suites, and all of them are clear of the
//     plain estate's and of the environment matrix's block (3375 to 3396);
//   - the env example defines everything the plain example does, and the two members the group-only tests
//     use.
func TestGroupEstateFilesAgree(t *testing.T) {
	compose := groupComposeFile(t)

	if m := regexp.MustCompile(`(?m)^name:\s*(\S+)\s*$`).FindStringSubmatch(compose); m == nil || m[1] == "tests" {
		t.Errorf("compose.group.yml has no project name of its own (found %v)", m)
	}

	var names []string
	for _, m := range regexp.MustCompile(`(?m)^\s*container_name:\s*(\S+)\s*$`).FindAllStringSubmatch(compose, -1) {
		names = append(names, m[1])
	}
	sort.Strings(names)
	script, err := os.ReadFile("../scripts/topology.sh")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^GROUP_CONTAINERS="([^"]*)"\s*$`).FindStringSubmatch(string(script))
	if m == nil {
		t.Fatal("topology.sh has no GROUP_CONTAINERS list")
	}
	listed := strings.Fields(m[1])
	sort.Strings(listed)
	if strings.Join(names, " ") != strings.Join(listed, " ") {
		t.Errorf("compose.group.yml names %v, topology.sh looks for %v", names, listed)
	}
	for _, n := range names {
		if n == "db1" || n == "db2" || n == "db3" {
			t.Errorf("the group estate has a container named %s, the plain estate's", n)
		}
	}

	var ports []int
	for _, m := range regexp.MustCompile(`"\$\{[A-Z0-9_]+:-(\d+)\}:3306"|"(\d+):3306"`).FindAllStringSubmatch(compose, -1) {
		p := m[1] + m[2]
		n, _ := strconv.Atoi(p)
		ports = append(ports, n)
	}
	if len(ports) != 5 {
		t.Errorf("compose.group.yml publishes %d server ports, want 5 (gr1 to gr3, the destination, the replica): %v", len(ports), ports)
	}
	plain := map[int]bool{3305: true, 3307: true, 3308: true, 33061: true, 33062: true, 33063: true}
	for _, p := range ports {
		if plain[p] || (p >= 3375 && p <= 3396) {
			t.Errorf("the group estate publishes port %d, which the plain estate or the environment matrix uses", p)
		}
	}

	env := envNames(t, "../dot.env.group")
	for name := range envNames(t, "../dot.env") {
		if _, ok := env[name]; !ok {
			t.Errorf("dot.env.group lacks %s, which dot.env defines", name)
		}
	}
	for _, role := range []string{"2", "3"} {
		for _, f := range []string{"HOST", "PORT", "USER", "PASSWORD"} {
			if _, ok := env["TEST_GROUP_MEMBER"+role+"_"+f]; !ok {
				t.Errorf("dot.env.group lacks TEST_GROUP_MEMBER%s_%s", role, f)
			}
		}
	}
	want := map[string]int{"TEST_SOURCE_PORT": 0, "TEST_GROUP_MEMBER2_PORT": 1, "TEST_GROUP_MEMBER3_PORT": 2, "TEST_DEST_PORT": 3, "TEST_REPLICA_PORT": 4}
	for name, i := range want {
		got, _ := strconv.Atoi(env[name])
		if i >= len(ports) || got != ports[i] {
			t.Errorf("dot.env.group has %s=%s, but the compose file's server %d publishes %v", name, env[name], i, ports)
		}
	}
}

// TestRequireContainersUpChecksTheGroupEstate: with TOPOLOGY=group the estate check looks at the two other
// members too, and refuses an environment that is not the group's (the plain ports answer while the
// plain estate is up, so a check on them would pass against the wrong estate).
func TestRequireContainersUpChecksTheGroupEstate(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	run := func(topology string, env ...string) (probed []string, out string, code int) {
		dir := filepath.Join(estateTree(t, root, "scripts/require-containers-up.sh", "scripts/topology.sh"), "scripts")
		log := filepath.Join(t.TempDir(), "probed")
		stub := "#!/usr/bin/env bash\necho \"$1\" >> \"$PROBE_LOG\"\n[ \"$1\" != \"${DOWN_PORT:-}\" ]\n"
		if werr := os.WriteFile(filepath.Join(dir, "mysql-query.sh"), []byte(stub), 0o755); werr != nil {
			t.Fatal(werr)
		}
		cmd := exec.Command("bash", filepath.Join(dir, "require-containers-up.sh"))
		cmd.Env = append([]string{"PATH=" + os.Getenv("PATH"), "PROBE_LOG=" + log, "MYSQL_ROOT_PASSWORD=x"}, env...)
		if topology != "" {
			cmd.Env = append(cmd.Env, "TOPOLOGY="+topology)
		}
		var b bytes.Buffer
		cmd.Stdout, cmd.Stderr = &b, &b
		if rerr := cmd.Run(); rerr != nil {
			if ee, ok := rerr.(*exec.ExitError); ok {
				code = ee.ExitCode()
			} else {
				t.Fatal(rerr)
			}
		}
		if data, rerr := os.ReadFile(log); rerr == nil {
			probed = strings.Fields(string(data))
		}
		return probed, b.String(), code
	}

	groupEnv := []string{"TEST_SOURCE_PORT=3410", "TEST_GROUP_MEMBER2_PORT=3411", "TEST_GROUP_MEMBER3_PORT=3412", "TEST_DEST_PORT=3413", "TEST_REPLICA_PORT=3414"}
	probed, out, code := run("group", groupEnv...)
	sort.Strings(probed)
	if code != 0 || strings.Join(probed, " ") != "3410 3411 3412 3413 3414" {
		t.Errorf("TOPOLOGY=group checked %v (exit %d), want all five servers\n%s", probed, code, out)
	}
	if _, out, code = run("group", append([]string{"DOWN_PORT=3411"}, groupEnv...)...); code == 0 || !strings.Contains(out, "DOWN: member 2 (port 3411)") {
		t.Errorf("a member that does not answer is not reported by name (exit %d)\n%s", code, out)
	}

	if probed, out, code = run("group", "TEST_SOURCE_PORT=3305", "TEST_DEST_PORT=3307", "TEST_REPLICA_PORT=3308"); code == 0 || len(probed) != 0 {
		t.Errorf("TOPOLOGY=group with the plain estate's environment checked %v (exit %d), want a refusal before any probe\n%s", probed, code, out)
	} else if !strings.Contains(out, ".env.group") {
		t.Errorf("the refusal does not name tests/.env.group\n%s", out)
	}

	probed, out, code = run("", "TEST_SOURCE_PORT=3305", "TEST_DEST_PORT=3307", "TEST_REPLICA_PORT=3308")
	sort.Strings(probed)
	if code != 0 || strings.Join(probed, " ") != "3305 3307 3308" {
		t.Errorf("the plain estate's check probed %v (exit %d), want its three servers unchanged\n%s", probed, code, out)
	}
}

// TestMakeE2ERefusesTheGroupEstate: the E2E configs name the plain estate's ports, so `make e2e` with
// TOPOLOGY=group would reset the group estate and then run against ports that are not its own. It refuses
// first, exit 2, before any docker call.
func TestMakeE2ERefusesTheGroupEstate(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	make, err := exec.LookPath("make")
	if err != nil {
		t.Fatal("make is needed to run the Makefile's e2e target")
	}
	bin := t.TempDir()
	log := filepath.Join(t.TempDir(), "docker.log")
	// Nothing real can run here, so a Makefile without the refusal cannot reach an estate: docker and
	// mysqlsh are fakes, and the rest of the PATH is the system's.
	fake := "#!/usr/bin/env bash\necho \"$*\" >> \"$FAKE_DOCKER_LOG\"\n"
	for _, name := range []string{"docker", "mysqlsh"} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(fake+"exit 99\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command(make, "-C", root, "e2e", "TOPOLOGY=group")
	cmd.Env = []string{"PATH=" + bin + string(os.PathListSeparator) + "/usr/bin:/bin", "HOME=" + t.TempDir(), "FAKE_DOCKER_LOG=" + log}
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err = cmd.Run()
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 2 {
		t.Errorf("make e2e TOPOLOGY=group = %v, want exit 2\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "group estate has no E2E") {
		t.Errorf("the refusal does not say why\n%s", out.String())
	}
	if b, rerr := os.ReadFile(log); rerr == nil {
		t.Errorf("make e2e TOPOLOGY=group called docker before refusing: %s", b)
	}
}
