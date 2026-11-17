package harness

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// Gate verify runs the gate on the group estate too (GRG-1, GR C10; spec §9): one image per series, three
// jobs for each part, beside the six images' twelve. These tests read .github/workflows/gate-verify.yml as
// data, since a workflow cannot run here; actionlint (TestWorkflowsPassActionlint) holds its syntax.

type gateWorkflow struct {
	Jobs map[string]gateJob `yaml:"jobs"`
}

type gateJob struct {
	Needs          any               `yaml:"needs"`
	Env            map[string]string `yaml:"env"`
	TimeoutMinutes int               `yaml:"timeout-minutes"`
	Strategy       struct {
		Matrix struct {
			Image []string `yaml:"image"`
		} `yaml:"matrix"`
	} `yaml:"strategy"`
	Steps []gateStep `yaml:"steps"`
}

type gateStep struct {
	Name string            `yaml:"name"`
	Run  string            `yaml:"run"`
	Env  map[string]string `yaml:"env"`
	With map[string]any    `yaml:"with"`
}

func loadGateVerify(t *testing.T) gateWorkflow {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "gate-verify.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var w gateWorkflow
	if err := yaml.Unmarshal(b, &w); err != nil {
		t.Fatal(err)
	}
	return w
}

// step returns the first step of j whose name starts with prefix.
func (j gateJob) step(t *testing.T, prefix string) gateStep {
	t.Helper()
	for _, s := range j.Steps {
		if strings.HasPrefix(s.Name, prefix) {
			return s
		}
	}
	t.Fatalf("no step named %q...", prefix)
	return gateStep{}
}

// TestGateVerifyRunsTheGroupEstate: the two group jobs run the gate's two parts, on the group topology, on
// the three Oracle images of the series (each one of the six the plain jobs run), and upload their evidence
// under names the summary counts apart; the summary waits for them.
func TestGateVerifyRunsTheGroupEstate(t *testing.T) {
	w := loadGateVerify(t)
	series := []string{"mysql:8.0.40", "mysql:8.4.11", "mysql:9.7.1"}
	for _, tc := range []struct{ job, plain, part, env, artifact string }{
		{"integration-group", "integration", "integration", "dot.env.group", "gate-group-integration-"},
		{"e2e-group", "e2e", "e2e", "dot.env.group", "gate-group-e2e-"},
	} {
		j, ok := w.Jobs[tc.job]
		if !ok {
			t.Errorf("gate-verify.yml has no job %s", tc.job)
			continue
		}
		if !slices.Equal(j.Strategy.Matrix.Image, series) {
			t.Errorf("%s runs the images %v, want one per series: %v", tc.job, j.Strategy.Matrix.Image, series)
		}
		for _, img := range j.Strategy.Matrix.Image {
			if !slices.Contains(w.Jobs[tc.plain].Strategy.Matrix.Image, img) {
				t.Errorf("%s runs %s, which the plain %s job does not: the images are the pinned ones", tc.job, img, tc.plain)
			}
		}
		if j.Env["TOPOLOGY"] != "group" {
			t.Errorf("%s does not run on the group topology: env %v", tc.job, j.Env)
		}
		if j.TimeoutMinutes == 0 {
			t.Errorf("%s has no timeout", tc.job)
		}
		run := j.step(t, "Gate, part "+tc.part).Run
		for _, want := range []string{"cp tests/dot.env.group tests/.env.group", ". tests/.env.group", "make e2e-setup", "run-gate.sh --part " + tc.part} {
			if !strings.Contains(run, want) {
				t.Errorf("%s's gate step lacks %q:\n%s", tc.job, want, run)
			}
		}
		if j.step(t, "Gate, part "+tc.part).Env["GA_ESTATE_IMAGE"] != "${{ matrix.image }}" {
			t.Errorf("%s does not pin the estate's image to the matrix's", tc.job)
		}
		found := false
		for _, s := range j.Steps {
			if name, _ := s.With["name"].(string); strings.HasPrefix(name, tc.artifact) && strings.HasSuffix(name, "${{ strategy.job-index }}") {
				found = true
			}
		}
		if !found {
			t.Errorf("%s does not upload an artifact named %s<job-index>", tc.job, tc.artifact)
		}
	}
	if j := w.Jobs["e2e-group"]; !strings.Contains(strings.Join(stepRuns(j), "\n"), "netcat-openbsd socat") {
		t.Errorf("e2e-group does not install the control socket's test tools: E2E 20 fails without them")
	}

	summary := w.Jobs["summary"]
	needs, _ := summary.Needs.([]any)
	for _, n := range []string{"integration-group", "e2e-group"} {
		if !slices.Contains(needs, any(n)) {
			t.Errorf("the summary does not wait for %s: needs %v", n, needs)
		}
	}
	text := strings.Join(stepRuns(summary), "\n")
	if !strings.Contains(text, `evidence "$TARGET_SHA" 0 6 3`) {
		t.Errorf("the summary does not want six plain gates and three group gates:\n%s", text)
	}
	for _, want := range []string{"integration-group=", "e2e-group="} {
		if !strings.Contains(text, want) {
			t.Errorf("the summary does not pass the result of %s:\n%s", want, text)
		}
	}
}

// stepRuns is every run script of j's steps.
func stepRuns(j gateJob) []string {
	var out []string
	for _, s := range j.Steps {
		if s.Run != "" {
			out = append(out, s.Run)
		}
	}
	return out
}

// TestGateVerifyServerVersionComesFromTheTopology (review finding 8): the "Server version" step of every job
// reads the source's version from the container the topology names and the password from its env file; none
// names db1, which a group job does not have.
func TestGateVerifyServerVersionComesFromTheTopology(t *testing.T) {
	w := loadGateVerify(t)
	steps := 0
	for name, j := range w.Jobs {
		for _, s := range j.Steps {
			if strings.Contains(s.Run, "docker exec db1") {
				t.Errorf("%s names db1: %q", name, s.Name)
			}
			if !strings.HasPrefix(s.Name, "Server version") {
				continue
			}
			steps++
			for _, want := range []string{". tests/scripts/topology.sh", `docker exec "$ESTATE_SOURCE"`, `tests/$ESTATE_ENV`, "tests/results/gate/server-version.txt"} {
				if !strings.Contains(s.Run, want) {
					t.Errorf("%s: the Server version step lacks %q:\n%s", name, want, s.Run)
				}
			}
		}
	}
	if steps != 4 {
		t.Errorf("%d Server version steps, want one in each of the four gate jobs", steps)
	}
}

// TestTheGroupMatrixRowStays: the matrix keeps its `group_replication_source` row beside the group gate
// (spec §9): the row runs the unfinished-work guard tests on a throwaway pair, the gate runs the estate's
// whole suite.
func TestTheGroupMatrixRowStays(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "envmatrix", "rows.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "group_replication_source\tgroup\t") {
		t.Error("tests/envmatrix/rows.tsv has no group_replication_source row of kind group")
	}
}
