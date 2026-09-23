package harness

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// checkGateDrift reports whether `make check` has drifted from the gate's non-database
// stages: the STAGES entries after estate and before integration, in order, with the gate's
// unit stage run as test-unit. build is carried by check alone.
func checkGateDrift(makefile, gateScript string) error {
	check, err := makeCheckPrerequisites(makefile)
	if err != nil {
		return err
	}
	gate, err := gateCheckStages(gateScript)
	if err != nil {
		return err
	}
	if n := len(check); n > 0 && check[n-1] == "build" {
		check = check[:n-1]
	}
	if !reflect.DeepEqual(check, gate) {
		return fmt.Errorf("make check prerequisites %v differ from gate stages %v", check, gate)
	}
	return nil
}

// gateCheckStages returns the gate's non-database stages as make targets.
func gateCheckStages(gateScript string) ([]string, error) {
	var stages []string
	found := false
	for _, line := range strings.Split(gateScript, "\n") {
		if rest, ok := strings.CutPrefix(line, "STAGES=("); ok {
			rest, _, _ = strings.Cut(rest, ")")
			stages, found = strings.Fields(rest), true
			break
		}
	}
	if !found {
		return nil, errors.New("run-gate.sh defines no STAGES=( line")
	}
	first, last := -1, -1
	for i, s := range stages {
		switch s {
		case "estate":
			first = i
		case "integration":
			last = i
		}
	}
	if first < 0 {
		return nil, errors.New("gate STAGES has no estate boundary")
	}
	if last < 0 {
		return nil, errors.New("gate STAGES has no integration boundary")
	}
	if last < first {
		return nil, errors.New("gate STAGES lists integration before estate")
	}
	var targets []string
	for _, s := range stages[first+1 : last] {
		if s == "unit" {
			s = "test-unit"
		}
		targets = append(targets, s)
	}
	return targets, nil
}

// makeCheckPrerequisites returns the prerequisites of the Makefile's check: rule.
func makeCheckPrerequisites(makefile string) ([]string, error) {
	for _, line := range strings.Split(makefile, "\n") {
		if rest, ok := strings.CutPrefix(line, "check:"); ok {
			return strings.Fields(rest), nil
		}
	}
	return nil, errors.New("Makefile has no check: rule")
}

func readRepoFile(t *testing.T, parts ...string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(parts...))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestMakeCheckMatchesGate(t *testing.T) {
	makefile := readRepoFile(t, "..", "..", "Makefile")
	gateScript := readRepoFile(t, "..", "scripts", "run-gate.sh")

	t.Run("repository", func(t *testing.T) {
		if err := checkGateDrift(makefile, gateScript); err != nil {
			t.Fatal(err)
		}
	})

	// The synthetic pair is equal by construction: the gate side is the real script, and
	// the Makefile side is a check: rule built from that script's own stages plus build.
	stages, err := gateCheckStages(gateScript)
	if err != nil {
		t.Fatal(err)
	}
	checkLine := "check: " + strings.Join(append(stages, "build"), " ")
	synthMakefile := checkLine + "\n"
	withNewCheck := strings.TrimSuffix(checkLine, " build") + " newstage build\n"
	var withNewGate string
	for _, line := range strings.Split(gateScript, "\n") {
		if strings.HasPrefix(line, "STAGES=(") {
			withNewGate = strings.Replace(gateScript, line, strings.Replace(line, " integration", " newstage integration", 1), 1)
			break
		}
	}
	if withNewGate == "" || withNewGate == gateScript {
		t.Fatal("could not insert newstage before integration in STAGES")
	}

	cases := []struct {
		name, makefile, gate string
		wantDrift            bool
	}{
		{"equal", synthMakefile, gateScript, false},
		{"gate_only", synthMakefile, withNewGate, true},
		{"check_only", withNewCheck, gateScript, true},
		{"both", withNewCheck, withNewGate, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := checkGateDrift(c.makefile, c.gate)
			switch {
			case !c.wantDrift && err != nil:
				t.Fatalf("unexpected drift: %v", err)
			case c.wantDrift && err == nil:
				t.Fatal("expected drift naming newstage, got none")
			case c.wantDrift && !strings.Contains(err.Error(), "newstage"):
				t.Fatalf("drift error does not name newstage: %v", err)
			}
		})
	}
}
