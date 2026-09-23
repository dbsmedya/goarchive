package harness

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// collectBuildSites reads go.mod and every file that builds goarchive with a Go release of
// its own choosing (Makefile, Dockerfile, workflows), keyed by root-relative slash path.
func collectBuildSites(root string) (map[string]string, error) {
	sites := map[string]string{}
	for _, name := range []string{"go.mod", "Makefile", "Dockerfile"} {
		b, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			if name != "go.mod" && errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		sites[name] = string(b)
	}
	var paths []string
	for _, pattern := range []string{"*.yml", "*.yaml"} {
		matches, err := filepath.Glob(filepath.Join(root, ".github", "workflows", pattern))
		if err != nil {
			return nil, err
		}
		paths = append(paths, matches...)
	}
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return nil, err
		}
		sites[filepath.ToSlash(rel)] = string(b)
	}
	return sites, nil
}

// checkToolchainDrift reports every build site that selects a Go release other than
// go.mod's toolchain line.
func checkToolchainDrift(goMod string, sites map[string]string) error {
	pin := ""
	if m := toolchainLine.FindStringSubmatch(goMod); m != nil {
		pin = m[1]
	}
	var drifts []toolchainDrift
	for path, text := range sites {
		if path == "go.mod" {
			continue
		}
		drifts = append(drifts, textDrifts(path, text, pin)...)
		if strings.HasPrefix(path, ".github/workflows/") {
			drifts = append(drifts, setupStepDrifts(path, text)...)
		}
	}
	if len(drifts) == 0 {
		return nil
	}
	sort.SliceStable(drifts, func(i, j int) bool {
		a, b := drifts[i], drifts[j]
		if a.path != b.path {
			return a.path < b.path
		}
		if a.line != b.line {
			return a.line < b.line
		}
		return a.order < b.order
	})
	msgs := make([]string, len(drifts))
	for i, d := range drifts {
		msgs[i] = d.path + ": " + d.reason
	}
	prefix := "build sites differ from go.mod toolchain go" + pin + ": "
	if pin == "" {
		prefix = "go.mod has no toolchain line; build sites name their own Go release: "
	}
	return errors.New(prefix + strings.Join(msgs, "; "))
}

var (
	toolchainLine = regexp.MustCompile(`(?m)^toolchain go([0-9]+\.[0-9]+\.[0-9]+)\s*$`)
	golangImage   = regexp.MustCompile(`golang:[A-Za-z0-9._-]+`)
	goRelease     = regexp.MustCompile(`\bgo1\.[0-9]+(?:\.[0-9]+)?(?:rc[0-9]+)?\b`)
	releaseTag    = regexp.MustCompile(`^v([0-9]+)(?:\.[0-9]+){0,2}$`)
)

// toolchainDrift is one build site's departure from the pin; line and order sort it within
// its file.
type toolchainDrift struct {
	path, reason string
	line, order  int
}

// textDrifts applies the text rules: a golang image tagged other than the pin (or the pin
// plus a -variant), and any go1.N[.P] token other than the pin. With no pin, every release
// named drifts.
func textDrifts(path, text, pin string) []toolchainDrift {
	var drifts []toolchainDrift
	for i, line := range strings.Split(text, "\n") {
		for _, img := range golangImage.FindAllString(line, -1) {
			tag := strings.TrimPrefix(img, "golang:")
			if pin == "" || (tag != pin && !strings.HasPrefix(tag, pin+"-")) {
				drifts = append(drifts, toolchainDrift{path: path, reason: img, line: i + 1})
			}
		}
		for _, rel := range goRelease.FindAllString(line, -1) {
			if pin == "" || rel != "go"+pin {
				drifts = append(drifts, toolchainDrift{path: path, reason: rel, line: i + 1})
			}
		}
	}
	return drifts
}

// setupStepDrifts applies the step rules to every actions/setup-go step of a workflow: a
// v6-or-later release tag (the first release that reads the toolchain line), its own
// go-version-file: go.mod, and no go-version.
func setupStepDrifts(path, text string) []toolchainDrift {
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(text), &doc); err != nil {
		return []toolchainDrift{{path: path, reason: "does not parse: " + err.Error()}}
	}
	var drifts []toolchainDrift
	for _, job := range mappingValues(yamlKey(doc.Content, "jobs")) {
		steps := yamlKey([]*yaml.Node{job}, "steps")
		if steps == nil || steps.Kind != yaml.SequenceNode {
			continue
		}
		for i, step := range steps.Content {
			uses := yamlKey([]*yaml.Node{step}, "uses")
			if uses == nil {
				continue
			}
			action, ref, hasRef := strings.Cut(uses.Value, "@")
			if !strings.EqualFold(action, "actions/setup-go") {
				continue
			}
			name := strconv.Itoa(i + 1)
			if n := yamlKey([]*yaml.Node{step}, "name"); n != nil {
				name = n.Value
			}
			add := func(order int, reason string) {
				drifts = append(drifts, toolchainDrift{path: path, reason: `step "` + name + `" ` + reason, line: step.Line, order: order})
			}
			if m := releaseTag.FindStringSubmatch(ref); hasRef && m != nil {
				if major, _ := strconv.Atoi(m[1]); major < 6 {
					add(0, "uses "+uses.Value+", which does not read the toolchain line")
				}
			} else {
				add(0, "uses "+uses.Value+", which is not a v6-or-later release tag")
			}
			with := yamlKey([]*yaml.Node{step}, "with")
			if f := yamlKey([]*yaml.Node{with}, "go-version-file"); f == nil || f.Value != "go.mod" {
				add(1, "has no go-version-file: go.mod")
			}
			if v := yamlKey([]*yaml.Node{with}, "go-version"); v != nil {
				add(2, "names go-version "+v.Value)
			}
		}
	}
	return drifts
}

// yamlKey returns the value of key in the first mapping among nodes (a document's content
// or a single mapping), or nil.
func yamlKey(nodes []*yaml.Node, key string) *yaml.Node {
	if len(nodes) == 0 || nodes[0] == nil {
		return nil
	}
	m := nodes[0]
	if m.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i+1]
		}
	}
	return nil
}

// mappingValues returns a mapping's values in document order.
func mappingValues(m *yaml.Node) []*yaml.Node {
	if m == nil || m.Kind != yaml.MappingNode {
		return nil
	}
	var vs []*yaml.Node
	for i := 1; i < len(m.Content); i += 2 {
		vs = append(vs, m.Content[i])
	}
	return vs
}

func TestBuildSitesMatchGoToolchain(t *testing.T) {
	t.Run("repository", func(t *testing.T) {
		sites, err := collectBuildSites(filepath.Join("..", ".."))
		if err != nil {
			t.Fatal(err)
		}
		if err := checkToolchainDrift(sites["go.mod"], sites); err != nil {
			t.Fatal(err)
		}
	})

	// Fictional releases, so this file names no real Go release: the pin is 1.99.1 and
	// every other release is 1.98.0.
	const goMod = "module example.com/drift\n\ngo 1.98.0\n\ntoolchain go1.99.1\n"
	const makefile = "build:\n\tgo build ./...\n"
	const dockerfile = "FROM golang:1.99.1-alpine AS builder\n"
	const workflow = `name: W
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: first setup
        uses: actions/setup-go@v6
        with:
          go-version-file: go.mod
      - name: second setup
        uses: actions/setup-go@v6
        with:
          go-version-file: go.mod
      - run: make check
`
	const wf = ".github/workflows/w.yml"
	aligned := func() map[string]string {
		return map[string]string{"go.mod": goMod, "Makefile": makefile, "Dockerfile": dockerfile, wf: workflow}
	}
	// replace returns s with old replaced once, failing if old is absent so that no case
	// silently degenerates into aligned.
	replace := func(t *testing.T, s, old, new string) string {
		t.Helper()
		if !strings.Contains(s, old) {
			t.Fatalf("fixture lacks %q", old)
		}
		return strings.Replace(s, old, new, 1)
	}
	const secondSetup = `      - name: second setup
        uses: actions/setup-go@v6
        with:
          go-version-file: go.mod
`

	cases := []struct {
		name  string
		sites func(t *testing.T) map[string]string
		label string   // what the drift must name; empty means no drift is expected
		want  []string // substrings the drift error must contain
	}{
		{"aligned", func(t *testing.T) map[string]string { return aligned() }, "", nil},
		{"image_differs", func(t *testing.T) map[string]string {
			s := aligned()
			s["Dockerfile"] = replace(t, dockerfile, "golang:1.99.1-alpine", "golang:1.98.0-alpine")
			return s
		}, "Dockerfile", []string{"Dockerfile: golang:1.98.0-alpine"}},
		{"workflow_literal", func(t *testing.T) map[string]string {
			s := aligned()
			s[wf] = replace(t, workflow, "go-version-file: go.mod", "go-version: '1.99.1'")
			return s
		}, wf, []string{wf + `: step "first setup" names go-version 1.99.1`}},
		{"step_unconfigured", func(t *testing.T) map[string]string {
			s := aligned()
			s[wf] = replace(t, workflow, secondSetup,
				"      - name: second setup\n        uses: actions/setup-go@v6\n"+
					"      - name: echo\n        run: |\n          echo 'go-version-file: go.mod'\n")
			return s
		}, wf, []string{wf + `: step "second setup" has no go-version-file: go.mod`}},
		{"setup_v5", func(t *testing.T) map[string]string {
			s := aligned()
			s[wf] = replace(t, workflow, "actions/setup-go@v6", "actions/setup-go@v5")
			return s
		}, wf, []string{wf + `: step "first setup" uses actions/setup-go@v5, which does not read the toolchain line`}},
		{"setup_unsupported_ref", func(t *testing.T) map[string]string {
			s := aligned()
			w := replace(t, workflow, "actions/setup-go@v6", "actions/setup-go@0123456789abcdef0123456789abcdef01234567")
			s[wf] = replace(t, w, "actions/setup-go@v6", "actions/setup-go@main")
			return s
		}, wf, []string{
			wf + `: step "first setup" uses actions/setup-go@0123456789abcdef0123456789abcdef01234567, which is not a v6-or-later release tag`,
			wf + `: step "second setup" uses actions/setup-go@main, which is not a v6-or-later release tag`,
		}},
		{"stray_release", func(t *testing.T) map[string]string {
			s := aligned()
			s["Makefile"] = "export GOTOOLCHAIN=go1.98.0\n" + makefile
			return s
		}, "Makefile", []string{"Makefile: go1.98.0"}},
		{"no_toolchain", func(t *testing.T) map[string]string {
			s := aligned()
			s["go.mod"] = replace(t, goMod, "toolchain go1.99.1\n", "")
			return s
		}, "go.mod", []string{"go.mod has no toolchain line"}},
		{"yaml_discovered", func(t *testing.T) map[string]string {
			root := t.TempDir()
			files := aligned()
			files[".github/workflows/extra.yaml"] = "name: X\non: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n" +
				"      - name: setup\n        uses: actions/setup-go@v6\n        with:\n          go-version: '1.98.0'\n"
			for p, text := range files {
				full := filepath.Join(root, filepath.FromSlash(p))
				if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(full, []byte(text), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			s, err := collectBuildSites(root)
			if err != nil {
				t.Fatal(err)
			}
			return s
		}, ".github/workflows/extra.yaml", []string{".github/workflows/extra.yaml: step \"setup\""}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sites := c.sites(t)
			err := checkToolchainDrift(sites["go.mod"], sites)
			switch {
			case c.label == "" && err != nil:
				t.Fatalf("unexpected drift: %v", err)
			case c.label == "":
			case err == nil:
				t.Fatalf("expected drift naming %s, got none", c.label)
			default:
				for _, w := range c.want {
					if !strings.Contains(err.Error(), w) {
						t.Fatalf("drift error does not contain %q: %v", w, err)
					}
				}
			}
		})
	}
}
