package harness

import (
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The control socket's default directory is the home that the system's user database
// records for the effective user, never $HOME (docs: TEL-1, spec §4.1). On macOS that lookup
// is os/user, which asks libSystem with or without cgo -- unless the `osusergo` build tag is
// set: then os/user reads /etc/passwd, which macOS does not use, and answers from $USER and
// $HOME. No build may set the tag. Linux reads /etc/passwd itself and Windows takes the
// process token's profile, so the tag changes nothing there; the rule is for every build
// because a release is one set of build sites.

// osusergoSite reports whether a build file names the tag, in any form: -tags=osusergo,
// -tags "netgo,osusergo", GOFLAGS, a go:build line in a script, a golangci build-tags list.
func osusergoSite(text string) bool { return strings.Contains(text, "osusergo") }

// isBuildFile reports whether a file can set build tags: a Makefile, a Dockerfile, a
// workflow or any other YAML, a shell script, a Makefile include.
func isBuildFile(path string) bool {
	base := filepath.Base(path)
	switch {
	case base == "Makefile", strings.HasPrefix(base, "Dockerfile"), base == "go.env":
		return true
	}
	switch filepath.Ext(base) {
	case ".yml", ".yaml", ".sh", ".mk", ".toml":
		return true
	}
	return false
}

// collectTagSites reads every build file below root, keyed by root-relative slash path. It
// skips the development tree (.ayder, gitignored and never built), git's own directory and
// the module cache a build may leave behind.
func collectTagSites(root string) (map[string]string, error) {
	sites := map[string]string{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", ".ayder", "node_modules", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if !isBuildFile(path) {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		sites[filepath.ToSlash(rel)] = string(b)
		return nil
	})
	return sites, err
}

// checkNoOsusergo names every build file that mentions the tag.
func checkNoOsusergo(sites map[string]string) error {
	var hits []string
	for path, text := range sites {
		if osusergoSite(text) {
			hits = append(hits, path)
		}
	}
	if len(hits) == 0 {
		return nil
	}
	sort.Strings(hits)
	return &osusergoError{hits}
}

type osusergoError struct{ paths []string }

func (e *osusergoError) Error() string {
	return "the osusergo build tag makes os/user answer from $USER and $HOME on macOS, which the " +
		"control socket's default directory must not do; remove it from: " + strings.Join(e.paths, ", ")
}

// TestNoBuildSetsOsusergo is a pin: no build site sets the tag today, so it passes before and
// after the control socket's code. The fixtures prove that it would see the tag.
func TestNoBuildSetsOsusergo(t *testing.T) {
	t.Run("repository", func(t *testing.T) {
		sites, err := collectTagSites(filepath.Join("..", ".."))
		if err != nil {
			t.Fatal(err)
		}
		for _, must := range []string{"Makefile", "Dockerfile", ".github/workflows/ci.yml", ".github/workflows/release.yml"} {
			if _, ok := sites[must]; !ok {
				t.Fatalf("the scan did not read %s: it would miss a tag there", must)
			}
		}
		if err := checkNoOsusergo(sites); err != nil {
			t.Fatal(err)
		}
	})

	cases := []struct {
		name string
		site string
		text string
		hit  bool
	}{
		{"flag", "Makefile", "build:\n\tgo build -tags=osusergo ./...\n", true},
		{"list", "Dockerfile", "RUN go build -tags \"netgo,osusergo\" ./...\n", true},
		{"goflags", ".github/workflows/x.yml", "env:\n  GOFLAGS: -tags=osusergo\n", true},
		{"script", "tests/scripts/build.sh", "#!/bin/sh\nGOFLAGS=\"-tags osusergo\" go build\n", true},
		{"lint config", ".golangci.yml", "run:\n  build-tags:\n    - osusergo\n", true},
		{"other tags", "Makefile", "build:\n\tgo build -tags=integration ./...\n", false},
		{"no tags", "Dockerfile", "RUN CGO_ENABLED=0 go build ./...\n", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := checkNoOsusergo(map[string]string{c.site: c.text})
			switch {
			case c.hit && err == nil:
				t.Fatalf("the tag in %s was not found", c.site)
			case c.hit && !strings.Contains(err.Error(), c.site):
				t.Fatalf("the error must name %s: %v", c.site, err)
			case !c.hit && err != nil:
				t.Fatalf("unexpected: %v", err)
			}
		})
	}

	t.Run("walk finds files below the root", func(t *testing.T) {
		root := t.TempDir()
		write := func(rel, text string) {
			full := filepath.Join(root, filepath.FromSlash(rel))
			if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(full, []byte(text), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		write("tests/scripts/deep/build.sh", "go build -tags=osusergo\n")
		write(".ayder/notes.sh", "go build -tags=osusergo\n") // development tree: never built
		write("docs/README.md", "the osusergo tag is discussed here\n")
		sites, err := collectTagSites(root)
		if err != nil {
			t.Fatal(err)
		}
		err = checkNoOsusergo(sites)
		if err == nil || !strings.Contains(err.Error(), "tests/scripts/deep/build.sh") {
			t.Fatalf("got %v, want the deep script named", err)
		}
		if strings.Contains(err.Error(), ".ayder") || strings.Contains(err.Error(), "README.md") {
			t.Fatalf("the scan read what it must skip: %v", err)
		}
	})
}
