package harness

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// Version sites (CLAUDE.md → Versioning). The Makefile's RELEASE_VERSION is the anchor; the
// other sites are compared with it, and no other tracked file may contain it.
const (
	makefilePath = "Makefile"
	rootGoPath   = "cmd/goarchive/cmd/root.go"
	readmePath   = "README.md"
	versionLine  = "- **Version**:"
	stableLine   = "- **Stable release**:"
)

var (
	releaseVersionLine = regexp.MustCompile(`(?m)^RELEASE_VERSION\s*:=\s*(\S+)\s*$`)
	rootGoVersion      = regexp.MustCompile(`(?m)^\s*Version\s*=\s*"([^"]*)"`)
	backticked         = regexp.MustCompile("`([^`]*)`")
)

// versionSpreadAllowed names tracked files that may contain the release version although
// they are not version sites, with the reason. An entry needs the operator's approval in the
// PR that adds it.
var versionSpreadAllowed = map[string]string{}

// collectTrackedFiles reads every file git tracks under root, keyed by slash path.
func collectTrackedFiles(root string) (map[string]string, error) {
	out, err := exec.Command("git", "-C", root, "ls-files", "-z").Output()
	if err != nil {
		return nil, fmt.Errorf("git ls-files: %w", err)
	}
	files := map[string]string{}
	for _, p := range bytes.Split(out, []byte{0}) {
		if len(p) == 0 {
			continue
		}
		path := string(p)
		b, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(path)))
		if err != nil {
			return nil, err
		}
		files[path] = string(b)
	}
	return files, nil
}

// isPrerelease reports whether a release version carries a prerelease marker; the
// -community suffix alone is not one.
func isPrerelease(v string) bool {
	for _, marker := range []string{"-RC", "-alpha", "-beta"} {
		if strings.Contains(v, marker) {
			return true
		}
	}
	return false
}

// readmeSiteLine returns the 1-based number and first backticked token of the README line
// beginning with prefix, or 0 when there is none.
func readmeSiteLine(readme, prefix string) (int, string) {
	for i, line := range strings.Split(readme, "\n") {
		if strings.HasPrefix(line, prefix) {
			token := ""
			if m := backticked.FindStringSubmatch(line); m != nil {
				token = m[1]
			}
			return i + 1, token
		}
	}
	return 0, ""
}

// checkVersionDrift returns every violation of the version rules, joined, or nil.
func checkVersionDrift(files map[string]string) error {
	const where = " (CLAUDE.md → Versioning)"
	m := releaseVersionLine.FindStringSubmatch(files[makefilePath])
	if m == nil {
		return errors.New("version anchor missing: " + makefilePath + " has no RELEASE_VERSION line" + where)
	}
	anchor := m[1]
	var errs []string

	if v := rootGoVersion.FindStringSubmatch(files[rootGoPath]); v == nil {
		errs = append(errs, "version site missing: "+rootGoPath+` has no Version = "…" line`+where)
	} else if v[1] != anchor {
		errs = append(errs, fmt.Sprintf("version site %s: Version = %q, want RELEASE_VERSION %q%s", rootGoPath, v[1], anchor, where))
	}

	readme := files[readmePath]
	siteLines := map[int]bool{}
	sites := []struct {
		label, prefix string
		compare       bool
	}{
		{"Version line", versionLine, true},
		{"Stable release line", stableLine, !isPrerelease(anchor)},
	}
	for _, s := range sites {
		n, token := readmeSiteLine(readme, s.prefix)
		if n == 0 {
			errs = append(errs, fmt.Sprintf("version site missing: %s has no line beginning %q%s", readmePath, s.prefix, where))
			continue
		}
		siteLines[n] = true
		if s.compare && token != anchor {
			stable := ""
			if s.prefix == stableLine {
				stable = ", which is stable"
			}
			errs = append(errs, fmt.Sprintf("version site %s:%d (%s): %q, want RELEASE_VERSION %q%s%s", readmePath, n, s.label, token, anchor, stable, where))
		}
	}

	paths := make([]string, 0, len(files))
	for p := range files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		if p == makefilePath || p == rootGoPath {
			continue
		}
		if _, ok := versionSpreadAllowed[p]; ok {
			continue
		}
		for i, line := range strings.Split(files[p], "\n") {
			if p == readmePath && siteLines[i+1] {
				continue
			}
			if strings.Contains(line, anchor) {
				errs = append(errs, fmt.Sprintf("version spread: %s:%d contains release version %q; only the version sites may%s", p, i+1, anchor, where))
			}
		}
	}

	if len(errs) == 0 {
		return nil
	}
	return errors.New(strings.Join(errs, "; "))
}

func TestVersionSitesMatchReleaseVersion(t *testing.T) {
	t.Run("repository", func(t *testing.T) {
		files, err := collectTrackedFiles(filepath.Join("..", ".."))
		if err != nil {
			t.Fatal(err)
		}
		if err := checkVersionDrift(files); err != nil {
			t.Fatal(err)
		}
	})

	// Fictional versions, so this file never contains the real release version: the anchor
	// is 9.8.7-community and every differing site says 9.8.6-community.
	const (
		makefile = "RELEASE_VERSION := 9.8.7-community\n"
		rootGo   = "package cmd\n\nvar (\n\tVersion = \"9.8.7-community\"\n)\n"
		readme   = "- **Version**: `9.8.7-community` (**stable**)\n- **Stable release**: `9.8.7-community` — fixture.\n"
	)
	aligned := func() map[string]string {
		return map[string]string{makefilePath: makefile, rootGoPath: rootGo, readmePath: readme}
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
	// prerelease returns a fixture whose anchor, root.go and Version line say v while the
	// Stable release line still names the previous stable release.
	prerelease := func(t *testing.T, v string) map[string]string {
		return map[string]string{
			makefilePath: replace(t, makefile, "9.8.7-community", v),
			rootGoPath:   replace(t, rootGo, "9.8.7-community", v),
			readmePath:   replace(t, readme, "`9.8.7-community` (**stable**)", "`"+v+"` (release candidate)"),
		}
	}

	rejecting := []struct {
		name  string
		files func(t *testing.T) map[string]string
		want  []string // substrings the drift error must contain, specific enough that the
		// pointer every message carries (CLAUDE.md → Versioning) cannot satisfy them
	}{
		{"root_go_differs", func(t *testing.T) map[string]string {
			f := aligned()
			f[rootGoPath] = replace(t, rootGo, "9.8.7-community", "9.8.6-community")
			return f
		}, []string{rootGoPath, `"9.8.6-community"`, `want RELEASE_VERSION "9.8.7-community"`}},
		{"readme_version_differs", func(t *testing.T) map[string]string {
			f := aligned()
			f[readmePath] = replace(t, readme, "`9.8.7-community` (**stable**)", "`9.8.6-community` (**stable**)")
			return f
		}, []string{"README.md:1", `"9.8.6-community"`, `want RELEASE_VERSION "9.8.7-community"`}},
		{"stable_line_differs_on_stable", func(t *testing.T) map[string]string {
			f := aligned()
			f[readmePath] = replace(t, readme, "`9.8.7-community` — fixture", "`9.8.6-community` — fixture")
			return f
		}, []string{"README.md:2", `"9.8.6-community"`, `want RELEASE_VERSION "9.8.7-community"`, "which is stable"}},
		{"spread_other_file", func(t *testing.T) map[string]string {
			f := aligned()
			f["docs/x.md"] = "Retired since v9.8.7-community.\n"
			return f
		}, []string{"docs/x.md:1"}},
		{"spread_third_readme_line", func(t *testing.T) map[string]string {
			f := aligned()
			f[readmePath] = readme + "Built from 9.8.7-community.\n"
			return f
		}, []string{"README.md:3"}},
		{"anchor_missing", func(t *testing.T) map[string]string {
			f := aligned()
			f[makefilePath] = "build:\n\tgo build ./...\n"
			return f
		}, []string{"Makefile has no RELEASE_VERSION line"}},
		{"site_missing", func(t *testing.T) map[string]string {
			f := aligned()
			f[rootGoPath] = "package cmd\n"
			return f
		}, []string{rootGoPath + " has no Version"}},
	}
	for _, c := range rejecting {
		t.Run(c.name, func(t *testing.T) {
			err := checkVersionDrift(c.files(t))
			for _, w := range c.want {
				if err == nil || !strings.Contains(err.Error(), w) {
					t.Fatalf("%s: checkVersionDrift = %v, want an error containing %q", c.name, err, w)
				}
			}
		})
	}

	t.Run("stable_line_free_on_prerelease", func(t *testing.T) {
		for _, v := range []struct{ name, version string }{{"rc", "9.9.0-RC-community"}, {"alpha", "9.9.0-alpha.1"}} {
			t.Run(v.name, func(t *testing.T) {
				if err := checkVersionDrift(prerelease(t, v.version)); err != nil {
					t.Fatalf("stable_line_free_on_prerelease: checkVersionDrift returned an error for an exempt prerelease mismatch: %v; want nil", err)
				}
			})
		}
	})

	t.Run("aligned_stable", func(t *testing.T) {
		if err := checkVersionDrift(aligned()); err != nil {
			t.Fatalf("aligned_stable: checkVersionDrift = %v, want nil", err)
		}
	})
}
