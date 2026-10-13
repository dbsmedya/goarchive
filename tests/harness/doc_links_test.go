package harness

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"unicode"
)

// TestDocLinks: every relative link in every tracked Markdown file resolves to a file, and
// every anchor to a heading of the file it names. External links (http, https, mailto) are
// out of scope. Anchors follow GitHub's rule (headingSlug); code fences are skipped; HTML
// anchors count, <a name> and <a id> alike (README_LIMITATIONS.md keeps an <a id> for a
// renamed heading's old links).
func TestDocLinks(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("git", "-C", root, "ls-files", "*.md").Output()
	if err != nil {
		t.Fatalf("git ls-files: %v", err)
	}
	files := strings.Fields(string(out))
	if len(files) == 0 {
		t.Fatal("no tracked Markdown files")
	}
	anchors := map[string]map[string]bool{}
	anchorsOf := func(path string) map[string]bool {
		if a, ok := anchors[path]; ok {
			return a
		}
		a, err := docAnchors(path)
		if err != nil {
			t.Fatal(err)
		}
		anchors[path] = a
		return a
	}
	link := regexp.MustCompile(`\]\(([^)\s]+)\)`)
	for _, f := range files {
		path := filepath.Join(root, f)
		lines, err := docLines(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, l := range lines {
			for _, m := range link.FindAllStringSubmatch(l.text, -1) {
				target := m[1]
				if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") || strings.HasPrefix(target, "mailto:") {
					continue
				}
				file, anchor, _ := strings.Cut(target, "#")
				dest := path
				if file != "" {
					dest = filepath.Join(filepath.Dir(path), file)
					if _, err := os.Stat(dest); err != nil {
						t.Errorf("%s:%d: %s: no such file", f, l.n, target)
						continue
					}
				}
				if anchor != "" && strings.HasSuffix(dest, ".md") && !anchorsOf(dest)[anchor] {
					t.Errorf("%s:%d: %s: no heading with that anchor", f, l.n, target)
				}
			}
		}
	}
}

type docLine struct {
	n    int
	text string
}

// docLines returns the lines of a Markdown file outside code fences.
func docLines(path string) ([]docLine, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var out []docLine
	fence := false
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for n := 1; sc.Scan(); n++ {
		text := sc.Text()
		if strings.HasPrefix(strings.TrimSpace(text), "```") {
			fence = !fence
			continue
		}
		if !fence {
			out = append(out, docLine{n, text})
		}
	}
	return out, sc.Err()
}

var (
	headingLine = regexp.MustCompile(`^#{1,6}\s+(.*?)\s*#*\s*$`)
	namedAnchor = regexp.MustCompile(`<a\s+(?:name|id)="([^"]+)"`)
)

// docAnchors returns the anchors of a Markdown file: its headings' slugs, the second
// identical slug suffixed -1, the third -2, and its <a name> and <a id> anchors.
func docAnchors(path string) (map[string]bool, error) {
	lines, err := docLines(path)
	if err != nil {
		return nil, err
	}
	out, seen := map[string]bool{}, map[string]int{}
	for _, l := range lines {
		for _, m := range namedAnchor.FindAllStringSubmatch(l.text, -1) {
			out[m[1]] = true
		}
		m := headingLine.FindStringSubmatch(l.text)
		if m == nil {
			continue
		}
		slug := headingSlug(m[1])
		if n := seen[slug]; n > 0 {
			out[fmt.Sprintf("%s-%d", slug, n)] = true
		} else {
			out[slug] = true
		}
		seen[slug]++
	}
	return out, nil
}

// headingSlug is GitHub's anchor for a heading: lower-case; every character that is not a
// letter, a digit, a space, a hyphen or an underscore dropped; spaces to hyphens.
func headingSlug(heading string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(heading) {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_':
			b.WriteRune(r)
		case r == ' ':
			b.WriteRune('-')
		}
	}
	return b.String()
}

// TestHeadingSlug pins the slug rule on the shapes the docs use.
func TestHeadingSlug(t *testing.T) {
	for in, want := range map[string]string{
		"Known Limits & Caution":                                  "known-limits--caution",
		"No DDL and no concurrent writes during a run (contract)": "no-ddl-and-no-concurrent-writes-during-a-run-contract",
		"`archiver_job_log_<id>`":                                 "archiver_job_log_id",
		"Moving from 3.0 alpha.0":                                 "moving-from-30-alpha0",
		"`ga_status` values":                                      "ga_status-values",
	} {
		if got := headingSlug(in); got != want {
			t.Errorf("headingSlug(%q) = %q, want %q", in, got, want)
		}
	}
}
