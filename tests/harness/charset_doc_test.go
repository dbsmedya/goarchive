package harness

import (
	"regexp"
	"slices"
	"strings"
	"testing"
)

// The character-set guide lists the sets COLUMN_CHARSET_CHECK accepts and the ones it refuses
// (CHD-1). The code's list is supportedCharsets, one []string literal.
const (
	charsetsGoPath  = "internal/preflight/charsets.go"
	charsetsDocPath = "docs/README_CHARSETS.md"
)

// fromRoot is path's elements under the repository root, as readRepoFile takes them.
func fromRoot(path string) []string {
	return append([]string{"..", ".."}, strings.Split(path, "/")...)
}

var (
	supportedLiteral = regexp.MustCompile(`(?s)var supportedCharsets = \[\]string\{(.*?)\}`)
	quotedName       = regexp.MustCompile(`"([a-z0-9]+)"`)
	tableName        = regexp.MustCompile("^\\| `([a-z0-9]+)` \\|")
)

// codeCharsets is supportedCharsets' names, in order.
func codeCharsets(t *testing.T) []string {
	t.Helper()
	m := supportedLiteral.FindStringSubmatch(readRepoFile(t, fromRoot(charsetsGoPath)...))
	if m == nil {
		t.Fatalf("%s: no `var supportedCharsets = []string{…}` literal", charsetsGoPath)
	}
	var out []string
	for _, q := range quotedName.FindAllStringSubmatch(m[1], -1) {
		out = append(out, q[1])
	}
	return out
}

// docCharsets is the first cell of every row of the table under heading, up to the next
// "## " heading.
func docCharsets(t *testing.T, doc, heading string) []string {
	t.Helper()
	lines := strings.Split(doc, "\n")
	i := slices.Index(lines, heading)
	if i < 0 {
		t.Fatalf("%s: no %q heading", charsetsDocPath, heading)
	}
	var out []string
	for _, l := range lines[i+1:] {
		if strings.HasPrefix(l, "## ") {
			break
		}
		if m := tableName.FindStringSubmatch(l); m != nil {
			out = append(out, m[1])
		}
	}
	if len(out) == 0 {
		t.Fatalf("%s: no table of character sets under %q", charsetsDocPath, heading)
	}
	return out
}

// TestCharsetDocMatchesCode (CHD-1): the guide's supported list is the code's, in order;
// its refused list shares none of it, and the two name each of MySQL's 41 sets once. A set
// added to the code without the guide fails here.
func TestCharsetDocMatchesCode(t *testing.T) {
	code := codeCharsets(t)
	t.Run("the literal parses", func(t *testing.T) {
		if len(code) != 19 || code[0] != "utf8mb4" || code[len(code)-1] != "binary" {
			t.Fatalf("%s: parsed %v, want 19 names from utf8mb4 to binary", charsetsGoPath, code)
		}
	})
	doc := readRepoFile(t, fromRoot(charsetsDocPath)...)
	supported := docCharsets(t, doc, "## Supported character sets")
	refused := docCharsets(t, doc, "## Refused character sets")
	t.Run("supported equals the code", func(t *testing.T) {
		if !slices.Equal(supported, code) {
			t.Errorf("the guide's supported sets %v, want the code's %v, in its order", supported, code)
		}
	})
	t.Run("refused shares none", func(t *testing.T) {
		for _, cs := range refused {
			if slices.Contains(code, cs) {
				t.Errorf("%s is refused in the guide but supported in the code", cs)
			}
		}
	})
	t.Run("41 distinct sets", func(t *testing.T) {
		all := slices.Concat(supported, refused)
		seen := map[string]bool{}
		for _, cs := range all {
			if seen[cs] {
				t.Errorf("%s is listed twice", cs)
			}
			seen[cs] = true
		}
		if len(seen) != 41 {
			t.Errorf("the guide names %d distinct sets, want MySQL's 41", len(seen))
		}
	})
}
