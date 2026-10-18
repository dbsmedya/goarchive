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

// mysqlCharsets is MySQL's 41 character sets, as SHOW CHARACTER SET lists them on 8.0.40, 8.4.11
// and 9.7.1 (CHS-1 Task 0), sorted. The guide must name exactly these.
var mysqlCharsets = []string{
	"armscii8", "ascii", "big5", "binary", "cp1250", "cp1251", "cp1256", "cp1257", "cp850", "cp852",
	"cp866", "cp932", "dec8", "eucjpms", "euckr", "gb18030", "gb2312", "gbk", "geostd8", "greek",
	"hebrew", "hp8", "keybcs2", "koi8r", "koi8u", "latin1", "latin2", "latin5", "latin7", "macce",
	"macroman", "sjis", "swe7", "tis620", "ucs2", "ujis", "utf16", "utf16le", "utf32", "utf8mb3",
	"utf8mb4",
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
// its refused list shares none of it, and the two name each of MySQL's 41 sets once, so a
// misspelled or invented set fails too. A set added to the code without the guide fails here.
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
	t.Run("MySQL's 41 sets, each once", func(t *testing.T) {
		all := slices.Sorted(slices.Values(slices.Concat(supported, refused)))
		if !slices.Equal(all, mysqlCharsets) {
			t.Errorf("the guide names %v, want each of MySQL's sets once: %v", all, mysqlCharsets)
		}
	})
}
