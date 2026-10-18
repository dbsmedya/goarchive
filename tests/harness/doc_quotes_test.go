package harness

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

// docQuote is one reference to a doc in a string the code builds: its position, the file it
// names (docs/README_X.md) and the heading it quotes, "" for a bare reference to the file.
type docQuote struct{ pos, file, heading string }

var (
	docRef    = regexp.MustCompile(`docs/README_[A-Za-z0-9_]+\.md`)
	quotedRef = regexp.MustCompile(`"([^"]+)" in $`)
)

// docQuotes returns the doc references in the string literals of every non-test .go file
// under root/dirs, and every reference whose form it does not recognise. A + chain of
// literals is read as one value, so a heading split over two lines is still seen; any other
// operand breaks the chain. Comments are not messages and are not read, and a heading passed
// as an argument (fmt.Sprintf's %s) is not a literal, so it is not seen either.
func docQuotes(root string, dirs ...string) (quotes []docQuote, unrecognised []string, err error) {
	for _, dir := range dirs {
		err = filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if d.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			var visit func(n ast.Node) bool
			read := func(at token.Pos, v string) {
				pos := fmt.Sprintf("%s:%d", rel, fset.Position(at).Line)
				q, u := classifyDocRefs(pos, v)
				quotes, unrecognised = append(quotes, q...), append(unrecognised, u...)
			}
			visit = func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.BinaryExpr:
					if n.Op != token.ADD {
						return true
					}
					var others []ast.Expr
					read(n.Pos(), joinLiterals(n, &others))
					for _, o := range others {
						ast.Inspect(o, visit)
					}
					return false
				case *ast.BasicLit:
					if n.Kind == token.STRING {
						if v, err := strconv.Unquote(n.Value); err == nil {
							read(n.Pos(), v)
						}
					}
				}
				return true
			}
			ast.Inspect(f, visit)
			return nil
		})
		if err != nil {
			return nil, nil, err
		}
	}
	return quotes, unrecognised, nil
}

// joinLiterals is the value of a + chain: its string literals joined, with "\x00" for every
// other operand, which it appends to others so that their own literals are read on their own.
func joinLiterals(e ast.Expr, others *[]ast.Expr) string {
	switch e := e.(type) {
	case *ast.ParenExpr:
		return joinLiterals(e.X, others)
	case *ast.BinaryExpr:
		if e.Op == token.ADD {
			return joinLiterals(e.X, others) + joinLiterals(e.Y, others)
		}
	case *ast.BasicLit:
		if e.Kind == token.STRING {
			if v, err := strconv.Unquote(e.Value); err == nil {
				return v
			}
		}
	}
	*others = append(*others, e)
	return "\x00"
}

// classifyDocRefs finds each docs/README_*.md in v and reads the heading it quotes:
//
//	A  docs/README_X.md, "Heading"
//	B  "Heading" in docs/README_X.md
//	C  docs/README_X.md → Heading            (up to . ) ; " or the end)
//	D  docs/README_X.md, Heading             (a capital first, the same ends)
//
// or none, for a bare reference followed by ), ., the end, or a lower-case word. Anything
// else is unrecognised, so a new way of quoting a heading fails the test instead of passing
// unread.
func classifyDocRefs(pos, v string) (quotes []docQuote, unrecognised []string) {
	for _, m := range docRef.FindAllStringIndex(v, -1) {
		file, before, after := v[m[0]:m[1]], v[:m[0]], v[m[1]:]
		q := docQuote{pos: pos, file: file}
		switch {
		case quotedRef.MatchString(before):
			q.heading = quotedRef.FindStringSubmatch(before)[1]
		case strings.HasPrefix(after, `, "`):
			h, _, ok := strings.Cut(after[len(`, "`):], `"`)
			if !ok {
				unrecognised = append(unrecognised, unrecognisedRef(pos, v, m))
				continue
			}
			q.heading = h
		case strings.HasPrefix(after, " → "):
			q.heading = headingUpToEnd(after[len(" → "):])
		case strings.HasPrefix(after, ", ") && startsWith(after[2:], unicode.IsUpper):
			q.heading = headingUpToEnd(after[2:])
		case after == "" || strings.HasPrefix(after, ")") || strings.HasPrefix(after, ".") || strings.HasPrefix(after, "\x00"):
		case (strings.HasPrefix(after, ", ") || strings.HasPrefix(after, " ")) && startsWith(strings.TrimPrefix(strings.TrimPrefix(after, ","), " "), unicode.IsLower):
		default:
			unrecognised = append(unrecognised, unrecognisedRef(pos, v, m))
			continue
		}
		quotes = append(quotes, q)
	}
	return quotes, unrecognised
}

// headingUpToEnd is an unquoted heading: s up to the first . ) ; " NUL or newline.
func headingUpToEnd(s string) string {
	if i := strings.IndexAny(s, ".);\"\x00\n"); i >= 0 {
		s = s[:i]
	}
	return strings.TrimSpace(s)
}

func startsWith(s string, is func(rune) bool) bool {
	r, _ := utf8.DecodeRuneInString(s)
	return is(r)
}

func unrecognisedRef(pos, v string, m []int) string {
	from, to := max(m[0]-20, 0), min(m[1]+20, len(v))
	return fmt.Sprintf("%s: unrecognised doc reference %q", pos, v[from:to])
}

// staleDocQuotes returns a problem for each quote whose file is missing or has no such
// heading. Headings are read outside code fences, and backticks are ignored on both sides.
func staleDocQuotes(root string, quotes []docQuote) ([]string, error) {
	var problems []string
	headings := map[string]map[string]bool{}
	for _, q := range quotes {
		path := filepath.Join(root, q.file)
		if _, err := os.Stat(path); err != nil {
			problems = append(problems, fmt.Sprintf("%s: %s: no such file", q.pos, q.file))
			continue
		}
		if q.heading == "" {
			continue
		}
		h, ok := headings[path]
		if !ok {
			lines, err := docLines(path)
			if err != nil {
				return nil, err
			}
			h = map[string]bool{}
			for _, l := range lines {
				if m := headingLine.FindStringSubmatch(l.text); m != nil {
					h[strings.ReplaceAll(m[1], "`", "")] = true
				}
			}
			headings[path] = h
		}
		if want := strings.ReplaceAll(q.heading, "`", ""); !h[want] {
			problems = append(problems, fmt.Sprintf("%s: %s has no heading %q", q.pos, q.file, want))
		}
	}
	return problems, nil
}

// TestErrorMessagesQuoteDocHeadings (HDG-1): an error message that points an operator at a doc
// heading by name must name a heading that exists, so a heading renamed with its links, or
// edited without changing its slug, fails here rather than leaving the message stale.
// TestDocLinks checks links and anchors only.
func TestErrorMessagesQuoteDocHeadings(t *testing.T) {
	doc := "# A\n\n## Plain heading\n\n#### A `code` heading\n\n```\n## Fenced heading\n```\n"
	for _, tc := range []struct {
		name string
		decl string // Go declarations after the package clause
		want string // a problem containing it; "" for none
	}{
		{"form A, current", `var _ = "See docs/README_A.md, \"Plain heading\""`, ""},
		{"form A, stale", `var _ = "See docs/README_A.md, \"Gone heading\""`, "Gone heading"},
		{"form A, backticks", "var _ = \"docs/README_A.md, \\\"A `code` heading\\\"\"\nvar _ = \"docs/README_A.md, \\\"A code heading\\\"\"", ""},
		{"form B, stale", `var _ = "See \"Gone heading\" in docs/README_A.md"`, "Gone heading"},
		{"form C, stale", `var _ = "see docs/README_A.md → Gone heading."`, "Gone heading"},
		{"form D split, current", `var _ = "(docs/README_A.md, Plain " + "heading)"`, ""},
		{"form D split, stale", `var _ = "(docs/README_A.md, Gone " + "one)"`, "Gone one"},
		{"a const", `const see = "See docs/README_A.md, \"Gone heading\""`, "Gone heading"},
		{"a raw string", "var _ = `See docs/README_A.md, \"Gone heading\"`", "Gone heading"},
		{"a fenced heading", `var _ = "docs/README_A.md, \"Fenced heading\""`, "Fenced heading"},
		{"a missing file", `var _ = "docs/README_B.md"`, "README_B.md"},
		{"an unrecognised form", `var _ = "docs/README_A.md; Plain heading"`, "unrecognised"},
		{"a bare reference", "var _ = \"(docs/README_A.md), or a key\"\nvar _ = \"docs/README_A.md for the limits\"", ""},
		{"a comment", "// See docs/README_A.md, \"Gone heading\"\nvar _ = 1", ""},
		// A heading form that yields no heading is never a bare reference (Codex R1).
		{"form C quoted, current", `var _ = "See docs/README_A.md → \"Plain heading\""`, ""},
		{"form C quoted, stale", `var _ = "See docs/README_A.md → \"Gone heading\""`, "Gone heading"},
		{"form C, empty", `var _ = "see docs/README_A.md → ."`, "unrecognised"},
		{"form A, empty", `var _ = "See docs/README_A.md, \"\""`, "unrecognised"},
		{"form A, backticks only", "var _ = \"See docs/README_A.md, \\\"``\\\"\"", "unrecognised"},
		{"form B, empty", `var _ = "See \"\" in docs/README_A.md"`, "unrecognised"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeFixture(t, filepath.Join(root, "docs", "README_A.md"), doc)
			writeFixture(t, filepath.Join(root, "internal", "x", "x.go"), "package x\n\n"+tc.decl+"\n")
			problems := docProblems(t, root, "internal")
			if tc.want == "" {
				if len(problems) > 0 {
					t.Errorf("problems %q, want none", problems)
				}
				return
			}
			if len(problems) != 1 || !strings.Contains(problems[0], tc.want) {
				t.Errorf("problems %q, want one naming %q", problems, tc.want)
			}
		})
	}

	t.Run("the repository", func(t *testing.T) {
		root, err := filepath.Abs("../..")
		if err != nil {
			t.Fatal(err)
		}
		if problems := docProblems(t, root, "internal", "cmd"); len(problems) > 0 {
			t.Errorf("stale or unrecognised doc references:\n%s", strings.Join(problems, "\n"))
		}
		quotes, _, err := docQuotes(root, "internal", "cmd")
		if err != nil {
			t.Fatal(err)
		}
		headed := 0
		for _, q := range quotes {
			if q.heading != "" {
				headed++
			}
		}
		if headed == 0 {
			t.Error("no quote with a heading found: the reader no longer sees the messages")
		}
	})
}

// docProblems is docQuotes' unrecognised references followed by staleDocQuotes' problems.
func docProblems(t *testing.T, root string, dirs ...string) []string {
	t.Helper()
	quotes, problems, err := docQuotes(root, dirs...)
	if err != nil {
		t.Fatal(err)
	}
	stale, err := staleDocQuotes(root, quotes)
	if err != nil {
		t.Fatal(err)
	}
	return append(problems, stale...)
}
