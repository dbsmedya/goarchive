package harness

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The list of expected skips (tests/README.md → "Expected skips") is what a reader of a test run
// holds a SKIP line against: a skip on the list is the intended result there, one that is off it
// is a finding. This guard keeps the list whole. Every test that skips by design (t.Skip, t.Skipf
// or t.SkipNow, in the test or in a helper it calls) must be named in the list, and every name in
// the list must exist and still skip, so a new skip without a line, and a line whose skip is gone,
// both fail here.
//
// What it reads, and what it does not see: the call graph is by name within one directory (one
// package, every build tag), through plain calls and method calls. A skip reached through a
// function value, or under a name two helpers share, is not followed. A subtest is not read: an
// entry such as `TestA/some_case` names TestA, and which of its subtests skip is the list's
// own account. The tests that a matrix row excludes with -skip never reach a skip call, so they
// are not listed there; the list says so in prose.

const (
	expectedSkipsFile    = "tests/README.md"
	expectedSkipsHeading = "## Expected skips"
)

// skipFunc is one function declared in a test file: whether it calls Skip itself, where, and what
// it calls by name.
type skipFunc struct {
	name  string
	dir   string // the package's directory, which scopes the names
	skips []string
	calls []string
}

// skipIndex reads every _test.go file under root/dirs, in every build tag, and returns the
// functions of each directory by name.
func skipIndex(root string, dirs ...string) (map[string]map[string][]*skipFunc, error) {
	byDir := map[string]map[string][]*skipFunc{}
	for _, dir := range dirs {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) && path == filepath.Join(root, dir) {
					return nil // a directory the tree lacks (a fixture's), not a failure to read it
				}
				return err
			}
			if d.IsDir() {
				if d.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, "_test.go") {
				return nil
			}
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			pkg := filepath.Dir(rel)
			if byDir[pkg] == nil {
				byDir[pkg] = map[string][]*skipFunc{}
			}
			for _, decl := range f.Decls {
				fd, ok := decl.(*ast.FuncDecl)
				if !ok || fd.Body == nil {
					continue
				}
				fn := &skipFunc{name: fd.Name.Name, dir: pkg}
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					switch fun := call.Fun.(type) {
					case *ast.Ident:
						fn.calls = append(fn.calls, fun.Name)
					case *ast.SelectorExpr:
						switch fun.Sel.Name {
						case "Skip", "Skipf", "SkipNow":
							fn.skips = append(fn.skips, fmt.Sprintf("%s:%d", rel, fset.Position(call.Pos()).Line))
						default:
							fn.calls = append(fn.calls, fun.Sel.Name)
						}
					}
					return true
				})
				byDir[pkg][fn.name] = append(byDir[pkg][fn.name], fn)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return byDir, nil
}

// isTest says whether a function is a test: Test, then a name that does not start lower case.
func isTest(name string) bool {
	rest, ok := strings.CutPrefix(name, "Test")
	return ok && (rest == "" || rest[0] < 'a' || rest[0] > 'z')
}

// namedInList reads the first cell of every table row in the "Expected skips" section of the
// README: each backticked name there, cut at the first "/" so that a subtest names its test. Only
// a table row is an entry; a name in the section's prose is not.
func namedInList(readme string) (names map[string]bool, err error) {
	names = map[string]bool{}
	in, found := false, false
	for _, line := range strings.Split(readme, "\n") {
		switch {
		case strings.TrimSpace(line) == expectedSkipsHeading:
			in, found = true, true
			continue
		case in && (strings.HasPrefix(line, "# ") || strings.HasPrefix(line, "## ")):
			in = false
		}
		if !in || !strings.HasPrefix(line, "|") {
			continue
		}
		cells := strings.Split(strings.Trim(line, "| \t"), "|")
		if len(cells) == 0 {
			continue
		}
		for _, m := range listedName.FindAllStringSubmatch(cells[0], -1) {
			if strings.Contains(m[1], "/") && !strings.HasPrefix(m[1], "Test") {
				continue // a package path (`cmd/goarchive/cmd`) beside a name, not a name
			}
			name, _, _ := strings.Cut(m[1], "/")
			names[name] = true
		}
	}
	if !found {
		return nil, fmt.Errorf("%s has no %q section", expectedSkipsFile, expectedSkipsHeading)
	}
	return names, nil
}

var listedName = regexp.MustCompile("`([A-Za-z0-9_/.-]+)`")

// skipProblems compares the skips the test files make with the names in the list: a skip whose
// test (or helper) the list does not name, a name that is no function, and a name that no longer
// reaches a skip.
func skipProblems(index map[string]map[string][]*skipFunc, named map[string]bool) []string {
	var problems []string

	// A function that no longer exists, or never skips.
	reaches := func(dir, name string) bool {
		seen := map[string]bool{}
		var walk func(string) bool
		walk = func(n string) bool {
			if seen[n] {
				return false
			}
			seen[n] = true
			for _, fn := range index[dir][n] {
				if len(fn.skips) > 0 {
					return true
				}
				for _, c := range fn.calls {
					if walk(c) {
						return true
					}
				}
			}
			return false
		}
		return walk(name)
	}
	for name := range named {
		exists, skips := false, false
		for dir := range index {
			if len(index[dir][name]) > 0 {
				exists = true
				skips = skips || reaches(dir, name)
			}
		}
		switch {
		case !exists:
			problems = append(problems, fmt.Sprintf("the list names %s, which is no test or helper in any test file", name))
		case !skips:
			problems = append(problems, fmt.Sprintf("the list names %s, which no longer skips", name))
		}
	}

	// A skip no entry covers. From each test, follow the calls, and stop at a named function: its
	// skips are the list's. What is reached unnamed and skips is a problem, attributed to the
	// tests that reach it.
	uncovered := map[string]map[string]bool{} // skip site -> the tests that reach it
	for dir, funcs := range index {
		for name := range funcs {
			if !isTest(name) {
				continue
			}
			seen := map[string]bool{}
			var walk func(string)
			walk = func(n string) {
				if seen[n] || named[n] {
					return
				}
				seen[n] = true
				for _, fn := range index[dir][n] {
					for _, site := range fn.skips {
						if uncovered[site] == nil {
							uncovered[site] = map[string]bool{}
						}
						uncovered[site][name] = true
					}
					for _, c := range fn.calls {
						walk(c)
					}
				}
			}
			walk(name)
		}
	}
	// A skip in a function no test reaches at all, named or not, and the list does not name.
	reachedAny := map[string]bool{}
	for dir, funcs := range index {
		for name := range funcs {
			if !isTest(name) {
				continue
			}
			var walk func(string)
			walk = func(n string) {
				if reachedAny[dir+"/"+n] {
					return
				}
				reachedAny[dir+"/"+n] = true
				for _, fn := range index[dir][n] {
					for _, c := range fn.calls {
						walk(c)
					}
				}
			}
			walk(name)
		}
	}
	for dir, funcs := range index {
		for name, fns := range funcs {
			if named[name] || reachedAny[dir+"/"+name] {
				continue
			}
			for _, fn := range fns {
				for _, site := range fn.skips {
					if uncovered[site] == nil {
						uncovered[site] = map[string]bool{}
					}
					uncovered[site]["(no test reaches it: "+name+")"] = true
				}
			}
		}
	}
	for site, tests := range uncovered {
		var ts []string
		for t := range tests {
			ts = append(ts, t)
		}
		sort.Strings(ts)
		problems = append(problems, fmt.Sprintf("%s: skips, and the list does not name %s", site, strings.Join(ts, ", ")))
	}
	sort.Strings(problems)
	return problems
}

// expectedSkipProblems reads root's test files and README and returns every problem.
func expectedSkipProblems(t *testing.T, root string) []string {
	t.Helper()
	index, err := skipIndex(root, "internal", "cmd", "tests")
	if err != nil {
		t.Fatal(err)
	}
	named, err := namedInList(readFixture(t, filepath.Join(root, expectedSkipsFile)))
	if err != nil {
		return []string{err.Error()}
	}
	return skipProblems(index, named)
}

// TestExpectedSkipsAreListed: a test that skips by design is named in tests/README.md's "Expected
// skips", so that a reader of a run can tell an expected SKIP from a finding, and a name there is a
// test that exists and still skips.
func TestExpectedSkipsAreListed(t *testing.T) {
	const list = "## Expected skips\n\n| Test | Why |\n|---|---|\n| `TestListed`, `TestWithSub/one_case` | because |\n| `shared` | helper |\n\nProse naming `TestInProse` is not an entry.\n\n## Next\n\n| `TestOutside` | not in the section |\n"
	// The same list with a package path beside a test name, as a row reads: "`TestPathed` (`cmd/goarchive/cmd`)".
	pathList := strings.Replace(list, "| `shared` | helper |", "| `shared` | helper |\n| `TestPathed` (`cmd/goarchive/cmd`) | in a package |", 1)
	for _, tc := range []struct {
		name string
		src  string // a _test.go file of package p
		list string // the README
		want string // a problem containing it; "" for none
	}{
		{"a listed test", "func TestListed(t *testing.T) { t.Skip(\"x\") }", list, ""},
		{"an unlisted test", "func TestOther(t *testing.T) { t.Skip(\"x\") }", list, "TestOther"},
		{"Skipf", "func TestOther(t *testing.T) { t.Skipf(\"x %d\", 1) }", list, "TestOther"},
		{"SkipNow", "func TestOther(t *testing.T) { t.SkipNow() }", list, "TestOther"},
		{"a skip in a subtest", "func TestOther(t *testing.T) { t.Run(\"a\", func(t *testing.T) { t.Skip(\"x\") }) }", list, "TestOther"},
		{"a skip in a closure", "func TestOther(t *testing.T) { f := func() { t.Skip(\"x\") }; f() }", list, "TestOther"},
		{"a subtest entry names its test", "func TestWithSub(t *testing.T) { t.Skip(\"x\") }\nfunc TestListed(t *testing.T) { t.Skip(\"x\") }", list, ""},
		{"a name in prose", "func TestInProse(t *testing.T) { t.Skip(\"x\") }", list, "TestInProse"},
		{"a name after the section", "func TestOutside(t *testing.T) { t.Skip(\"x\") }", list, "TestOutside"},
		{"a test that does not skip", "func TestOther(t *testing.T) {}\nfunc TestListed(t *testing.T) { t.Skip(\"x\") }", list, ""},
		{"a helper's skip names its callers", "func hold(t *testing.T) { t.Skip(\"x\") }\nfunc TestListed(t *testing.T) { hold(t) }\nfunc TestB(t *testing.T) { hold(t) }", list, "TestB"},
		{"a helper through a helper", "func deep(t *testing.T) { t.Skip(\"x\") }\nfunc mid(t *testing.T) { deep(t) }\nfunc TestOther(t *testing.T) { mid(t) }", list, "TestOther"},
		{"a method helper", "type e struct{}\nfunc (e) hold(t *testing.T) { t.Skip(\"x\") }\nfunc TestOther(t *testing.T) { e{}.hold(t) }", list, "TestOther"},
		{"a named helper covers its callers", "func shared(t *testing.T) { t.Skip(\"x\") }\nfunc TestOther(t *testing.T) { shared(t) }\nfunc TestListed(t *testing.T) { t.Skip(\"x\") }", list, ""},
		{"a helper reached only through a listed test is covered", "func hold(t *testing.T) { t.Skip(\"x\") }\nfunc TestListed(t *testing.T) { hold(t) }", list, ""},
		{"a path in the first cell is not a name", "func TestPathed(t *testing.T) { t.Skip(\"x\") }\nfunc TestListed(t *testing.T) { t.Skip(\"x\") }", pathList, ""},
		{"a helper no test reaches", "func orphan(t *testing.T) { t.Skip(\"x\") }\nfunc TestListed(t *testing.T) { t.Skip(\"x\") }", list, "orphan"},
		{"a listed test that no longer skips", "func TestListed(t *testing.T) {}", list, "TestListed, which no longer skips"},
		{"a lower-case word after Test is no test", "func Testing(t *testing.T) { t.Skip(\"x\") }", list, "(no test reaches it: Testing)"},
		{"no section", "func TestListed(t *testing.T) { t.Skip(\"x\") }", "# README\n\nno list\n", "no \"## Expected skips\" section"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeFixture(t, filepath.Join(root, "tests", "README.md"), tc.list)
			writeFixture(t, filepath.Join(root, "internal", "p", "p_test.go"),
				"package p\n\nimport \"testing\"\n\n"+tc.src+"\n")
			// the names the list holds that a fixture does not declare would be stale: declare them, skipping.
			writeFixture(t, filepath.Join(root, "internal", "q", "q_test.go"),
				"package q\n\nimport \"testing\"\n\nfunc TestWithSub(t *testing.T) { t.Skip(\"x\") }\nfunc shared(t *testing.T) { t.Skip(\"x\") }\n")
			problems := expectedSkipProblems(t, root)
			if tc.want == "" {
				if len(problems) > 0 {
					t.Errorf("problems %q, want none", problems)
				}
				return
			}
			if !containsAny(problems, tc.want) {
				t.Errorf("problems %q, want one containing %q", problems, tc.want)
			}
		})
	}

	t.Run("a listed name that is no function", func(t *testing.T) {
		root := t.TempDir()
		writeFixture(t, filepath.Join(root, "tests", "README.md"), list)
		writeFixture(t, filepath.Join(root, "internal", "p", "p_test.go"),
			"package p\n\nimport \"testing\"\n\nfunc TestListed(t *testing.T) { t.Skip(\"x\") }\n")
		problems := expectedSkipProblems(t, root)
		for _, want := range []string{"TestWithSub, which is no test or helper", "shared, which is no test or helper"} {
			if !containsAny(problems, want) {
				t.Errorf("problems %q, want one containing %q", problems, want)
			}
		}
	})

	t.Run("a helper's name is scoped to its package", func(t *testing.T) {
		root := t.TempDir()
		writeFixture(t, filepath.Join(root, "tests", "README.md"), list)
		writeFixture(t, filepath.Join(root, "internal", "p", "p_test.go"),
			"package p\n\nimport \"testing\"\n\nfunc hold(t *testing.T) { t.Skip(\"x\") }\n")
		writeFixture(t, filepath.Join(root, "internal", "q", "q_test.go"),
			"package q\n\nimport \"testing\"\n\nfunc hold(t *testing.T) {}\nfunc TestListed(t *testing.T) { hold(t); t.Skip(\"x\") }\nfunc TestWithSub(t *testing.T) { t.Skip(\"x\") }\nfunc shared(t *testing.T) { t.Skip(\"x\") }\n")
		problems := expectedSkipProblems(t, root)
		if !containsAny(problems, "no test reaches it: hold") {
			t.Errorf("problems %q, want p's own hold to be unreached", problems)
		}
	})

	t.Run("a file of another build tag is read", func(t *testing.T) {
		root := t.TempDir()
		writeFixture(t, filepath.Join(root, "tests", "README.md"), list)
		writeFixture(t, filepath.Join(root, "internal", "p", "p_windows_test.go"),
			"//go:build windows\n\npackage p\n\nimport \"testing\"\n\nfunc TestOther(t *testing.T) { t.Skip(\"x\") }\n")
		writeFixture(t, filepath.Join(root, "internal", "q", "q_test.go"),
			"package q\n\nimport \"testing\"\n\nfunc TestListed(t *testing.T) { t.Skip(\"x\") }\nfunc TestWithSub(t *testing.T) { t.Skip(\"x\") }\nfunc shared(t *testing.T) { t.Skip(\"x\") }\n")
		if problems := expectedSkipProblems(t, root); !containsAny(problems, "TestOther") {
			t.Errorf("problems %q, want TestOther of the windows file", problems)
		}
	})

	t.Run("the repository", func(t *testing.T) {
		root, err := filepath.Abs("../..")
		if err != nil {
			t.Fatal(err)
		}
		if problems := expectedSkipProblems(t, root); len(problems) > 0 {
			t.Errorf("tests/README.md's %q and the tests' skips disagree:\n%s",
				expectedSkipsHeading, strings.Join(problems, "\n"))
		}
		index, err := skipIndex(root, "internal", "cmd", "tests")
		if err != nil {
			t.Fatal(err)
		}
		sites := 0
		for _, funcs := range index {
			for _, fns := range funcs {
				for _, fn := range fns {
					sites += len(fn.skips)
				}
			}
		}
		if sites < 20 {
			t.Errorf("only %d skip sites found: the reader no longer sees the tests' skips", sites)
		}
	})
}

func containsAny(problems []string, want string) bool {
	for _, p := range problems {
		if strings.Contains(p, want) {
			return true
		}
	}
	return false
}

// TestExpectedSkipsResolveByPackageAndReceiver (Codex S1 findings 2 and 3): a list entry is a
// function of one package, and a call to a method is not a call to every method of that name.
// A path in backticks after names in a row qualifies them: "`TestShared` (`internal/a`)".
func TestExpectedSkipsResolveByPackageAndReceiver(t *testing.T) {
	const skips = `t.Skip("x")`
	for _, tc := range []struct {
		name  string
		list  string            // the rows of the table
		files map[string]string // package directory under internal -> the declarations of its test file
		want  []string          // each a problem containing it; none for no problem
	}{
		{
			"a qualified row names its own package",
			"| `TestShared` (`internal/a`) | x |",
			map[string]string{"a": "func TestShared(t *testing.T) { " + skips + " }"},
			nil,
		},
		{
			"an unlisted namesake in another package",
			"| `TestShared` (`internal/a`) | x |",
			map[string]string{
				"a": "func TestShared(t *testing.T) { " + skips + " }",
				"b": "func TestShared(t *testing.T) { " + skips + " }",
			},
			[]string{"internal/b/x_test.go"},
		},
		{
			"a stale qualified row kept alive by a namesake elsewhere",
			"| `TestShared` (`internal/a`), `TestShared` (`internal/b`) | x |",
			map[string]string{
				"a": "func TestShared(t *testing.T) {}",
				"b": "func TestShared(t *testing.T) { " + skips + " }",
			},
			[]string{"TestShared in internal/a, which no longer skips"},
		},
		{
			"a name in two packages needs its package",
			"| `TestShared` | x |",
			map[string]string{
				"a": "func TestShared(t *testing.T) { " + skips + " }",
				"b": "func TestShared(t *testing.T) { " + skips + " }",
			},
			[]string{"TestShared is declared in internal/a, internal/b"},
		},
		{
			"a name in one package needs none",
			"| `TestShared` | x |",
			map[string]string{
				"a": "func TestShared(t *testing.T) { " + skips + " }",
				"b": "func TestOther(t *testing.T) {}",
			},
			nil,
		},
		{
			"a listed helper still covers its callers in its own package",
			"| `hold` (`internal/a`) | x |",
			map[string]string{
				"a": "func hold(t *testing.T) { " + skips + " }\nfunc TestUses(t *testing.T) { hold(t) }",
				"b": "func hold(t *testing.T) {}\nfunc TestOrdinary(t *testing.T) { hold(t) }",
			},
			nil,
		},
		{
			"two receivers that share a method name",
			"| `TestListed` | x |",
			map[string]string{"a": `type unavailable struct{}
func (unavailable) prepare(t *testing.T) { ` + skips + ` }
type ready struct{}
func (ready) prepare(t *testing.T) {}
func TestListed(t *testing.T) { unavailable{}.prepare(t) }
func TestOrdinary(t *testing.T) { ready{}.prepare(t) }`},
			nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeFixture(t, filepath.Join(root, "tests", "README.md"),
				"## Expected skips\n\n| Test | Why |\n|---|---|\n"+tc.list+"\n")
			for dir, decls := range tc.files {
				writeFixture(t, filepath.Join(root, "internal", dir, "x_test.go"),
					"package "+dir+"\n\nimport \"testing\"\n\n"+decls+"\n")
			}
			problems := expectedSkipProblems(t, root)
			if len(tc.want) == 0 {
				if len(problems) > 0 {
					t.Errorf("problems %q, want none", problems)
				}
				return
			}
			for _, w := range tc.want {
				if !containsAny(problems, w) {
					t.Errorf("problems %q, want one containing %q", problems, w)
				}
			}
		})
	}
}
