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
// What a list entry names. A function of one package: `TestShared` when no other package declares
// it, and otherwise with its package, a path in backticks after the names it qualifies in the same
// cell: "`TestShared` (`internal/a`)". A method is `Receiver.name`. A subtest is not read: an
// entry such as `TestA/some_case` names TestA, and which of its subtests skip is the list's own
// account.
//
// What it reads, and what it does not see. The call graph is by name within one directory (one
// package, every build tag). A call to a function is followed to that function. A call through a
// selector is followed to the method of that name when one receiver type declares it, and the
// declarations of one method in files of different build tags are one method. When several
// receiver types of the package declare the name, the receiver is not known without types, so the
// call is followed only to cover: a skip reached that way is covered by a listed test or method
// that may reach it, and no test is made to list it for that call alone. A call through a function
// value is not followed. The tests that a matrix row excludes with -skip never reach a skip call,
// so they are not listed; the list says so in prose.

const (
	expectedSkipsFile    = "tests/README.md"
	expectedSkipsHeading = "## Expected skips"
)

// skipFunc is one declaration in a test file: whether it calls Skip itself, where, and what it
// calls by name. recv is the receiver's type name, "" for a function.
type skipFunc struct {
	recv, name string
	skips      []string
	plain      []string // names called as name(...)
	selected   []string // names called as x.name(...)
}

// id is how the list names it: name, or Receiver.name.
func (f *skipFunc) id() string {
	if f.recv == "" {
		return f.name
	}
	return f.recv + "." + f.name
}

// pkgFuncs is the declarations of one directory by id, and the receivers that declare each method name.
type pkgFuncs struct {
	byID      map[string][]*skipFunc
	receivers map[string]map[string]bool // method name -> receiver types
}

// skipIndex reads every _test.go file under root/dirs, in every build tag, by directory.
type skipIndex map[string]*pkgFuncs

func readSkipIndex(root string, dirs ...string) (skipIndex, error) {
	index := skipIndex{}
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
			rel = filepath.ToSlash(rel)
			pkg := filepath.ToSlash(filepath.Dir(rel))
			if index[pkg] == nil {
				index[pkg] = &pkgFuncs{byID: map[string][]*skipFunc{}, receivers: map[string]map[string]bool{}}
			}
			for _, decl := range f.Decls {
				fd, ok := decl.(*ast.FuncDecl)
				if !ok || fd.Body == nil {
					continue
				}
				fn := &skipFunc{name: fd.Name.Name, recv: receiverName(fd)}
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					switch fun := call.Fun.(type) {
					case *ast.Ident:
						fn.plain = append(fn.plain, fun.Name)
					case *ast.SelectorExpr:
						switch fun.Sel.Name {
						case "Skip", "Skipf", "SkipNow":
							fn.skips = append(fn.skips, fmt.Sprintf("%s:%d", rel, fset.Position(call.Pos()).Line))
						default:
							fn.selected = append(fn.selected, fun.Sel.Name)
						}
					}
					return true
				})
				p := index[pkg]
				p.byID[fn.id()] = append(p.byID[fn.id()], fn)
				if fn.recv != "" {
					if p.receivers[fn.name] == nil {
						p.receivers[fn.name] = map[string]bool{}
					}
					p.receivers[fn.name][fn.recv] = true
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return index, nil
}

// receiverName is the type name of a method's receiver, "" for a function.
func receiverName(fd *ast.FuncDecl) string {
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return ""
	}
	t := fd.Recv.List[0].Type
	for {
		switch x := t.(type) {
		case *ast.StarExpr:
			t = x.X
		case *ast.IndexExpr:
			t = x.X
		case *ast.IndexListExpr:
			t = x.X
		case *ast.ParenExpr:
			t = x.X
		case *ast.Ident:
			return x.Name
		default:
			return "?"
		}
	}
}

// sites is how many skip calls the files hold.
func (index skipIndex) sites() int {
	n := 0
	for _, p := range index {
		for _, fns := range p.byID {
			for _, fn := range fns {
				n += len(fn.skips)
			}
		}
	}
	return n
}

// edge is a call from one declaration to another by its id. possible is a call through a method name
// that several receivers declare: it may be to any of them.
type edge struct {
	to       string
	possible bool
}

// callees are the declarations fn calls, within its directory.
func (p *pkgFuncs) callees(fn *skipFunc) []edge {
	var out []edge
	for _, name := range fn.plain {
		if len(p.byID[name]) > 0 {
			out = append(out, edge{to: name})
		}
	}
	for _, name := range fn.selected {
		recvs := p.receivers[name]
		for r := range recvs {
			out = append(out, edge{to: r + "." + name, possible: len(recvs) > 1})
		}
	}
	return out
}

// reach is every id reachable from start in a directory, start included. definite follows only the
// calls whose target is known; stop is an id the walk does not enter (a listed one).
func (p *pkgFuncs) reach(start string, definite bool, stop func(string) bool) map[string]bool {
	seen := map[string]bool{}
	var walk func(string)
	walk = func(id string) {
		if seen[id] || (id != start && stop != nil && stop(id)) {
			return
		}
		seen[id] = true
		for _, fn := range p.byID[id] {
			for _, e := range p.callees(fn) {
				if definite && e.possible {
					continue
				}
				walk(e.to)
			}
		}
	}
	walk(start)
	return seen
}

// skips says whether any declaration of any id in set calls Skip.
func (p *pkgFuncs) skips(set map[string]bool) bool {
	for id := range set {
		for _, fn := range p.byID[id] {
			if len(fn.skips) > 0 {
				return true
			}
		}
	}
	return false
}

// isTest says whether a function is a test: Test, then a name that does not start lower case.
func isTest(id string) bool {
	rest, ok := strings.CutPrefix(id, "Test")
	return ok && !strings.Contains(id, ".") && (rest == "" || rest[0] < 'a' || rest[0] > 'z')
}

// entry is one name in the list: its id, and the package a path after it in the same cell gave it.
type entry struct{ dir, id string }

func (e entry) String() string {
	if e.dir == "" {
		return e.id
	}
	return e.id + " in " + e.dir
}

// namedInList reads the first cell of every table row in the "Expected skips" section of the
// README: each backticked name there, cut at the first "/" so that a subtest names its test, and
// qualified by the next backticked path in the cell. Only a table row is an entry; a name in the
// section's prose is not.
func namedInList(readme string) (entries []entry, err error) {
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
		var pending []int // entries of this cell still without a package
		for _, m := range listedName.FindAllStringSubmatch(cells[0], -1) {
			tok := m[1]
			if strings.Contains(tok, "/") && !strings.Contains(tok, ".") && !strings.HasPrefix(tok, "Test") {
				for _, i := range pending { // a package path qualifies the names before it
					entries[i].dir = tok
				}
				pending = nil
				continue
			}
			id, _, _ := strings.Cut(tok, "/")
			entries = append(entries, entry{id: id})
			pending = append(pending, len(entries)-1)
		}
	}
	if !found {
		return nil, fmt.Errorf("%s has no %q section", expectedSkipsFile, expectedSkipsHeading)
	}
	return entries, nil
}

var listedName = regexp.MustCompile("`([A-Za-z0-9_/.-]+)`")

// skipProblems compares the skips the test files make with the names in the list: a skip whose
// test (or helper) the list does not name, a name that is no function or is declared in several
// packages, and a name that no longer reaches a skip.
func skipProblems(index skipIndex, entries []entry) []string {
	var problems []string
	dirs := make([]string, 0, len(index))
	for dir := range index {
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)

	// Resolve each entry to one declaration of one package.
	named := map[string]bool{} // dir + "\x00" + id
	key := func(dir, id string) string { return dir + "\x00" + id }
	for _, e := range entries {
		var homes []string
		switch {
		case e.dir != "":
			if p := index[e.dir]; p != nil && len(p.byID[e.id]) > 0 {
				homes = []string{e.dir}
			} else {
				problems = append(problems, fmt.Sprintf("the list names %s, which is no test or helper there", e))
				continue
			}
		default:
			for _, dir := range dirs {
				if len(index[dir].byID[e.id]) > 0 {
					homes = append(homes, dir)
				}
			}
			switch len(homes) {
			case 0:
				problems = append(problems, fmt.Sprintf("the list names %s, which is no test or helper in any test file", e))
				continue
			case 1:
			default:
				problems = append(problems, fmt.Sprintf("%s is declared in %s: the list must name its package, as in `%s` (`%s`)",
					e.id, strings.Join(homes, ", "), e.id, homes[0]))
				continue
			}
		}
		dir := homes[0]
		named[key(dir, e.id)] = true
		if p := index[dir]; !p.skips(p.reach(e.id, false, nil)) {
			problems = append(problems, fmt.Sprintf("the list names %s in %s, which no longer skips", e.id, dir))
		}
	}

	// Each skip site must be covered: named itself, or reached only through named functions, or
	// reached through a method name several receivers share while a listed function may reach it.
	for _, dir := range dirs {
		p := index[dir]
		isNamed := func(id string) bool { return named[key(dir, id)] }
		var ids []string
		for id := range p.byID {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		var tests []string
		for _, id := range ids {
			if isTest(id) {
				tests = append(tests, id)
			}
		}
		for _, id := range ids {
			if isNamed(id) {
				continue
			}
			var sites []string
			for _, fn := range p.byID[id] {
				sites = append(sites, fn.skips...)
			}
			if len(sites) == 0 {
				continue
			}
			sort.Strings(sites)
			// The tests that reach it by calls whose target is known, without passing a listed function.
			var blamed, maybe []string
			for _, t := range tests {
				if isNamed(t) {
					continue
				}
				if p.reach(t, true, isNamed)[id] {
					blamed = append(blamed, t)
				} else if p.reach(t, false, isNamed)[id] {
					maybe = append(maybe, t)
				}
			}
			if len(blamed) > 0 {
				for _, site := range sites {
					problems = append(problems, fmt.Sprintf("%s: skips, and the list does not name %s", site, strings.Join(blamed, ", ")))
				}
				continue
			}
			// Covered when a listed function may reach it.
			covered := false
			for other := range p.byID {
				if isNamed(other) && p.reach(other, false, nil)[id] {
					covered = true
					break
				}
			}
			if covered {
				continue
			}
			for _, site := range sites {
				switch {
				case len(maybe) > 0:
					problems = append(problems, fmt.Sprintf("%s: skips, and the list names neither %s nor any test that reaches it through a method name several receivers share (%s)",
						site, id, strings.Join(maybe, ", ")))
				default:
					problems = append(problems, fmt.Sprintf("%s: skips, and the list does not name (no test reaches it: %s)", site, id))
				}
			}
		}
	}
	sort.Strings(problems)
	return problems
}

// expectedSkipProblems reads root's test files and README and returns every problem.
func expectedSkipProblems(t *testing.T, root string) []string {
	t.Helper()
	index, err := readSkipIndex(root, "internal", "cmd", "tests")
	if err != nil {
		t.Fatal(err)
	}
	entries, err := namedInList(readFixture(t, filepath.Join(root, expectedSkipsFile)))
	if err != nil {
		return []string{err.Error()}
	}
	return skipProblems(index, entries)
}

// TestExpectedSkipsAreListed: a test that skips by design is named in tests/README.md's "Expected
// skips", so that a reader of a run can tell an expected SKIP from a finding, and a name there is a
// test that exists and still skips.
func TestExpectedSkipsAreListed(t *testing.T) {
	const list = "## Expected skips\n\n| Test | Why |\n|---|---|\n| `TestListed`, `TestWithSub/one_case` | because |\n| `shared` | helper |\n\nProse naming `TestInProse` is not an entry.\n\n## Next\n\n| `TestOutside` | not in the section |\n"
	// The same list with a package path after a test name, as a row reads: "`TestPathed` (`internal/p`)".
	pathList := strings.Replace(list, "| `shared` | helper |", "| `shared` | helper |\n| `TestPathed` (`internal/p`) | in a package |", 1)
	strayList := strings.Replace(list, "| `shared` | helper |", "| `shared` | helper |\n| `TestPathed` (`internal/q`) | in another package |", 1)
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
		{"a path after a name is its package, not a name", "func TestPathed(t *testing.T) { t.Skip(\"x\") }\nfunc TestListed(t *testing.T) { t.Skip(\"x\") }", pathList, ""},
		{"a path that is not the name's package", "func TestPathed(t *testing.T) { t.Skip(\"x\") }\nfunc TestListed(t *testing.T) { t.Skip(\"x\") }", strayList, "TestPathed in internal/q, which is no test or helper there"},
		{"a helper no test reaches", "func orphan(t *testing.T) { t.Skip(\"x\") }\nfunc TestListed(t *testing.T) { t.Skip(\"x\") }", list, "orphan"},
		{"a listed test that no longer skips", "func TestListed(t *testing.T) {}", list, "TestListed in internal/p, which no longer skips"},
		{"a lower-case word after Test is no test", "func Testing(t *testing.T) { t.Skip(\"x\") }", list, "(no test reaches it: Testing)"},
		{"no section", "func TestListed(t *testing.T) { t.Skip(\"x\") }", "# README\n\nno list\n", "no \"## Expected skips\" section"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeFixture(t, filepath.Join(root, "tests", "README.md"), tc.list)
			writeFixture(t, filepath.Join(root, "internal", "p", "p_test.go"),
				"package p\n\nimport \"testing\"\n\n"+tc.src+"\n")
			// the names the list holds that a fixture does not declare would be stale: declare them in
			// another package, skipping, unless the case declares them (a name in two packages needs its package).
			other := ""
			for _, name := range []string{"TestWithSub", "shared"} {
				if !strings.Contains(tc.src, "func "+name+"(") {
					other += "func " + name + "(t *testing.T) { t.Skip(\"x\") }\n"
				}
			}
			writeFixture(t, filepath.Join(root, "internal", "q", "q_test.go"), "package q\n\nimport \"testing\"\n\n"+other)
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
		index, err := readSkipIndex(root, "internal", "cmd", "tests")
		if err != nil {
			t.Fatal(err)
		}
		sites := index.sites()
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
			"a method that two receivers share, reached only by an unlisted test, is reported",
			"",
			map[string]string{"a": `type unavailable struct{}
func (unavailable) prepare(t *testing.T) { ` + skips + ` }
type ready struct{}
func (ready) prepare(t *testing.T) {}
func TestOrdinary(t *testing.T) { ready{}.prepare(t) }`},
			[]string{"unavailable.prepare", "TestOrdinary"},
		},
		{
			"a method of one receiver is listed as Receiver.name",
			"| `unavailable.prepare` | x |",
			map[string]string{"a": `type unavailable struct{}
func (unavailable) prepare(t *testing.T) { ` + skips + ` }
type ready struct{}
func (ready) prepare(t *testing.T) {}
func TestOrdinary(t *testing.T) { ready{}.prepare(t) }`},
			nil,
		},
		{
			"the declarations of one method in files of different build tags are one method",
			"",
			map[string]string{
				"a":               "type env struct{}\nfunc (env) hold(t *testing.T) { " + skips + " }\nfunc TestOrdinary(t *testing.T) { env{}.hold(t) }",
				"a:other_windows": "func (env) hold(t *testing.T) {}",
			},
			[]string{"TestOrdinary"},
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
			for key, decls := range tc.files {
				dir, file, _ := strings.Cut(key, ":") // "a" is a/x_test.go; "a:other" is a/other_test.go
				if file == "" {
					file = "x"
				}
				writeFixture(t, filepath.Join(root, "internal", dir, file+"_test.go"),
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
