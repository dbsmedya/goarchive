package harness

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// The control socket's protocol is a public contract (TEL-1, spec §4.3, D19): README_OPERATIONS.md's
// *Runtime control* section lists its actions, its settings, its error reason words and the fields of
// status-json, and a protocol change must update that section in the same change. These tests compare
// each list with the server's own tables, read from its source, so that the doc and the server cannot
// drift apart. The doc's tables are found by their lead-in labels and their first column is compared.
const (
	contractDocPath  = "docs/README_OPERATIONS.md"
	commandsGoPath   = "internal/control/commands.go"
	protocolGoPath   = "internal/control/protocol.go"
	statusGoPath     = "internal/control/status.go"
	tuningKeysPath   = "internal/tuning/keys.go"
	tuningRefusePath = "internal/tuning/refusal.go"
)

// docTable is the first column of the table that follows the line starting with label, with the
// backticks taken off.
func docTable(t *testing.T, doc, label string) []string {
	t.Helper()
	lines := strings.Split(doc, "\n")
	start := slices.IndexFunc(lines, func(l string) bool { return strings.HasPrefix(l, label) })
	if start < 0 {
		t.Fatalf("%s: no line starts with %q", contractDocPath, label)
	}
	var out []string
	inTable := false
	for _, l := range lines[start+1:] {
		switch {
		case strings.HasPrefix(l, "|"):
			inTable = true
			cells := strings.Split(strings.Trim(l, "| "), "|")
			first := strings.Trim(strings.TrimSpace(cells[0]), "`")
			if first == "" || strings.HasPrefix(first, "---") || strings.Contains(first, "---") {
				continue
			}
			out = append(out, first)
		case inTable:
			return out[1:] // the header row is the first
		}
	}
	return out[1:]
}

// actionNames are the string cases of commands.action: the server's actions.
func actionNames(t *testing.T) []string {
	t.Helper()
	f := parseGo(t, commandsGoPath)
	var out []string
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "action" || fn.Recv == nil {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if cc, ok := n.(*ast.CaseClause); ok {
				for _, e := range cc.List {
					if lit, ok := e.(*ast.BasicLit); ok && lit.Kind == token.STRING {
						s, _ := strconv.Unquote(lit.Value)
						out = append(out, s)
					}
				}
			}
			return true
		})
	}
	if len(out) == 0 {
		t.Fatalf("%s: no `case \"…\":` in commands.action", commandsGoPath)
	}
	return out
}

func parseGo(t *testing.T, path string) *ast.File {
	t.Helper()
	src := readRepoFile(t, fromRoot(path)...)
	f, err := parser.ParseFile(token.NewFileSet(), path, src, 0)
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return f
}

var (
	settingConst = regexp.MustCompile(`Key = "([a-z_]+)"`)
	reasonConst  = regexp.MustCompile(`Reason = "([a-z]+)"`)
)

func matches(t *testing.T, re *regexp.Regexp, path string) []string {
	t.Helper()
	var out []string
	for _, m := range re.FindAllStringSubmatch(readRepoFile(t, fromRoot(path)...), -1) {
		out = append(out, m[1])
	}
	return out
}

// statusFields are the leaf paths of the statusJSON struct, as the doc writes them: `.` between
// names, `[]` after a list, `<name>` for a map's key.
func statusFields(t *testing.T) []string {
	t.Helper()
	f := parseGo(t, statusGoPath)
	structs := map[string]*ast.StructType{}
	for _, d := range f.Decls {
		if gd, ok := d.(*ast.GenDecl); ok {
			for _, sp := range gd.Specs {
				if ts, ok := sp.(*ast.TypeSpec); ok {
					if st, ok := ts.Type.(*ast.StructType); ok {
						structs[ts.Name.Name] = st
					}
				}
			}
		}
	}
	root, ok := structs["statusJSON"]
	if !ok {
		t.Fatalf("%s: no statusJSON struct", statusGoPath)
	}
	return jsonLeaves(structs, root, "")
}

func jsonLeaves(structs map[string]*ast.StructType, st *ast.StructType, prefix string) []string {
	var out []string
	for _, field := range st.Fields.List {
		tag := ""
		if field.Tag != nil {
			s, _ := strconv.Unquote(field.Tag.Value)
			tag = reflect.StructTag(s).Get("json")
		}
		name, _, _ := strings.Cut(tag, ",")
		if name == "" || name == "-" {
			continue
		}
		path, typ := prefix+name, field.Type
		for {
			switch x := typ.(type) {
			case *ast.StarExpr:
				typ = x.X
				continue
			case *ast.ArrayType:
				path += "[]"
				typ = x.Elt
				continue
			case *ast.MapType:
				path += ".<name>"
				typ = x.Value
				continue
			}
			break
		}
		if id, ok := typ.(*ast.Ident); ok && structs[id.Name] != nil {
			out = append(out, jsonLeaves(structs, structs[id.Name], path+".")...)
			continue
		}
		out = append(out, path)
	}
	return out
}

// sameSet reports what is in a and not b, and in b and not a, ignoring order.
func sameSet(a, b []string) (onlyA, onlyB []string) {
	for _, x := range a {
		if !slices.Contains(b, x) {
			onlyA = append(onlyA, x)
		}
	}
	for _, x := range b {
		if !slices.Contains(a, x) {
			onlyB = append(onlyB, x)
		}
	}
	return onlyA, onlyB
}

// TestProtocolContractMatchesDoc (spec §7 row 10): the actions, the settings, the reason words and the
// status-json fields the doc lists are exactly the server's full tables, not the list `help` gives for
// one job type. The request and reason examples are the behaviour tests; this only guards against drift.
func TestProtocolContractMatchesDoc(t *testing.T) {
	doc := readRepoFile(t, fromRoot(contractDocPath)...)
	for _, tc := range []struct {
		what  string
		label string
		code  []string
	}{
		{"actions", "**Actions.**", actionNames(t)},
		{"settings", "**Settings.**", matches(t, settingConst, tuningKeysPath)},
		{"error reasons", "**Errors.**", append(matches(t, reasonConst, tuningRefusePath), matches(t, reasonConst, protocolGoPath)...)},
		{"status-json fields", "**`status-json`** is one object.", statusFields(t)},
	} {
		got := docTable(t, doc, tc.label)
		if len(tc.code) == 0 || len(got) == 0 {
			t.Fatalf("%s: the doc lists %d and the code %d: a table was not found", tc.what, len(got), len(tc.code))
		}
		if onlyDoc, onlyCode := sameSet(got, tc.code); len(onlyDoc) > 0 || len(onlyCode) > 0 {
			t.Errorf("%s: the doc and the server differ.\n  only in %s: %v\n  only in the code: %v", tc.what, contractDocPath, onlyDoc, onlyCode)
		}
		if len(got) != len(tc.code) {
			t.Errorf("%s: the doc lists %d, the code has %d (a name is repeated)", tc.what, len(got), len(tc.code))
		}
	}
}

// TestContractComparison: the comparison itself notices a name missing on either side and a repeated
// one, and reads the doc's tables and the status struct's paths as designed (nothing of the repository's
// code or docs is altered to show it).
func TestContractComparison(t *testing.T) {
	onlyA, onlyB := sameSet([]string{"help", "status", "frob"}, []string{"help", "status", "pause"})
	if !slices.Equal(onlyA, []string{"frob"}) || !slices.Equal(onlyB, []string{"pause"}) {
		t.Errorf("sameSet = %v, %v", onlyA, onlyB)
	}
	doc := "intro\n\n**Things.**\n\n| Name | Meaning |\n|---|---|\n| `a` | one |\n| `b.c` | two |\n\nafter\n| `z` | not part |\n"
	if got := docTable(t, doc, "**Things.**"); !slices.Equal(got, []string{"a", "b.c"}) {
		t.Errorf("docTable = %v, want a and b.c, the header and the table after the text left out", got)
	}
	fields := statusFields(t)
	for _, want := range []string{"protocol", "activity.step", "pause.requested[]", "hold.servers[].lag_seconds", "tables.<name>.copied", "settings.<name>.configured", "replication.servers[].checked_at", "changes"} {
		if !slices.Contains(fields, want) {
			t.Errorf("statusFields lacks %q: %v", want, fields)
		}
	}
	for _, not := range []string{"activity", "hold", "hold.servers", "tables"} {
		if slices.Contains(fields, not) {
			t.Errorf("statusFields has the container %q, want leaves only", not)
		}
	}
}
