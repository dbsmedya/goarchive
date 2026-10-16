package harness

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const packetTest = "TestMySQLArchiveValueLargerThanThePacket"

// packetRowProblem says why the matrix row, the packet test and the skip lists do not tie
// packetTest to its row, or returns "" when they do. rows is rows.tsv, read with the runner's
// own filter (run-matrix.sh rows(): drop '#' lines and blank lines, split on TAB); src is
// cmd/goarchive/cmd/packet_integration_test.go; skips is gh-skip.txt; runner is run-matrix.sh.
func packetRowProblem(rows, src, skips, runner string) string {
	file, err := parser.ParseFile(token.NewFileSet(), "packet_integration_test.go", src, 0)
	if err != nil {
		return "parse: " + err.Error()
	}
	name := ""
	var test *ast.FuncDecl
	for _, d := range file.Decls {
		switch d := d.(type) {
		case *ast.GenDecl:
			if d.Tok != token.CONST {
				continue
			}
			for _, s := range d.Specs {
				vs := s.(*ast.ValueSpec)
				for i, n := range vs.Names {
					if n.Name != "packetDstRow" || i >= len(vs.Values) {
						continue
					}
					if lit, ok := vs.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
						name, _ = strconv.Unquote(lit.Value)
					}
				}
			}
		case *ast.FuncDecl:
			if d.Name.Name == packetTest {
				test = d
			}
		}
	}
	if name == "" {
		return "no const packetDstRow"
	}
	if test == nil {
		return "no " + packetTest
	}
	uses := false
	ast.Inspect(test.Body, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok && id.Name == "packetDstRow" {
			uses = true
		}
		return !uses
	})
	if !uses {
		return packetTest + " does not use packetDstRow"
	}
	found := false
	for _, line := range strings.Split(rows, "\n") {
		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			continue
		}
		if f := strings.Split(line, "\t"); f[0] == name {
			if len(f) < 4 || f[1] != "dynamic" || !strings.HasPrefix(f[2], "dst:") {
				return "row " + name + " is not a dynamic dst: row"
			}
			found = true
		}
	}
	if !found {
		return "no row " + name + " in rows.tsv"
	}
	for _, line := range strings.Split(skips, "\n") {
		if f := strings.Fields(line); len(f) > 0 && f[0] == packetTest {
			return "gh-skip.txt skips " + packetTest
		}
	}
	for _, line := range strings.Split(runner, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "SKIP=") && strings.Contains(line, packetTest) {
			return "run-matrix.sh skips " + packetTest
		}
	}
	return ""
}

// TestMatrixPacketRowDeclared (PKT-1, Sonnet F2 and R2-2): the packet test skips everywhere but
// its matrix row, keyed on the row's name, so the name must be one literal in rows.tsv and in
// the test, the row must set the destination only, and nothing may skip the test. Green on
// arrival; its red is the table of mismatched copies, each failing for the reason it names.
func TestMatrixPacketRowDeclared(t *testing.T) {
	read := func(p string) string { return readFixture(t, filepath.Join("..", "..", p)) }
	rows, src := read("tests/envmatrix/rows.tsv"), read("cmd/goarchive/cmd/packet_integration_test.go")
	skips, runner := read("tests/envmatrix/gh-skip.txt"), read("tests/envmatrix/run-matrix.sh")
	if why := packetRowProblem(rows, src, skips, runner); why != "" {
		t.Fatalf("committed files: %s", why)
	}
	swap := func(s, old, new string) string {
		t.Helper()
		if !strings.Contains(s, old) {
			t.Fatalf("fixture lacks %q", old)
		}
		return strings.Replace(s, old, new, 1)
	}
	for _, tc := range []struct {
		name, rows, src, skips, runner, says string
	}{
		{"a renamed row", swap(rows, "max_allowed_packet_dst_4m\tdynamic", "max_allowed_packet_dst_4mb\tdynamic"), src, skips, runner,
			"no row max_allowed_packet_dst_4m in rows.tsv"},
		{"a commented-out row", swap(rows, "max_allowed_packet_dst_4m\tdynamic", "#max_allowed_packet_dst_4m\tdynamic"), src, skips, runner,
			"no row max_allowed_packet_dst_4m in rows.tsv"},
		{"a mistyped prefix", swap(rows, "\tdst: SET GLOBAL", "\tdest: SET GLOBAL"), src, skips, runner,
			"row max_allowed_packet_dst_4m is not a dynamic dst: row"},
		{"a renamed const", rows, swap(src, `packetDstRow = "max_allowed_packet_dst_4m"`, `packetDstRow = "max_allowed_packet_dst_8m"`), skips, runner,
			"no row max_allowed_packet_dst_8m in rows.tsv"},
		{"a renamed test", rows, swap(src, "func "+packetTest+"(", "func TestMySQLArchiveValueOverThePacket("), skips, runner,
			"no " + packetTest},
		{"a test that ignores the row", rows, swap(src, "if row == packetDstRow {", `if row == "" {`), skips, runner,
			packetTest + " does not use packetDstRow"},
		{"skipped on GitHub", rows, src, skips + packetTest + " # no reason\n", runner,
			"gh-skip.txt skips " + packetTest},
		{"skipped by the runner", rows, src, skips, swap(runner, `SKIP="TestMySQLLiveReplicationGate|`, `SKIP="`+packetTest+`|TestMySQLLiveReplicationGate|`),
			"run-matrix.sh skips " + packetTest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := packetRowProblem(tc.rows, tc.src, tc.skips, tc.runner); got != tc.says {
				t.Errorf("problem = %q, want %q", got, tc.says)
			}
		})
	}
}
