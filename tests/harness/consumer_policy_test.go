package harness

import (
	"fmt"
	"go/ast"
	"go/build/constraint"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestConsumerPolicyNoResidualInformationSchemaQueries fails when any non-test Go file in
// this module contains a string literal naming the metadata catalog owned by dbsgomysql.
//
// This is the structural form of the consumer-policy rule (spec section 2). GoArchive
// performs no low-level discovery of databases, schemas, tables, grants, primary keys or
// any other database object: dbsgomysql owns that, and verifies it against MySQL 8.0, 8.4
// and 9.7 on every release. A hand-rolled probe here is an unverifiable claim about MySQL
// that no goarchive test can falsify. Before 2.0 the rule was enforced only by review.
//
// It inspects STRING LITERALS rather than file text, deliberately. A query lives in a
// literal, so that is where a violation can be; comments legitimately name
// information_schema when recording where a library fact comes from or what 1.8 used to
// read. A whole-file text search would force those explanations to be deleted to keep the
// gate green — trading real documentation for no additional safety.
//
// Stated limit: a query assembled from fragments that never spell information_schema in a
// single literal is not detected. The guard makes the rule visible and the obvious
// regression loud. It is not a proof.
func TestConsumerPolicyNoResidualInformationSchemaQueries(t *testing.T) {
	root := moduleRoot(t)
	metadataCatalog := wireRuleByName(t, loadDBSGOMySQLWireRules(t, root), "information-schema").needle
	fset := token.NewFileSet()

	walkErr := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path != root && skipPolicyWalkDir(entry.Name()) {
				return filepath.SkipDir
			}
			return nil
		}

		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}

		// Mode 0: comments are not attached to the AST, so ast.Inspect never sees them.
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return fmt.Errorf("parse %s: %w", path, parseErr)
		}

		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			rel = path
		}

		ast.Inspect(file, func(node ast.Node) bool {
			lit, ok := node.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			value, unquoteErr := strconv.Unquote(lit.Value)
			if unquoteErr != nil {
				value = lit.Value // raw form is still searchable
			}
			if !strings.Contains(strings.ToLower(value), metadataCatalog) {
				return true
			}
			t.Errorf("%s:%d: string literal names %s. GoArchive 2.0 obtains "+
				"every database fact from dbsgomysql/pkg/validations, which verifies them "+
				"against MySQL 8.0, 8.4 and 9.7. If the library cannot answer this, file an "+
				"upstream issue (spec section 5) — do not query it here",
				rel, fset.Position(lit.Pos()).Line, metadataCatalog)
			return true
		})

		return nil
	})
	if walkErr != nil {
		t.Fatalf("walk module: %v", walkErr)
	}
}

// replicationStatusNeedles are the replication-status statements that dbsgomysql owns as of
// 2.1. GoArchive reads replication state exclusively through dbsgomysql/pkg/replication,
// which verifies the field layout of both statements against the supported MySQL versions;
// a statement issued here would be an unverifiable claim about MySQL, and the 1.8-era
// lagmonitor's hand-rolled parsing of these exact fields is what 2.1 removed.
//
// Lowercase, and matched case-insensitively: the ban is on the statement, not on a spelling.
var replicationStatusNeedles = []string{
	"show replica status",
	"show slave status",
}

// replicationViolation is one string literal naming a replication-status statement.
type replicationViolation struct {
	needle string
	line   int
}

// findReplicationStatusViolations reports every string literal under node that names a
// replication-status statement.
//
// Like the information-schema guard above it inspects STRING LITERALS, not file text. A
// statement lives in a literal, so that is where a violation can be; comments legitimately
// name these statements when recording what 1.8 used to read or which library call replaced
// them. Callers therefore parse with Mode 0, which leaves comments off the AST entirely.
func findReplicationStatusViolations(fset *token.FileSet, node ast.Node) []replicationViolation {
	if node == nil {
		return nil
	}
	var violations []replicationViolation
	ast.Inspect(node, func(node ast.Node) bool {
		lit, ok := node.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		value, unquoteErr := strconv.Unquote(lit.Value)
		if unquoteErr != nil {
			value = lit.Value // raw form is still searchable
		}
		lower := strings.ToLower(value)
		for _, needle := range replicationStatusNeedles {
			if strings.Contains(lower, needle) {
				violations = append(violations, replicationViolation{
					needle: needle,
					line:   fset.Position(lit.Pos()).Line,
				})
			}
		}
		return true
	})
	return violations
}

// TestConsumerPolicyNoReplicationStatusSQL fails when any non-test Go file in this module
// contains a string literal naming SHOW REPLICA STATUS or SHOW SLAVE STATUS.
//
// This is the 2.1 extension of the consumer-policy rule to replication state. Until 2.0 the
// statement was carved out as an "application-owned administrative" exception, because
// dbsgomysql had no replication surface and internal/archiver/lagmonitor.go issued and parsed
// it directly. dbsgomysql/pkg/replication closed that gap, the monitor was deleted, and the
// carve-out is retired — so the statements are now banned in production code the same way
// information_schema is.
//
// The walk covers internal/ and cmd/, which includes the new internal/replication package.
// Stated limit is the same as the information-schema guard: a statement assembled from
// fragments that never spell it in a single literal is not detected. The guard makes the rule
// visible and the obvious regression loud. It is not a proof.
func TestConsumerPolicyNoReplicationStatusSQL(t *testing.T) {
	root := moduleRoot(t)
	fset := token.NewFileSet()

	walkErr := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path != root && skipPolicyWalkDir(entry.Name()) {
				return filepath.SkipDir
			}
			return nil
		}

		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}

		// Mode 0: comments are not attached to the AST, so ast.Inspect never sees them.
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return fmt.Errorf("parse %s: %w", path, parseErr)
		}

		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			rel = path
		}
		rel = filepath.ToSlash(rel)

		for _, violation := range findReplicationStatusViolations(fset, file) {
			t.Errorf("%s:%d: string literal names %q. GoArchive 2.1 reads replication "+
				"state through dbsgomysql/pkg/replication, which verifies the field layout "+
				"of this statement against the supported MySQL versions. The 2.0 "+
				"application-owned carve-out for it is retired. If the library cannot answer "+
				"this, file an upstream issue — do not query it here",
				rel, violation.line, violation.needle)
		}

		return nil
	})
	if walkErr != nil {
		t.Fatalf("walk module: %v", walkErr)
	}
}

// TestConsumerPolicyReplicationDetectorPatterns proves the replication needle scanner can
// actually fail, and pins every independently meaningful branch of its matcher.
//
// Without this, TestConsumerPolicyNoReplicationStatusSQL is green on a clean tree whether the
// scanner works or matches nothing at all. Each fixture line closes a specific hole:
//
//   - line 5 spells the replica statement in MIXED case, so dropping the case-folding in
//     findReplicationStatusViolations fails here. An all-uppercase fixture would not notice.
//   - line 6 is a real lowercase SHOW SLAVE STATUS *literal*, so deleting that needle from
//     replicationStatusNeedles fails here. Spelling the statement only in the comment would
//     leave the second needle permanently unexercised.
//   - line 3 names SHOW SLAVE STATUS in a COMMENT and must never be flagged. The Mode-0
//     parse keeps comments off the AST; a future rewrite of the scanner into a whole-file
//     text search fails here rather than silently forcing every explanatory comment out of
//     the tree.
//
// The assertion is exact — needle AND line for both violations, and no third — because
// "some violations were found" would survive most of those regressions.
func TestConsumerPolicyReplicationDetectorPatterns(t *testing.T) {
	const (
		commentLine          = 3 // // SHOW SLAVE STATUS is legacy — comment-invisibility control
		mixedCaseReplicaLine = 5 // mixedCaseReplicaQuery = "Show Replica Status"
		lowercaseSlaveLine   = 6 // lowercaseSlaveQuery   = "show slave status for channel 'billing'"
	)

	root := moduleRoot(t)
	fset := token.NewFileSet()
	path := filepath.Join(root, "tests", "harness", "testdata", "consumer_policy", "replication_forbidden.go.txt")
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse replication detector fixture: %v", err)
	}

	want := map[replicationViolation]int{
		{needle: "show replica status", line: mixedCaseReplicaLine}: 1,
		{needle: "show slave status", line: lowercaseSlaveLine}:     1,
	}

	got := findReplicationStatusViolations(fset, file)
	gotCounts := make(map[replicationViolation]int, len(got))
	for _, violation := range got {
		gotCounts[violation]++
	}

	if len(got) != len(want) {
		t.Fatalf("replication detector found %d violations (%+v), want exactly %d: the "+
			"mixed-case replica literal on line %d and the lowercase slave literal on line %d. "+
			"Fewer means a needle or the case-folding was lost and the production guard is "+
			"partly vacuous; more means the comment on line %d was scanned too",
			len(got), got, len(want), mixedCaseReplicaLine, lowercaseSlaveLine, commentLine)
	}

	for violation, count := range want {
		if gotCounts[violation] != count {
			t.Errorf("violation %+v found %d times, want %d", violation, gotCounts[violation], count)
		}
	}

	for violation := range gotCounts {
		if _, expected := want[violation]; !expected {
			t.Errorf("unexpected violation %+v; want only %+v", violation, want)
		}
		if violation.line == commentLine {
			t.Errorf("comment on line %d was flagged as %q; comments are documentation and "+
				"must stay invisible to the scanner (parse Mode 0)", commentLine, violation.needle)
		}
	}
}

type wireRule struct {
	kind    string
	name    string
	needle  string
	columns []string
}

type wireViolation struct {
	fingerprint string
	line        int
}

// TestConsumerPolicyNoDBSGOMySQLWireFormatInUnitTests enforces the unit-test side of the
// consumer boundary. Integration tests may inspect a real estate; unit tests must inject
// dbsgomysql's exported typed facts instead of teaching sqlmock the library's private SQL
// or result-column layout.
func TestConsumerPolicyNoDBSGOMySQLWireFormatInUnitTests(t *testing.T) {
	root := moduleRoot(t)
	rules := loadDBSGOMySQLWireRules(t, root)
	fset := token.NewFileSet()

	walkErr := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path != root && skipPolicyWalkDir(entry.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}

		file, parseErr := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if parseErr != nil {
			return fmt.Errorf("parse %s: %w", path, parseErr)
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return fmt.Errorf("relativize %s: %w", path, relErr)
		}
		rel = filepath.ToSlash(rel)
		if isIntegrationTestFile(rel, file) {
			return nil
		}

		for _, decl := range file.Decls {
			function := "<package>"
			node := ast.Node(decl)
			if fn, ok := decl.(*ast.FuncDecl); ok {
				function = fn.Name.Name
				node = fn.Body
			}
			for _, violation := range findWireViolations(fset, node, rules) {
				t.Errorf("%s:%d (%s): %s encodes dbsgomysql private metadata wire format; inject exported typed facts instead",
					rel, violation.line, function, violation.fingerprint)
			}
		}
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walk unit tests: %v", walkErr)
	}
}

func loadDBSGOMySQLWireRules(t *testing.T, root string) []wireRule {
	t.Helper()
	path := filepath.Join(root, "tests", "harness", "testdata", "consumer_policy", "dbsgomysql_wire_rules.tsv")
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read dbsgomysql wire rules: %v", err)
	}

	var rules []wireRule
	for lineNumber, raw := range strings.Split(string(content), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) != 3 {
			t.Fatalf("%s:%d: rule must have kind, name, and value", path, lineNumber+1)
		}
		rule := wireRule{kind: parts[0], name: parts[1]}
		switch rule.kind {
		case "literal":
			rule.needle = strings.ToLower(parts[2])
		case "columns":
			for _, column := range strings.Split(parts[2], ",") {
				rule.columns = append(rule.columns, strings.ToLower(strings.TrimSpace(column)))
			}
		default:
			t.Fatalf("%s:%d: unknown rule kind %q", path, lineNumber+1, rule.kind)
		}
		rules = append(rules, rule)
	}
	if len(rules) == 0 {
		t.Fatalf("%s: no dbsgomysql wire rules loaded", path)
	}
	return rules
}

func wireRuleByName(t *testing.T, rules []wireRule, name string) wireRule {
	t.Helper()
	for _, rule := range rules {
		if rule.name == name {
			return rule
		}
	}
	t.Fatalf("dbsgomysql wire rule %q not found", name)
	return wireRule{}
}

func findWireViolations(fset *token.FileSet, node ast.Node, rules []wireRule) []wireViolation {
	if node == nil {
		return nil
	}
	var violations []wireViolation
	ast.Inspect(node, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.BasicLit:
			if typed.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(typed.Value)
			if err != nil {
				value = typed.Value
			}
			lower := strings.ToLower(value)
			for _, rule := range rules {
				if rule.kind == "literal" && strings.Contains(lower, rule.needle) {
					violations = append(violations, wireViolation{
						fingerprint: "literal:" + rule.name,
						line:        fset.Position(typed.Pos()).Line,
					})
				}
			}
		case *ast.CallExpr:
			columns, ok := sqlmockNewRowsColumns(typed)
			if !ok {
				return true
			}
			for _, rule := range rules {
				if rule.kind == "columns" && equalStrings(columns, rule.columns) {
					violations = append(violations, wireViolation{
						fingerprint: "columns:" + rule.name,
						line:        fset.Position(typed.Pos()).Line,
					})
				}
			}
		}
		return true
	})
	return violations
}

func sqlmockNewRowsColumns(call *ast.CallExpr) ([]string, bool) {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "NewRows" || len(call.Args) != 1 {
		return nil, false
	}
	pkg, ok := selector.X.(*ast.Ident)
	if !ok || pkg.Name != "sqlmock" {
		return nil, false
	}
	list, ok := call.Args[0].(*ast.CompositeLit)
	if !ok {
		return nil, false
	}
	columns := make([]string, 0, len(list.Elts))
	for _, element := range list.Elts {
		literal, ok := element.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return nil, false
		}
		column, err := strconv.Unquote(literal.Value)
		if err != nil {
			return nil, false
		}
		columns = append(columns, strings.ToLower(column))
	}
	return columns, true
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func isIntegrationTestFile(path string, file *ast.File) bool {
	base := filepath.Base(path)
	if base == "integration_test.go" || strings.HasSuffix(base, "_integration_test.go") {
		return true
	}
	for _, group := range file.Comments {
		if group.Pos() > file.Package {
			break
		}
		for _, comment := range group.List {
			if !strings.HasPrefix(comment.Text, "//go:build") {
				continue
			}
			expr, err := constraint.Parse(comment.Text)
			if err == nil && constraintContainsPositiveTag(expr, "integration", false) {
				return true
			}
		}
	}
	return false
}

func constraintContainsPositiveTag(expr constraint.Expr, tag string, negated bool) bool {
	switch typed := expr.(type) {
	case *constraint.TagExpr:
		return typed.Tag == tag && !negated
	case *constraint.NotExpr:
		return constraintContainsPositiveTag(typed.X, tag, !negated)
	case *constraint.AndExpr:
		return constraintContainsPositiveTag(typed.X, tag, negated) ||
			constraintContainsPositiveTag(typed.Y, tag, negated)
	case *constraint.OrExpr:
		return constraintContainsPositiveTag(typed.X, tag, negated) ||
			constraintContainsPositiveTag(typed.Y, tag, negated)
	default:
		return false
	}
}

func TestConsumerPolicyWireDetectorPatterns(t *testing.T) {
	root := moduleRoot(t)
	rules := loadDBSGOMySQLWireRules(t, root)
	fset := token.NewFileSet()
	path := filepath.Join(root, "tests", "harness", "testdata", "consumer_policy", "forbidden.go.txt")
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse detector fixture: %v", err)
	}

	got := make(map[string]int)
	for _, violation := range findWireViolations(fset, file, rules) {
		got[violation.fingerprint]++
	}
	want := make(map[string]int)
	for _, fingerprint := range []string{
		"literal:information-schema",
		"literal:innodb-foreign",
		"literal:key-column-usage",
		"literal:referential-constraints",
		"literal:enabled-roles",
		"literal:user-privileges",
		"literal:schema-privileges",
		"literal:table-privileges",
		"literal:current-user",
		"literal:tables-select",
		"literal:columns-select",
		"literal:primary-keys-select",
		"literal:indexes-select",
		"literal:triggers-select",
		"literal:schema-grants-select",
		"literal:table-grants-select",
		"columns:tables",
		"columns:columns",
		"columns:primary-keys",
		"columns:indexes",
		"columns:triggers",
		"columns:schema-grants",
		"columns:table-grants",
	} {
		want[fingerprint] = 1
	}
	if len(got) != len(want) {
		t.Fatalf("detector fingerprints = %v, want %v", got, want)
	}
	for fingerprint, count := range want {
		if got[fingerprint] != count {
			t.Errorf("detector fingerprint %s count = %d, want %d", fingerprint, got[fingerprint], count)
		}
	}

	allowedPath := filepath.Join(root, "tests", "harness", "testdata", "consumer_policy", "allowed.go.txt")
	allowedFile, err := parser.ParseFile(fset, allowedPath, nil, 0)
	if err != nil {
		t.Fatalf("parse allowed detector fixture: %v", err)
	}
	if violations := findWireViolations(fset, allowedFile, rules); len(violations) != 0 {
		t.Fatalf("legitimate GoArchive SQL produced wire violations: %v", violations)
	}
}

func TestConsumerPolicyIntegrationBuildConstraint(t *testing.T) {
	root := moduleRoot(t)
	fset := token.NewFileSet()
	path := filepath.Join(root, "tests", "harness", "testdata", "consumer_policy", "integration.go.txt")
	file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse integration fixture: %v", err)
	}
	if !isIntegrationTestFile("internal/engine/fixture_integration_test.go", file) {
		t.Fatal("integration build constraint was not classified")
	}

	nonIntegrationPath := filepath.Join(root, "tests", "harness", "testdata", "consumer_policy", "nonintegration.go.txt")
	nonIntegrationFile, err := parser.ParseFile(fset, nonIntegrationPath, nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse non-integration fixture: %v", err)
	}
	if isIntegrationTestFile("internal/engine/unit_fixture_test.go", nonIntegrationFile) {
		t.Fatal("negated integration build constraint was classified as integration")
	}
}

// skipPolicyWalkDir reports whether a policy walk skips the directory named name. It
// mirrors what `go build ./...` ignores: names beginning with "." or "_" (which covers .git
// and internal directories such as .ayder), testdata, where a fixture may deliberately not
// parse, and vendor, which holds no first-party source.
func skipPolicyWalkDir(name string) bool {
	return strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") ||
		name == "testdata" || name == "vendor"
}

// moduleRoot returns the directory holding go.mod, walking up from the package directory
// that `go test` sets as the working directory.
func moduleRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("go.mod not found at or above %s", dir)
		}
		dir = parent
	}
}

// TestPolicyWalkSkipsWhatGoToolingIgnores pins the directory rule the policy walkers share.
// `go build ./...` ignores directories whose names begin with "." or "_", plus testdata and
// vendor, so none of them holds module source. Before 3.0 the walkers skipped only .git,
// vendor and testdata, and a reference .go file kept under the ignored .ayder/ directory
// failed the information_schema guard although no build ever compiles it.
func TestPolicyWalkSkipsWhatGoToolingIgnores(t *testing.T) {
	for _, name := range []string{".git", ".ayder", ".superpowers", "_scratch", "vendor", "testdata"} {
		if !skipPolicyWalkDir(name) {
			t.Errorf("skipPolicyWalkDir(%q) = false; Go tooling ignores this directory", name)
		}
	}
	for _, name := range []string{"internal", "cmd", "engine", "harness"} {
		if skipPolicyWalkDir(name) {
			t.Errorf("skipPolicyWalkDir(%q) = true; this directory holds module source", name)
		}
	}
}
