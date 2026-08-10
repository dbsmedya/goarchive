package archiver

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestNoResidualInformationSchemaQueries fails when any non-test Go file in this module
// contains a string literal naming information_schema.
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
func TestNoResidualInformationSchemaQueries(t *testing.T) {
	root := moduleRoot(t)
	fset := token.NewFileSet()

	walkErr := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			// testdata is skipped because Go tooling ignores it and a fixture there may
			// deliberately not parse; .git and vendor hold no first-party source.
			switch entry.Name() {
			case ".git", "vendor", "testdata":
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
			if !strings.Contains(strings.ToLower(value), "information_schema") {
				return true
			}
			t.Errorf("%s:%d: string literal names information_schema. GoArchive 2.0 obtains "+
				"every database fact from dbsgomysql/pkg/validations, which verifies them "+
				"against MySQL 8.0, 8.4 and 9.7. If the library cannot answer this, file an "+
				"upstream issue (spec section 5) — do not query it here",
				rel, fset.Position(lit.Pos()).Line)
			return true
		})

		return nil
	})
	if walkErr != nil {
		t.Fatalf("walk module: %v", walkErr)
	}
}

// sqlmockBudget pins how much sqlmock a converted test file is still allowed to contain.
type sqlmockBudget struct {
	file    string
	mocks   int    // ExpectQuery + ExpectExec calls
	handles int    // sqlmock.New calls
	why     string // what the remaining count consists of
}

// sqlmockBudgets is the ratchet protecting the preflight-test conversion.
//
// These files used to program dbsgomysql's SQL — column names, aliases and row order that
// nothing verified, so a library rename would diverge silently while goarchive stayed green.
// They now receive the library's typed fact preloaded on the run instead. The counts below
// are what legitimately remains, and every entry names why.
//
// TWO AXES, because either alone is insufficient. `mocks` catches a reintroduced query.
// `handles` catches a reintroduced database that is never queried — which is not harmless: 40
// tests in preflight_test.go once held a handle they never used, each paired with an
// ExpectationsWereMet() call that returns nil unconditionally when nothing was programmed
// (sqlmock.go:187). That reads as a tripwire and asserts nothing.
//
// EQUALITY, NOT A CEILING. A `<=` bound would silently absorb the day someone deletes one of
// the two RunAllChecks tests, which is a coverage loss this guard is well placed to notice.
// Both directions are a decision worth making deliberately, so both fail here.
var sqlmockBudgets = []sqlmockBudget{
	{
		file: "preflight_test.go", mocks: 4, handles: 10,
		why: "4 mocks in TestRunAllChecks_MissingTables and TestRunAllChecks_NonInnoDBTables: " +
			"RunWithProfile builds its own preflightRun and preflightRun deliberately exposes no " +
			"inspector accessor, so there is no seam to inject a preloaded fact through. " +
			"10 handles: 1 shared deny-all stub in newDestChecker (serving all 12 destination " +
			"tests), 4 in TestNewPreflightChecker_*, 2 in TestConfigureDestination_Success, " +
			"2 in the RunAllChecks pair, and 1 in TestValidateTablesExist_ContextCancellation " +
			"— where Inspector.Tables validates the Querier before consulting ctx, so a nil " +
			"handle would mask context.Canceled and the handle is the mechanism under test",
	},
	{
		file: "composite_pk_test.go", mocks: 0, handles: 0,
		why: "converted to typed validations.PKInfo facts; this file must remain independent " +
			"of dbsgomysql query text and sqlmock database handles",
	},
}

// TestSqlmockBudgetsHold fails when a budgeted file's sqlmock usage moves in either
// direction.
//
// It counts AST call expressions, not text, for the same reason the guard above inspects
// string literals: a comment explaining why a call was removed matches any text pattern that
// would match the call itself. Six such comments already exist in preflight_test.go, so a
// textual count could only be made green by deleting the explanations — trading real
// documentation for no additional safety.
func TestSqlmockBudgetsHold(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}

	for _, budget := range sqlmockBudgets {
		t.Run(budget.file, func(t *testing.T) {
			fset := token.NewFileSet()
			file, parseErr := parser.ParseFile(fset, filepath.Join(dir, budget.file), nil, 0)
			if parseErr != nil {
				t.Fatalf("parse %s: %v", budget.file, parseErr)
			}

			var mocks, handles int
			ast.Inspect(file, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch sel.Sel.Name {
				case "ExpectQuery", "ExpectExec":
					mocks++
				case "New":
					// sqlmock.New only; any other New( is unrelated.
					if pkg, isIdent := sel.X.(*ast.Ident); isIdent && pkg.Name == "sqlmock" {
						handles++
					}
				}
				return true
			})

			if mocks != budget.mocks {
				t.Errorf("%s programs %d sqlmock queries, budget is %d.\n"+
					"  If you ADDED one: preload the library fact on the run instead — see the "+
					"tests marked REFERENCE SHAPE in preflight_test.go.\n"+
					"  If you REMOVED one: confirm you did not delete coverage, then lower the "+
					"budget in sqlmockBudgets.\n  Budget rationale: %s",
					budget.file, mocks, budget.mocks, budget.why)
			}
			if handles != budget.handles {
				t.Errorf("%s opens %d sqlmock database handles, budget is %d.\n"+
					"  A test whose facts are preloaded needs no handle at all: source stages read "+
					"the handle only to build an Inspector, and the library answers a nil Querier "+
					"with a named error, so an accidental query fails as an assertion rather than "+
					"being answered by a mock. Use newSourceOnlyChecker, or newDestChecker for the "+
					"five stages that guard on p.destinationDB == nil.\n  Budget rationale: %s",
					budget.file, handles, budget.handles, budget.why)
			}
		})
	}
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
