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
