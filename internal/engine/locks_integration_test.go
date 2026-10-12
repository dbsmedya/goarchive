//go:build integration

package engine

import (
	"context"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/lock"
)

// The source-table-locks tests (spec .ayder/superpowers_20261012/specs/
// 2026-10-12-source-table-locks-design.md §8).

// TestMySQLLeakGuard (T10): the env's cleanup guard reports a recorded lock some session
// still holds, and nothing once it is released.
func TestMySQLLeakGuard(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, "leak")
	e.both(t, intTables)
	e.record(e.config(t, job(t, jobspec.Archive, decimalJob)))
	name := lock.TableLockName(e.srcSchema, "r")
	held := lock.NewSet(e.src, name)
	if err := held.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	if got := e.leaked(t); len(got) != 1 || got[0] != name {
		t.Errorf("leaked = %v, want [%s]", got, name)
	}
	if err := held.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if got := e.leaked(t); len(got) != 0 {
		t.Errorf("leaked after the release = %v", got)
	}
}
