//go:build integration

package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/lock"
)

// The source-table-locks tests (spec .ayder/superpowers_20261012/specs/
// 2026-10-12-source-table-locks-design.md §8).

const cs4 = "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"

// purgeNotesJob purges line_notes alone: its tree overlaps orderJob's on one table.
const purgeNotesJob = `
    root_table: line_notes
    primary_key: note_id
    where: "note_id > 0"
`

// TestMySQLOverlappingTreesRunOneAtATime (T3, G1): an archive whose tree ends in line_notes
// runs; a purge of line_notes, another root, is refused naming the table and deletes
// nothing.
func TestMySQLOverlappingTreesRunOneAtATime(t *testing.T) {
	e := newEnv(t, "overlap")
	e.both(t, orderTables(cs4)...)
	seedOrders(t, e, 2)
	notes := count(t, e.src, "SELECT COUNT(*) FROM line_notes")
	a := e.config(t, job(t, jobspec.Archive, orderJob))
	b := e.config(t, job(t, jobspec.Purge, purgeNotesJob))
	b.Name = "notes"
	var errB error
	ran := false
	a.at = func(p point) {
		if p != atRunning || ran {
			return
		}
		ran = true
		_, errB = e.run(t, b)
		// dry-run takes no lock (Review Focus 4): it is not refused while a job runs.
		if _, err := Explain(context.Background(), e.config(t, job(t, jobspec.Archive, orderJob))); err != nil {
			t.Errorf("Explain while the archive runs = %v", err)
		}
		if got := count(t, e.src, "SELECT COUNT(*) FROM line_notes"); got != notes {
			t.Errorf("line_notes = %d after the purge's attempt, want %d", got, notes)
		}
	}
	e.mustRun(t, a)
	locked := wantErr[*TableLockedError](t, errB)
	if locked.Database != e.srcSchema || locked.Table != "line_notes" || locked.HolderConnectionID == 0 {
		t.Errorf("refusal = %+v, want %s.line_notes and its holder", locked, e.srcSchema)
	}
}

// TestMySQLOneSourceTreeTwoDestinations (T4, G2): the same job pointed at a second
// destination job_schema is refused while the first runs, and copies nothing. One
// destination server with two job_schemas is the whole case: the refusal comes from the
// source table locks, and the destination server plays no part in it (plan P12).
func TestMySQLOneSourceTreeTwoDestinations(t *testing.T) {
	e := newEnv(t, "twodest")
	e.both(t, orderTables(cs4)...)
	seedOrders(t, e, 2)
	otherSchema, other := scratch(t, "TEST_DEST", "twodest_b")
	execAll(t, other, orderTables(cs4)...)
	a := e.config(t, job(t, jobspec.Archive, orderJob))
	b := e.config(t, job(t, jobspec.Archive, orderJob))
	b.Destination, b.JobSchema = other, otherSchema
	var errB error
	ran := false
	a.at = func(p point) {
		if p != atRunning || ran {
			return
		}
		ran = true
		_, errB = e.run(t, b)
	}
	e.mustRun(t, a)
	locked := wantErr[*TableLockedError](t, errB)
	if locked.Database != e.srcSchema {
		t.Errorf("refusal = %+v, want a table of %s", locked, e.srcSchema)
	}
	if got := count(t, other, "SELECT COUNT(*) FROM orders"); got != 0 {
		t.Errorf("the second destination holds %d orders, want 0", got)
	}
}

// TestMySQLLongNamesRun (T2e, §1.1): a 64-character root table and a 255-character job name
// archive to completion. Both were refused at the 2.x-shaped lock names (error 4163).
func TestMySQLLongNamesRun(t *testing.T) {
	e := newEnv(t, "longnames")
	table := "r" + strings.Repeat("x", 63)
	e.both(t, "CREATE TABLE `"+table+"` (k INT NOT NULL, eligible TINYINT NOT NULL, PRIMARY KEY (k)) ENGINE=InnoDB")
	execAll(t, e.src, "INSERT INTO `"+table+"` VALUES (1, 1), (2, 0)")
	cfg := e.config(t, job(t, jobspec.Archive, "\n    root_table: "+table+"\n    primary_key: k\n    where: \"eligible = 1\"\n"))
	cfg.Name = strings.Repeat("j", 255)
	e.mustRun(t, cfg)
	if src, dst := count(t, e.src, "SELECT COUNT(*) FROM `"+table+"`"), count(t, e.dst, "SELECT COUNT(*) FROM `"+table+"`"); src != 1 || dst != 1 {
		t.Errorf("source %d rows, destination %d; want 1 and 1", src, dst)
	}
}

// TestMySQLPoolMinimums (T9, spec §5.5): a one-connection source pool is refused by name and
// two connections run; the destination keeps its minimum of three (two refused, three run).
// The bounded context turns a hang into a failure.
func TestMySQLPoolMinimums(t *testing.T) {
	e := newEnv(t, "srcpool")
	e.both(t, intTables)
	execAll(t, e.src, "INSERT INTO r VALUES (1, 1), (2, 1)")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	e.src.SetMaxOpenConns(1)
	if _, err := e.runContext(t, ctx, e.config(t, job(t, jobspec.Archive, decimalJob))); err == nil || !strings.Contains(err.Error(), "source.max_connections") {
		t.Fatalf("Run with a one-connection source pool = %v, want the source.max_connections refusal", err)
	}
	e.src.SetMaxOpenConns(2)
	e.dst.SetMaxOpenConns(2)
	if _, err := e.runContext(t, ctx, e.config(t, job(t, jobspec.Archive, decimalJob))); err == nil || !strings.Contains(err.Error(), "destination.max_connections") {
		t.Fatalf("Run with a two-connection destination pool = %v, want the destination.max_connections refusal", err)
	}
	e.dst.SetMaxOpenConns(3)
	if _, err := e.runContext(t, ctx, e.config(t, job(t, jobspec.Archive, decimalJob))); err != nil {
		t.Fatalf("Run with two source and three destination connections = %v", err)
	}
}

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
