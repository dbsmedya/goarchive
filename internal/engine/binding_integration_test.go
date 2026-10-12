//go:build integration

package engine

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
)

// The binding tests of spec §10 (§6.2, §6.4, §6.6) and the review counterexamples: while a
// job has pending work, a run whose binding differs in a compared part is refused before
// anything is replayed, copied, skipped or deleted. A clean job rebinds freely.

// interrupted runs cfg and cancels it when the run reaches p, leaving what was logged by then.
func (e env) interrupted(t *testing.T, cfg Config, p point) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg.at = func(q point) {
		if q == p {
			cancel()
		}
	}
	// Only the cancellation at p counts: a run refused or failed before p (a preflight
	// refusal under the environment matrix's partial_revokes_on row, M7c-2) is reported as
	// itself, not taken for the interruption.
	if _, err := e.runContext(t, ctx, cfg); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() = %v, want it interrupted at %s (context.Canceled)", err, p)
	}
}

// refused checks that err is a binding refusal naming every part in parts.
func refused(t *testing.T, err error, parts ...tracking.Part) {
	t.Helper()
	be := wantErr[*tracking.BindingError](t, err)
	for _, p := range parts {
		if !slices.Contains(be.Changed, p) {
			t.Errorf("refusal %v does not name %q", be.Changed, p)
		}
	}
}

// twin is a second source schema on the same server holding the same tables and the same
// keys: a repointed source whose overlapping keys would otherwise be replayed or skipped.
func (e env) twin(t *testing.T, name string, ddl []string, seed func(env)) env {
	t.Helper()
	ss, src := scratch(t, "TEST_SOURCE", name)
	o := e
	o.src, o.srcSchema = src, ss
	execAll(t, src, ddl...)
	seed(o)
	return o
}

func (e env) storedBinding(t *testing.T, jobSchema string) string {
	t.Helper()
	var uuid, sdb, ddb, fp string
	if err := e.dst.QueryRowContext(context.Background(), "SELECT source_server_uuid, source_database, destination_database, config_fingerprint FROM `"+
		jobSchema+"`.archiver_job WHERE job_name = 'j'").Scan(&uuid, &sdb, &ddb, &fp); err != nil {
		t.Fatal(err)
	}
	return strings.Join([]string{uuid, sdb, ddb, fp}, "|")
}

const extraRelation = `
      - table: order_tags
        primary_key: id
        foreign_key: order_id
`

// withTags adds order_tags, tied to orders by order_id without a declared foreign key, so a
// job may leave it out (FK coverage) or add it later as a relation.
func withTags(cs string) []string {
	return append(orderTables(cs), "CREATE TABLE order_tags (id INT NOT NULL, order_id CHAR(36) "+cs+" NOT NULL, PRIMARY KEY (id), "+
		"KEY ix_order (order_id)) ENGINE=InnoDB")
}

// orderJobWith inserts extra relations under the root.
func orderJobWith(where, extra string) string {
	body := strings.Replace(orderJob, `where: "placed < '2020-01-01'"`, "where: \""+where+"\"", 1)
	return strings.Replace(body, "    relations:\n", "    relations:"+extra+"\n", 1)
}

func TestMySQLArchiveBinding(t *testing.T) {
	cs := "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
	ddl := withTags(cs)
	setup := func(t *testing.T) env {
		e := newEnv(t, "abind")
		e.both(t, ddl...)
		seedOrders(t, e, 3)
		e.interrupted(t, e.config(t, job(t, jobspec.Archive, orderJob)), atCommitted)
		if got := statuses(t, e.dst, e.dstSchema); got[int(tracking.StatusCopied)] != 2 {
			t.Fatalf("log = %v, want 2 copied", got)
		}
		return e
	}
	srcRows := func(t *testing.T, db *sql.DB) int64 {
		return count(t, db, "SELECT (SELECT COUNT(*) FROM orders) + (SELECT COUNT(*) FROM order_lines) + (SELECT COUNT(*) FROM line_notes)")
	}

	t.Run("a repointed source database is refused before any delete", func(t *testing.T) {
		e := setup(t)
		o := e.twin(t, "abind2", ddl, func(o env) { seedOrders(t, o, 3) })
		before, beforeTwin := srcRows(t, e.src), srcRows(t, o.src)
		_, err := o.run(t, o.config(t, job(t, jobspec.Archive, orderJob)))
		refused(t, err, tracking.PartSource)
		if srcRows(t, e.src) != before || srcRows(t, o.src) != beforeTwin {
			t.Error("a source changed")
		}
		// --skip-validate-preflight skips no bookkeeping check (spec §6.4).
		cfg := o.config(t, job(t, jobspec.Archive, orderJob))
		cfg.SkipPolicy = true
		_, err = o.run(t, cfg)
		refused(t, err, tracking.PartSource)
		if srcRows(t, o.src) != beforeTwin {
			t.Error("the twin changed under SkipPolicy")
		}
	})

	t.Run("another source server is refused; the documented re-bind finishes the work", func(t *testing.T) {
		e := setup(t)
		real := e.storedBinding(t, e.dstSchema)
		execAll(t, e.dst, "UPDATE archiver_job SET source_server_uuid = '00000000-0000-0000-0000-000000000000' WHERE job_name = 'j'")
		_, err := e.run(t, e.config(t, job(t, jobspec.Archive, orderJob)))
		refused(t, err, tracking.PartSource)
		// The failover re-bind of docs/README_JOBS_SCHEMA.md: point the job at the server
		// that now holds the same data.
		execAll(t, e.dst, "UPDATE archiver_job SET source_server_uuid = '"+strings.Split(real, "|")[0]+"' WHERE job_name = 'j'")
		rep := e.mustRun(t, e.config(t, job(t, jobspec.Archive, orderJob)))
		if rep.Recovered != 2 || count(t, e.src, "SELECT COUNT(*) FROM orders") != 1 {
			t.Errorf("after the re-bind: %+v", rep)
		}
	})

	t.Run("a relation added is refused; only where changed finishes", func(t *testing.T) {
		e := setup(t)
		before := srcRows(t, e.src)
		_, err := e.run(t, e.config(t, job(t, jobspec.Archive, orderJobWith("placed < '2020-01-01'", extraRelation))))
		refused(t, err, tracking.PartRelations)
		if srcRows(t, e.src) != before {
			t.Error("the source changed")
		}
		rep := e.mustRun(t, e.config(t, job(t, jobspec.Archive, orderJobWith("placed < '2019-06-01'", ""))))
		if rep.Recovered != 2 {
			t.Errorf("a where-only change: %+v", rep)
		}
	})

	t.Run("a config change between clean runs rebinds", func(t *testing.T) {
		e := setup(t)
		e.mustRun(t, e.config(t, job(t, jobspec.Archive, orderJob)))
		before := e.storedBinding(t, e.dstSchema)
		e.mustRun(t, e.config(t, job(t, jobspec.Archive, orderJobWith("placed < '2021-01-01'", extraRelation))))
		if after := e.storedBinding(t, e.dstSchema); after == before || !strings.Contains(after, "order_tags") {
			t.Errorf("the fingerprint was not rewritten: %s", after)
		}
	})
}

// TestMySQLPurgeBinding: a purge job stopped with roots in deleting, restarted with a
// narrowed where, is refused before any delete.
func TestMySQLPurgeBinding(t *testing.T) {
	e := newEnv(t, "pbind")
	execAll(t, e.src, orderTables("CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")...)
	seedOrders(t, e, 3)
	e.interrupted(t, e.config(t, job(t, jobspec.Purge, orderJob)), atLogged)
	before := count(t, e.src, "SELECT COUNT(*) FROM line_notes")
	_, err := e.run(t, e.config(t, job(t, jobspec.Purge, orderJobWith("placed < '2018-01-01'", ""))))
	refused(t, err, tracking.PartWhere)
	if count(t, e.src, "SELECT COUNT(*) FROM line_notes") != before {
		t.Error("the source changed")
	}
}

// TestMySQLCopyOnlyBinding: copy-only's completed roots pin the binding whatever resume says
// (spec §6.6), so no run, failed or empty, can rebind the log for a later resume: true run.
func TestMySQLCopyOnlyBinding(t *testing.T) {
	cs := "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
	ddl := withTags(cs)
	resume := orderJob + "    resume: true\n"
	setup := func(t *testing.T) env {
		e := newEnv(t, "cbind")
		e.both(t, ddl...)
		seedOrders(t, e, 3)
		e.mustRun(t, e.config(t, job(t, jobspec.CopyOnly, resume)))
		return e
	}

	t.Run("resume: true against a repointed source is refused before any skip", func(t *testing.T) {
		e := setup(t)
		o := e.twin(t, "cbind2", ddl, func(o env) { seedOrders(t, o, 3) })
		_, err := o.run(t, o.config(t, job(t, jobspec.CopyOnly, resume)))
		refused(t, err, tracking.PartSource)
	})

	t.Run("a relation added is refused; only where changed runs", func(t *testing.T) {
		e := setup(t)
		_, err := e.run(t, e.config(t, job(t, jobspec.CopyOnly, orderJobWith("placed < '2020-01-01'", extraRelation)+"    resume: true\n")))
		refused(t, err, tracking.PartRelations)
		rep := e.mustRun(t, e.config(t, job(t, jobspec.CopyOnly, orderJobWith("placed < '2019-06-01'", "")+"    resume: true\n")))
		if rep.Skipped != 3 || rep.Roots != 0 {
			t.Errorf("a where-only change: %+v", rep)
		}
	})

	t.Run("an empty resume: false run against a new source cannot rebind", func(t *testing.T) {
		e := setup(t)
		bound := e.storedBinding(t, e.dstSchema)
		empty := e.twin(t, "cbind3", ddl, func(env) {})
		_, err := empty.run(t, empty.config(t, job(t, jobspec.CopyOnly, orderJob)))
		refused(t, err, tracking.PartSource)
		_, err = empty.run(t, empty.config(t, job(t, jobspec.CopyOnly, resume)))
		refused(t, err, tracking.PartSource)
		_, err = empty.run(t, empty.config(t, job(t, jobspec.CopyOnly, orderJobWith("placed < '2020-01-01'", extraRelation))))
		refused(t, err, tracking.PartSource, tracking.PartRelations)
		if got := e.storedBinding(t, e.dstSchema); got != bound {
			t.Errorf("the binding changed:\n%s\nwas\n%s", got, bound)
		}
	})

	t.Run("a failed resume: false run does not rebind", func(t *testing.T) {
		e := setup(t)
		bound := e.storedBinding(t, e.dstSchema)
		// The same job with resume: false fails on the first duplicate. (A where-only edit
		// would be written even with pending work: where is not a compared part of copy-only.)
		_, err := e.run(t, e.config(t, job(t, jobspec.CopyOnly, orderJob)))
		_ = wantErr[*DuplicateError](t, err)
		if got := e.storedBinding(t, e.dstSchema); got != bound {
			t.Errorf("the failed run rebound the job:\n%s\nwas\n%s", got, bound)
		}
	})
}

// TestMySQLSharedJobSchema: copy-only into archive_old, then destination.database changed to
// an empty archive_new that shares the job_schema, is refused (spec §6.6, review B2).
func TestMySQLSharedJobSchema(t *testing.T) {
	cs := "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
	e := newEnv(t, "shared")
	js, _ := scratch(t, "TEST_DEST", "sharedjs")
	_, newDst := scratch(t, "TEST_DEST", "sharednew")
	e.both(t, orderTables(cs)...)
	execAll(t, newDst, orderTables(cs)...)
	seedOrders(t, e, 2)
	cfg := e.config(t, job(t, jobspec.CopyOnly, orderJob+"    resume: true\n"))
	cfg.JobSchema = js
	e.mustRun(t, cfg)
	cfg.Destination = newDst
	_, err := e.run(t, cfg)
	refused(t, err, tracking.PartDestination)
	if n := count(t, newDst, "SELECT COUNT(*) FROM orders"); n != 0 {
		t.Errorf("archive_new holds %d orders", n)
	}
}

// TestMySQLEmptyRange (master plan Review Focus 1): a where that matches nothing finishes
// with zero batches and no log rows, and leaves the job free to rebind.
func TestMySQLEmptyRange(t *testing.T) {
	cs := "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
		t.Run(string(typ), func(t *testing.T) {
			e := newEnv(t, "empty")
			e.both(t, withTags(cs)...)
			seedOrders(t, e, 2)
			rep := e.mustRun(t, e.config(t, job(t, typ, orderJobWith("placed < '1900-01-01'", ""))))
			if rep.Batches != 0 || rep.Roots != 0 {
				t.Errorf("report = %+v, want nothing done", rep)
			}
			if got := statuses(t, e.dst, e.dstSchema); len(got) != 0 {
				t.Errorf("log = %v, want empty", got)
			}
			e.mustRun(t, e.config(t, job(t, typ, orderJobWith("placed < '1900-01-01'", extraRelation))))
		})
	}
}
