//go:build integration

package cmd

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/engine"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// firstOrder is seed's first order, the smallest root key of the where range.
const firstOrder = "00000000-0000-0000-0000-000000000000"

// newOrder is a root the fixture never seeds, in the where range and in firstOrder's batch.
const newOrder = "00000000-0000-0000-0000-000000000009"

// recurring checks that err is the log collision of firstOrder for typ, and that it exits 1.
func recurring(t *testing.T, err error, typ jobspec.Type) {
	t.Helper()
	var rr *engine.RecurringRootError
	if !errors.As(err, &rr) || rr.Root != firstOrder || rr.Type != typ {
		t.Fatalf("the rerun returned %v; want a RecurringRootError for %s", err, firstOrder)
	}
	if code := exitCode(err); code != exitFailed {
		t.Errorf("exit code %d, want 1", code)
	}
}

// logRows counts job's log rows (the tracking tables live in the destination database).
func logRows(t *testing.T, f fixture, job string) int64 {
	t.Helper()
	id := scalar(t, f.dst, "SELECT id FROM archiver_job WHERE job_name = '"+job+"'")
	return scalar(t, f.dst, fmt.Sprintf("SELECT COUNT(*) FROM archiver_job_log_%d", id))
}

// TestMySQLRecurringRoot (RKP-1, RKP-2): a root back in the where range after its job finished
// it fails its batch on the log, naming the root; nothing of the batch is copied, logged or
// deleted. Each subtest makes the state a contract violation leaves, with SQL on the fixture's
// tables.
func TestMySQLRecurringRoot(t *testing.T) {
	order := fmt.Sprintf("INSERT INTO orders VALUES ('%s', '2019-01-01')", firstOrder)
	lines := fmt.Sprintf("INSERT INTO order_lines VALUES ('%s', 1, 'a'), ('%s', 2, 'b')", firstOrder, firstOrder)
	one := func(table, column string) string {
		return fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = '%s'", table, column, firstOrder)
	}
	t.Run("purge", func(t *testing.T) {
		f := orderFixture(t, "recur_purge")
		if out, err := execute(t, "purge", "-c", f.config, "--job", "purge_orders"); err != nil {
			t.Fatalf("purge: %v\n%s", err, out)
		}
		before := logRows(t, f, "purge_orders")
		// Beside the recurring root, a new one in the same batch (batch_size 2): the log INSERT
		// fails on the first, and the new root is not deleted either.
		mustExec(t, f.src, order, "INSERT INTO orders VALUES ('"+newOrder+"', '2019-04-01')")
		_, err := execute(t, "purge", "-c", f.config, "--job", "purge_orders")
		recurring(t, err, jobspec.Purge)
		if got := scalar(t, f.src, one("orders", "id")); got != 1 {
			t.Errorf("source holds %d of the root, want it kept", got)
		}
		if got := scalar(t, f.src, "SELECT COUNT(*) FROM orders WHERE id = '"+newOrder+"'"); got != 1 {
			t.Errorf("source holds %d of the new root, want it kept", got)
		}
		if got := logRows(t, f, "purge_orders"); got != before {
			t.Errorf("log rows %d, want %d unchanged", got, before)
		}
	})
	t.Run("archive", func(t *testing.T) {
		f := orderFixture(t, "recur_archive")
		if out, err := execute(t, "archive", "-c", f.config, "--job", "archive_orders"); err != nil {
			t.Fatalf("archive: %v\n%s", err, out)
		}
		before := logRows(t, f, "archive_orders")
		// The destination's copy pruned (a retention job), then the tree back in the source.
		mustExec(t, f.dst, "DELETE FROM order_lines WHERE order_id = '"+firstOrder+"'", "DELETE FROM orders WHERE id = '"+firstOrder+"'")
		mustExec(t, f.src, order, lines)
		_, err := execute(t, "archive", "-c", f.config, "--job", "archive_orders")
		recurring(t, err, jobspec.Archive)
		if got := scalar(t, f.src, one("orders", "id")) + scalar(t, f.src, one("order_lines", "order_id")); got != 3 {
			t.Errorf("source holds %d rows of the tree, want all 3 kept", got)
		}
		if got := scalar(t, f.dst, one("orders", "id")); got != 0 {
			t.Errorf("destination holds %d of the root, want the copy rolled back", got)
		}
		if got := logRows(t, f, "archive_orders"); got != before {
			t.Errorf("log rows %d, want %d unchanged", got, before)
		}
	})
	t.Run("copy-only", func(t *testing.T) {
		f := orderFixture(t, "recur_copy")
		if out, err := execute(t, "copy-only", "-c", f.config, "--job", "copy_orders", "--verification=count"); err != nil {
			t.Fatalf("copy-only: %v\n%s", err, out)
		}
		before := logRows(t, f, "copy_orders")
		// Every root's destination rows gone, the log kept: a destination row still there would
		// fail the copy first, with DuplicateError (spec rev 2 §2.3).
		mustExec(t, f.dst, "DELETE FROM order_lines", "DELETE FROM orders")
		_, err := execute(t, "copy-only", "-c", f.config, "--job", "copy_orders")
		recurring(t, err, jobspec.CopyOnly)
		if !strings.Contains(err.Error(), "set resume: true on the job") {
			t.Errorf("no resume hint: %v", err)
		}
		if got := scalar(t, f.dst, "SELECT COUNT(*) FROM orders"); got != 0 {
			t.Errorf("destination holds %d orders, want the copy rolled back", got)
		}
		if got := logRows(t, f, "copy_orders"); got != before {
			t.Errorf("log rows %d, want %d unchanged", got, before)
		}
	})
}
