//go:build integration

package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
)

// The plan's Review Focus: conditions the spec implies that the other command tests do not
// reach.

// withDestination adds a key to the destination block of f's config and returns the new path.
func (f fixture) withDestination(t *testing.T, line string) string {
	t.Helper()
	body := mustRead(t, f.config)
	at := strings.Index(body, "\ndestination:\n")
	marker := "  tls: disable\n"
	end := strings.Index(body[at:], marker)
	if at < 0 || end < 0 {
		t.Fatal("no destination block")
	}
	end += at + len(marker)
	return writeConfig(t, body[:end]+line+"\n"+body[end:])
}

// TestMySQLJobSchemaCommand (Review Focus 1, spec §6.1): with destination.job_schema set, the
// tracking tables go there and nowhere else, and the rows land in the destination database.
func TestMySQLJobSchemaCommand(t *testing.T) {
	f := newFixture(t, "jobschema", orderJobs)
	mustExec(t, f.src, orderDDL...)
	mustExec(t, f.dst, orderDDL...)
	seed(t, f)
	ds := serverFor(t, "TEST_DEST")
	admin := ds.open(t, "")
	js := fmt.Sprintf("ga3m6_js_%d", os.Getpid())
	mustExec(t, admin, "DROP DATABASE IF EXISTS `"+js+"`", "CREATE DATABASE `"+js+"`")
	t.Cleanup(func() { _, _ = admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS `"+js+"`") })

	cfg := f.withDestination(t, "  job_schema: "+js)
	if out, err := execute(t, "archive", "-c", cfg, "--job", "archive_orders"); err != nil {
		t.Fatalf("archive: %v\n%s", err, out)
	}
	if got := scalar(t, admin, "SELECT COUNT(*) FROM `"+js+"`.archiver_job WHERE job_name = 'archive_orders'"); got != 1 {
		t.Errorf("registry rows in job_schema = %d, want 1", got)
	}
	var name string
	if err := f.dst.QueryRowContext(context.Background(), "SHOW TABLES LIKE 'archiver_job'").Scan(&name); err != sql.ErrNoRows {
		t.Errorf("the destination database holds tracking tables too (%q, %v)", name, err)
	}
	if got := scalar(t, f.dst, "SELECT COUNT(*) FROM order_lines"); got != 6 {
		t.Errorf("destination lines = %d, want 6", got)
	}
}

// TestMySQLDryRunPurgeWithoutDestinationTables (Review Focus 2, spec §8.1, R5): dry-run uses
// the job's own profile, so a purge job is never asked for destination tables.
func TestMySQLDryRunPurgeWithoutDestinationTables(t *testing.T) {
	f := newFixture(t, "dryrunpurge", orderJobs)
	mustExec(t, f.src, orderDDL...)
	seed(t, f)
	if out, err := execute(t, "dry-run", "-c", f.config, "--job", "purge_orders"); err != nil {
		t.Errorf("dry-run of a purge job without destination tables: %v\n%s", err, out)
	}
	if got := scalar(t, f.src, "SELECT COUNT(*) FROM orders"); got != 4 {
		t.Errorf("dry-run changed the source: %d orders", got)
	}
}

// TestMySQLSmallPoolCommand (Review Focus 3, M5-F6): destination.max_connections below three is
// refused at once, naming the setting, before any lock or registry write.
func TestMySQLSmallPoolCommand(t *testing.T) {
	f := newFixture(t, "smallpool", orderJobs)
	mustExec(t, f.src, orderDDL...)
	mustExec(t, f.dst, orderDDL...)
	seed(t, f)
	cfg := f.withDestination(t, "  max_connections: 2")
	_, err := execute(t, "archive", "-c", cfg, "--job", "archive_orders")
	if err == nil || !strings.Contains(err.Error(), "destination.max_connections") {
		t.Fatalf("archive with max_connections: 2 = %v", err)
	}
	var name string
	if err := f.dst.QueryRowContext(context.Background(), "SHOW TABLES LIKE 'archiver_job'").Scan(&name); err != sql.ErrNoRows {
		t.Errorf("the refused run created the tracking tables (%q, %v)", name, err)
	}
}

// TestMySQLValidateAllJobs (Review Focus 5): without --job, validate checks every job under its
// own profile, reports each one, and fails naming exactly the jobs that failed.
func TestMySQLValidateAllJobs(t *testing.T) {
	f := newFixture(t, "validateall", orderJobs)
	mustExec(t, f.src, orderDDL...)
	out, err := execute(t, "validate", "-c", f.config)
	if err == nil || !strings.Contains(err.Error(), "validation failed for 2 of 3 job(s): [archive_orders copy_orders]") {
		t.Fatalf("validate = %v\n%s", err, out)
	}
	if !strings.Contains(out, "Job: purge_orders (purge)\n   PASSED") {
		t.Errorf("purge_orders not reported as passed:\n%s", out)
	}
}

// TestMySQLPreflightFlags (spec §8.3, M3-F3): the two preflight overrides reach preflight from
// the command line. A DELETE trigger on a source table is refused; --force-triggers accepts it,
// and so does --skip-validate-preflight, which skips the policy checks.
func TestMySQLPreflightFlags(t *testing.T) {
	for _, flag := range []string{"--force-triggers", "--skip-validate-preflight"} {
		t.Run(flag, func(t *testing.T) {
			f := newFixture(t, "flags", orderJobs)
			mustExec(t, f.src, orderDDL...)
			mustExec(t, f.dst, orderDDL...)
			mustExec(t, f.src, "CREATE TRIGGER tr_lines BEFORE DELETE ON order_lines FOR EACH ROW SET @deleted = 1")
			seed(t, f)
			if _, err := execute(t, "archive", "-c", f.config, "--job", "archive_orders"); err == nil || !strings.Contains(strings.ToLower(err.Error()), "trigger") {
				t.Fatalf("archive over a DELETE trigger = %v, want preflight's refusal", err)
			}
			if out, err := execute(t, "archive", "-c", f.config, "--job", "archive_orders", flag); err != nil {
				t.Fatalf("archive %s: %v\n%s", flag, err, out)
			}
			if got := scalar(t, f.src, "SELECT COUNT(*) FROM order_lines"); got != 2 {
				t.Errorf("source lines after archive %s = %d, want 2", flag, got)
			}
		})
	}
}

// TestMySQLValidateForceTriggers (P4, M6-4): validate checks DELETE triggers like the run would,
// and accepts --force-triggers, so an operator who accepts a trigger can still validate.
func TestMySQLValidateForceTriggers(t *testing.T) {
	f := newFixture(t, "validatetrig", orderJobs)
	mustExec(t, f.src, orderDDL...)
	mustExec(t, f.dst, orderDDL...)
	mustExec(t, f.src, "CREATE TRIGGER tr_lines BEFORE DELETE ON order_lines FOR EACH ROW SET @deleted = 1")
	if _, err := execute(t, "validate", "-c", f.config, "--job", "archive_orders"); err == nil {
		t.Fatal("validate passed a DELETE trigger")
	}
	if out, err := execute(t, "validate", "-c", f.config, "--job", "archive_orders", "--force-triggers"); err != nil {
		t.Errorf("validate --force-triggers: %v\n%s", err, out)
	}
}
