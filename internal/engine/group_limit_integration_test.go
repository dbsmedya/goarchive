//go:build integration

package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
)

// GRT-1 (spec §4.4, §8, GR C3): a commit that Group Replication refuses over its transaction size limit
// ends the run with a GroupCommitError that names the limit and the setting, and the rerun with a lower
// setting finishes the work. The members' limit is lowered to 1 MiB, so a batch of eight 200 KB rows is
// over it and a chunk of two is not; nothing builds a large transaction (a 100 MB one killed a member in
// C1). The tests need the group estate's members and skip on the plain estate (requireGroupMemberSource).

const (
	limitBytes = 1 << 20 // the members' limit during a test
	rowBytes   = 200000  // the payload of one row
	rowCount   = 8       // 1.6 MB, over limitBytes; two rows are 400 KB, under it
)

const bigRowsDDL = "CREATE TABLE big_rows (id INT NOT NULL PRIMARY KEY, payload MEDIUMTEXT NOT NULL) ENGINE=InnoDB"

const bigRowsJob = "\n    root_table: big_rows\n    primary_key: id\n    where: '1=1'\n"

// seedBigRows writes rowCount rows of rowBytes each, one transaction each, before the limit is lowered.
func seedBigRows(t *testing.T, db *sql.DB) {
	t.Helper()
	for i := 1; i <= rowCount; i++ {
		execAll(t, db, fmt.Sprintf("INSERT INTO big_rows VALUES (%d, REPEAT('x', %d))", i, rowBytes))
	}
}

// groupMemberPrefixes name the three servers of the group estate (tests/.env.group), as root.
var groupMemberPrefixes = []string{"TEST_SOURCE", "TEST_GROUP_MEMBER2", "TEST_GROUP_MEMBER3"}

// lowerGroupLimit sets group_replication_transaction_size_limit to bytes on every member, and restores
// each member's own value when the test ends, reading it back: a member left at 1 MiB would break every
// later test, so a restore that did not take fails this one.
func lowerGroupLimit(t *testing.T, bytes int) {
	t.Helper()
	for _, prefix := range groupMemberPrefixes {
		db := openServer(t, prefix, "", nil)
		const variable = "group_replication_transaction_size_limit"
		var original int
		if err := db.QueryRowContext(context.Background(), "SELECT @@GLOBAL."+variable).Scan(&original); err != nil {
			t.Fatalf("%s: read %s: %v (is the group estate's member configured in tests/.env.group?)", prefix, variable, err)
		}
		execAll(t, db, fmt.Sprintf("SET GLOBAL %s = %d", variable, bytes))
		prefix := prefix
		t.Cleanup(func() {
			if _, err := db.ExecContext(context.Background(), fmt.Sprintf("SET GLOBAL %s = %d", variable, original)); err != nil {
				t.Errorf("%s: restore %s to %d: %v", prefix, variable, original, err)
				return
			}
			var now int
			if err := db.QueryRowContext(context.Background(), "SELECT @@GLOBAL."+variable).Scan(&now); err != nil || now != original {
				t.Errorf("%s: %s is %d (%v) after the restore, want %d", prefix, variable, now, err, original)
			}
		})
	}
}

// groupCommitFailure asserts err is a GroupCommitError of the given side and commit and known limit, over MySQL error 3100
// (asserted by its number and by the wrap, never by the server's text), and returns it.
func groupCommitFailure(t *testing.T, err error, side string, commit Commit, limit uint64) *GroupCommitError {
	t.Helper()
	var gce *GroupCommitError
	if !errors.As(err, &gce) {
		t.Fatalf("Run() = %v (%T), want a GroupCommitError", err, err)
	}
	var me *mysql.MySQLError
	if !errors.As(err, &me) || me.Number != 3100 {
		t.Errorf("the chain holds %v, want MySQL error 3100", me)
	}
	if gce.Side != side || gce.Commit != commit || !gce.Limit.Known || gce.Limit.Bytes != limit {
		t.Errorf("GroupCommitError = side %q commit %d limit %+v, want %s, commit %d, a known limit of %d", gce.Side, gce.Commit, gce.Limit, side, commit, limit)
	}
	return gce
}

// TestMySQLSourceLimitFailsTheDeleteChunk is spec §8's *source limit*: an archive's delete chunk over the
// source group's limit ends the run with the message of §4.4; the source rows are all present and the
// roots are copied; the rerun with a lower batch_delete_size finishes them.
func TestMySQLSourceLimitFailsTheDeleteChunk(t *testing.T) {
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge} {
		t.Run(string(typ), func(t *testing.T) {
			requireGroupMemberSource(t)
			e := newEnv(t, "grtsrc"+string(typ))
			stuck := tracking.StatusCopied
			if typ == jobspec.Purge {
				execAll(t, e.src, bigRowsDDL)
				stuck = tracking.StatusDeleting
			} else {
				e.both(t, bigRowsDDL)
			}
			seedBigRows(t, e.src)
			lowerGroupLimit(t, limitBytes)

			cfg := e.config(t, job(t, typ, bigRowsJob))
			cfg.Processing = config.ProcessingConfig{BatchSize: 20, BatchDeleteSize: 100}
			_, err := e.run(t, cfg)
			t.Logf("the error: %v", err)
			gce := groupCommitFailure(t, err, "source", CommitDelete, limitBytes)
			if gce.Setting != "batch_delete_size" || gce.Value != 100 || gce.Limit.Member != preflight.MemberOnline {
				t.Errorf("GroupCommitError = %s=%d, member %d; want batch_delete_size=100 on an ONLINE member", gce.Setting, gce.Value, gce.Limit.Member)
			}
			for _, want := range []string{"on the source, which was an ONLINE member of Group Replication group ", "1048576 bytes",
				"lower batch_delete_size (now 100) and rerun", "the roots stay logged as " + stuck.String()} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the message lacks %q:\n%s", want, err)
				}
			}
			if n := count(t, e.src, "SELECT COUNT(*) FROM big_rows"); n != rowCount {
				t.Errorf("source rows = %d, want all %d: the failed chunk deleted nothing", n, rowCount)
			}
			if got := statuses(t, e.dst, e.dstSchema); got[int(stuck)] != rowCount || len(got) != 1 {
				t.Errorf("the log = %v, want %d roots as %s", got, rowCount, stuck)
			}
			if typ == jobspec.Archive {
				if n := count(t, e.dst, "SELECT COUNT(*) FROM big_rows"); n != rowCount {
					t.Errorf("destination rows = %d, want the copy that committed: %d", n, rowCount)
				}
			}

			cfg.Processing.BatchDeleteSize = 2
			rep := e.mustRun(t, cfg)
			if rep.Recovered != rowCount {
				t.Errorf("the rerun recovered %d roots, want %d", rep.Recovered, rowCount)
			}
			if n := count(t, e.src, "SELECT COUNT(*) FROM big_rows"); n != 0 {
				t.Errorf("source rows after the rerun = %d, want 0", n)
			}
			if got := statuses(t, e.dst, e.dstSchema); got[int(tracking.StatusCompleted)] != rowCount || len(got) != 1 {
				t.Errorf("the log after the rerun = %v, want %d completed", got, rowCount)
			}
		})
	}
}

// TestMySQLDestinationLimitFailsTheCopyTransaction is spec §8's *destination limit*: the roles are swapped,
// so the destination is the group's primary under another database name (SRC_DEST_IDENTITY_CHECK compares
// servers, and the source is the estate's plain destination server). A copy transaction over the
// destination's limit rolls back: nothing is logged or deleted, and the message names batch_size. The
// destination's view is not read, so the message does not say it was a running member.
func TestMySQLDestinationLimitFailsTheCopyTransaction(t *testing.T) {
	requireGroupMemberSource(t)
	e := newEnvOn(t, "grtdst", "TEST_DEST", "TEST_SOURCE")
	e.both(t, bigRowsDDL)
	seedBigRows(t, e.src)
	lowerGroupLimit(t, limitBytes)

	cfg := e.config(t, job(t, jobspec.Archive, bigRowsJob))
	cfg.Processing = config.ProcessingConfig{BatchSize: 20, BatchDeleteSize: 100}
	_, err := e.run(t, cfg)
	t.Logf("the error: %v", err)
	gce := groupCommitFailure(t, err, "destination", CommitCopy, limitBytes)
	if gce.Setting != "batch_size" || gce.Value != 20 || gce.Limit.Member != preflight.MemberUnread {
		t.Errorf("GroupCommitError = %s=%d, member %d; want batch_size=20 on a destination whose view is not read", gce.Setting, gce.Value, gce.Limit.Member)
	}
	for _, want := range []string{"on the destination, where the Group Replication plugin is active (group ", "1048576 bytes",
		"lower batch_size (now 20) and rerun", "the batch was rolled back, so nothing of it was copied, logged or deleted"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the message lacks %q:\n%s", want, err)
		}
	}
	if strings.Contains(err.Error(), "ONLINE member") {
		t.Errorf("the message says the destination was an ONLINE member, which it did not observe:\n%s", err)
	}
	if n := count(t, e.dst, "SELECT COUNT(*) FROM big_rows"); n != 0 {
		t.Errorf("destination rows = %d, want 0: the transaction rolled back", n)
	}
	if got := statuses(t, e.dst, e.dstSchema); len(got) != 0 {
		t.Errorf("the log = %v, want nothing logged", got)
	}
	if n := count(t, e.src, "SELECT COUNT(*) FROM big_rows"); n != rowCount {
		t.Errorf("source rows = %d, want all %d: nothing was deleted", n, rowCount)
	}

	cfg.Processing.BatchSize = 2
	e.mustRun(t, cfg)
	if n := count(t, e.dst, "SELECT COUNT(*) FROM big_rows"); n != rowCount {
		t.Errorf("destination rows after the rerun = %d, want %d", n, rowCount)
	}
	if n := count(t, e.src, "SELECT COUNT(*) FROM big_rows"); n != 0 {
		t.Errorf("source rows after the rerun = %d, want 0", n)
	}
}
