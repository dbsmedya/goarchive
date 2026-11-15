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

// The write-set error, 3231 (spec §4.4 rev 5.2, fact 25): many SMALL rows, and a limit lowered so that a
// transaction's write set is over it (8 bytes a row key: 512 rows at 4096) while its size is not. A chunk of a
// hundred rows is under it.

const (
	smallLimit = 4096 // the members' limit for the write-set tests
	smallRows  = 600  // 4800 bytes of write set, over smallLimit
)

const numsDDL = "CREATE TABLE nums (id INT NOT NULL PRIMARY KEY) ENGINE=InnoDB"

const numsJob = "\n    root_table: nums\n    primary_key: id\n    where: '1=1'\n"

// seedNums writes n small rows in one statement, before the limit is lowered.
func seedNums(t *testing.T, db *sql.DB, n int) {
	t.Helper()
	values := make([]string, n)
	for i := range values {
		values[i] = fmt.Sprintf("(%d)", i+1)
	}
	execAll(t, db, "INSERT INTO nums VALUES "+strings.Join(values, ","))
}

// writeSetFailure asserts err is a GroupWriteSetError of the given side and transaction and known limit, over
// MySQL error 3231 (asserted by its number and by the wrap, never by the server's text), and returns it.
func writeSetFailure(t *testing.T, err error, side string, commit Commit, limit uint64) *GroupWriteSetError {
	t.Helper()
	var gwe *GroupWriteSetError
	if !errors.As(err, &gwe) {
		t.Fatalf("Run() = %v (%T), want a GroupWriteSetError", err, err)
	}
	var me *mysql.MySQLError
	if !errors.As(err, &me) || me.Number != 3231 {
		t.Errorf("the chain holds %v, want MySQL error 3231", me)
	}
	if gwe.Side != side || gwe.Commit != commit || !gwe.Limit.Known || gwe.Limit.Bytes != limit {
		t.Errorf("GroupWriteSetError = side %q commit %d limit %+v, want %s, commit %d, a known limit of %d", gwe.Side, gwe.Commit, gwe.Limit, side, commit, limit)
	}
	return gwe
}

// stopDeletes makes every DELETE on table fail, by a trigger on the server: a run that copied its roots and
// then met it ends with them logged, and dropping the trigger lets the rerun's recovery delete them. The
// trigger is the server's state, and the run is interrupted by it as it would be by any failing statement.
func stopDeletes(t *testing.T, db *sql.DB, table string) (allow func()) {
	t.Helper()
	execAll(t, db, "CREATE TRIGGER "+table+"_stop BEFORE DELETE ON "+table+" FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'stopped by the test'")
	return func() { execAll(t, db, "DROP TRIGGER "+table+"_stop") }
}

// TestMySQLSourceWriteSetFailsTheDeleteChunk is spec §8's *write set, source*: a delete chunk whose write set is
// over the members' limit ends the run with the 3231 message; the source rows are all present and the roots are
// copied (deleting for a purge); the rerun with a lower batch_delete_size finishes them.
func TestMySQLSourceWriteSetFailsTheDeleteChunk(t *testing.T) {
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge} {
		t.Run(string(typ), func(t *testing.T) {
			requireGroupMemberSource(t)
			e := newEnv(t, "grwssrc"+string(typ))
			stuck := tracking.StatusCopied
			if typ == jobspec.Purge {
				execAll(t, e.src, numsDDL)
				stuck = tracking.StatusDeleting
			} else {
				e.both(t, numsDDL)
			}
			seedNums(t, e.src, smallRows)
			lowerGroupLimit(t, smallLimit)

			cfg := e.config(t, job(t, typ, numsJob))
			cfg.Processing = config.ProcessingConfig{BatchSize: smallRows, BatchDeleteSize: smallRows}
			_, err := e.run(t, cfg)
			t.Logf("the error: %v", err)
			gwe := writeSetFailure(t, err, "source", CommitDelete, smallLimit)
			if gwe.Setting != "batch_delete_size" || gwe.Value != smallRows || gwe.Limit.Member != preflight.MemberOnline {
				t.Errorf("GroupWriteSetError = %s=%d, member %d; want batch_delete_size=%d on an ONLINE member", gwe.Setting, gwe.Value, gwe.Limit.Member, smallRows)
			}
			for _, want := range []string{"the statement failed with MySQL error 3231 on the source, which was an ONLINE member of Group Replication group ",
				"4096 bytes", "Lower batch_delete_size (now 600) and rerun", "the roots stay logged as " + stuck.String()} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the message lacks %q:\n%s", want, err)
				}
			}
			if n := count(t, e.src, "SELECT COUNT(*) FROM nums"); n != smallRows {
				t.Errorf("source rows = %d, want all %d: the refused DELETE removed nothing", n, smallRows)
			}
			if got := statuses(t, e.dst, e.dstSchema); got[int(stuck)] != smallRows || len(got) != 1 {
				t.Errorf("the log = %v, want %d roots as %s", got, smallRows, stuck)
			}

			cfg.Processing.BatchDeleteSize = 100
			rep := e.mustRun(t, cfg)
			if rep.Recovered != smallRows {
				t.Errorf("the rerun recovered %d roots, want %d", rep.Recovered, smallRows)
			}
			if n := count(t, e.src, "SELECT COUNT(*) FROM nums"); n != 0 {
				t.Errorf("source rows after the rerun = %d, want 0", n)
			}
			if got := statuses(t, e.dst, e.dstSchema); got[int(tracking.StatusCompleted)] != smallRows || len(got) != 1 {
				t.Errorf("the log after the rerun = %v, want %d completed", got, smallRows)
			}
		})
	}
}

// TestMySQLDestinationWriteSetFailsAnInsert is spec §8's *write set, destination*, on a member destination (the
// roles swapped, as for the 3100 test): an archive's copy transaction of many small rows fails at an INSERT, a
// purge's log INSERT fails the same; nothing is copied, logged or deleted, the message names batch_size, and
// the rerun with a lower batch_size completes.
func TestMySQLDestinationWriteSetFailsAnInsert(t *testing.T) {
	for _, tc := range []struct {
		typ    jobspec.Type
		commit Commit
		lost   string
	}{
		{jobspec.Archive, CommitCopy, "the batch was rolled back, so nothing of it was copied, logged or deleted"},
		{jobspec.Purge, CommitLogBefore, "nothing of the batch was logged or deleted"},
	} {
		t.Run(string(tc.typ), func(t *testing.T) {
			requireGroupMemberSource(t)
			e := newEnvOn(t, "grwsdst"+string(tc.typ), "TEST_DEST", "TEST_SOURCE")
			if tc.typ == jobspec.Purge {
				execAll(t, e.src, numsDDL)
			} else {
				e.both(t, numsDDL)
			}
			seedNums(t, e.src, smallRows)
			lowerGroupLimit(t, smallLimit)

			cfg := e.config(t, job(t, tc.typ, numsJob))
			cfg.Processing = config.ProcessingConfig{BatchSize: smallRows, BatchDeleteSize: 100}
			_, err := e.run(t, cfg)
			t.Logf("the error: %v", err)
			gwe := writeSetFailure(t, err, "destination", tc.commit, smallLimit)
			if gwe.Setting != "batch_size" || gwe.Value != smallRows || gwe.Limit.Member != preflight.MemberUnread {
				t.Errorf("GroupWriteSetError = %s=%d, member %d; want batch_size=%d on a destination whose view is not read", gwe.Setting, gwe.Value, gwe.Limit.Member, smallRows)
			}
			for _, want := range []string{"the statement failed with MySQL error 3231 on the destination, where the Group Replication plugin is active (group ",
				"Lower batch_size (now 600) and rerun", tc.lost} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the message lacks %q:\n%s", want, err)
				}
			}
			if strings.Contains(err.Error(), "ONLINE member") {
				t.Errorf("the message says the destination was an ONLINE member, which it did not observe:\n%s", err)
			}
			if tc.typ == jobspec.Archive {
				if n := count(t, e.dst, "SELECT COUNT(*) FROM nums"); n != 0 {
					t.Errorf("destination rows = %d, want 0: the transaction rolled back", n)
				}
			}
			if got := statuses(t, e.dst, e.dstSchema); len(got) != 0 {
				t.Errorf("the log = %v, want nothing logged", got)
			}
			if n := count(t, e.src, "SELECT COUNT(*) FROM nums"); n != smallRows {
				t.Errorf("source rows = %d, want all %d: nothing was deleted", n, smallRows)
			}

			cfg.Processing.BatchSize = 20
			e.mustRun(t, cfg)
			if n := count(t, e.src, "SELECT COUNT(*) FROM nums"); n != 0 {
				t.Errorf("source rows after the rerun = %d, want 0", n)
			}
			if got := statuses(t, e.dst, e.dstSchema); got[int(tracking.StatusCompleted)] != smallRows || len(got) != 1 {
				t.Errorf("the log after the rerun = %v, want %d completed", got, smallRows)
			}
		})
	}
}

// interruptedOnAMemberDestination runs a job on a member destination (the roles swapped) whose source refuses
// every DELETE, so that it ends with its roots logged as copied (archive) or deleting (purge) and nothing
// deleted; the trigger is then dropped. Spec §8: the run is interrupted, and the limit is lowered after.
func interruptedOnAMemberDestination(t *testing.T, name string, typ jobspec.Type, table, ddl, seed, jobBody string, proc config.ProcessingConfig) (env, Config, tracking.Status) {
	t.Helper()
	e := newEnvOn(t, name+string(typ), "TEST_DEST", "TEST_SOURCE")
	stuck := tracking.StatusCopied
	if typ == jobspec.Purge {
		execAll(t, e.src, ddl)
		stuck = tracking.StatusDeleting
	} else {
		e.both(t, ddl)
	}
	execAll(t, e.src, seed)
	allow := stopDeletes(t, e.src, table)
	cfg := e.config(t, job(t, typ, jobBody))
	cfg.Processing, cfg.ForceTriggers = proc, true
	_, err := e.run(t, cfg)
	if err == nil || !strings.Contains(err.Error(), "stopped by the test") {
		t.Fatalf("the first run = %v, want it to end at the refused DELETE", err)
	}
	if got := statuses(t, e.dst, e.dstSchema); got[int(stuck)] == 0 || len(got) != 1 {
		t.Fatalf("the log after the interrupted run = %v, want the roots as %s", got, stuck)
	}
	allow()
	cfg.ForceTriggers = false
	return e, cfg, stuck
}

// TestMySQLLogWriteAfterTheDeletesMeets3231 is spec §8's *log write after the deletes, through recovery*: on a
// member destination an interrupted run leaves roots logged; the members' limit is then lowered; the rerun's
// recovery deletes the page's rows and its log UPDATE fails with 3231. The rows are deleted, the roots stay as
// logged, the message names batch_size, and the rerun with a lower batch_size completes them.
func TestMySQLLogWriteAfterTheDeletesMeets3231(t *testing.T) {
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge} {
		t.Run(string(typ), func(t *testing.T) {
			requireGroupMemberSource(t)
			values := make([]string, smallRows)
			for i := range values {
				values[i] = fmt.Sprintf("(%d)", i+1)
			}
			e, cfg, stuck := interruptedOnAMemberDestination(t, "grwslog", typ, "nums", numsDDL,
				"INSERT INTO nums VALUES "+strings.Join(values, ","), numsJob,
				config.ProcessingConfig{BatchSize: smallRows, BatchDeleteSize: 100})
			lowerGroupLimit(t, smallLimit)

			_, err := e.run(t, cfg)
			t.Logf("the error: %v", err)
			gwe := writeSetFailure(t, err, "destination", CommitLogAfter, smallLimit)
			if gwe.Setting != "batch_size" || gwe.Value != smallRows || gwe.Roots != stuck {
				t.Errorf("GroupWriteSetError = %s=%d roots %s; want batch_size=%d, %s", gwe.Setting, gwe.Value, gwe.Roots, smallRows, stuck)
			}
			if want := "the batch's rows are deleted and its roots stay logged as " + stuck.String(); !strings.Contains(err.Error(), want) {
				t.Errorf("the message lacks %q:\n%s", want, err)
			}
			if n := count(t, e.src, "SELECT COUNT(*) FROM nums"); n != 0 {
				t.Errorf("source rows = %d, want 0: recovery deleted them before the log UPDATE failed", n)
			}
			if got := statuses(t, e.dst, e.dstSchema); got[int(stuck)] != smallRows || len(got) != 1 {
				t.Errorf("the log = %v, want %d roots still as %s", got, smallRows, stuck)
			}

			cfg.Processing.BatchSize = 20
			rep := e.mustRun(t, cfg)
			if rep.Recovered != smallRows {
				t.Errorf("the rerun recovered %d roots, want %d", rep.Recovered, smallRows)
			}
			if got := statuses(t, e.dst, e.dstSchema); got[int(tracking.StatusCompleted)] != smallRows || len(got) != 1 {
				t.Errorf("the log after the rerun = %v, want %d completed", got, smallRows)
			}
		})
	}
}

// TestMySQLLogWriteAfterTheDeletesMeets3100 is the one integration attempt of spec §8's *the log writes' 3100*:
// recovery's log UPDATE is the first write of its size on the destination, so a limit over its write set and
// under its size reaches 3100 at the commit. A wide root key widens that range: the write set stays 8 bytes
// a row while each row image holds the key twice. The limit tried is 8192: a hundred roots of a 200-byte key
// are 800 bytes of write set and about 40 KB of row images.
func TestMySQLLogWriteAfterTheDeletesMeets3100(t *testing.T) {
	requireGroupMemberSource(t)
	const wideRows = 100
	values := make([]string, wideRows)
	for i := range values {
		values[i] = fmt.Sprintf("(CONCAT(LPAD(%d, 10, '0'), REPEAT('x', 190)))", i+1)
	}
	e, cfg, stuck := interruptedOnAMemberDestination(t, "grlog3100", jobspec.Archive, "wide",
		"CREATE TABLE wide (id VARCHAR(200) CHARACTER SET ascii NOT NULL PRIMARY KEY) ENGINE=InnoDB",
		"INSERT INTO wide VALUES "+strings.Join(values, ","),
		"\n    root_table: wide\n    primary_key: id\n    where: '1=1'\n",
		config.ProcessingConfig{BatchSize: wideRows, BatchDeleteSize: 50})
	const limit = 8192
	lowerGroupLimit(t, limit)

	_, err := e.run(t, cfg)
	t.Logf("the error: %v", err)
	gce := groupCommitFailure(t, err, "destination", CommitLogAfter, limit)
	if gce.Setting != "batch_size" || gce.Value != wideRows || gce.Roots != stuck {
		t.Errorf("GroupCommitError = %s=%d roots %s; want batch_size=%d, %s", gce.Setting, gce.Value, gce.Roots, wideRows, stuck)
	}
	if n := count(t, e.src, "SELECT COUNT(*) FROM wide"); n != 0 {
		t.Errorf("source rows = %d, want 0: recovery deleted them before the log UPDATE failed", n)
	}
	if got := statuses(t, e.dst, e.dstSchema); got[int(stuck)] != wideRows || len(got) != 1 {
		t.Errorf("the log = %v, want %d roots still as %s", got, wideRows, stuck)
	}

	cfg.Processing.BatchSize = 10
	e.mustRun(t, cfg)
	if got := statuses(t, e.dst, e.dstSchema); got[int(tracking.StatusCompleted)] != wideRows || len(got) != 1 {
		t.Errorf("the log after the rerun = %v, want %d completed", got, wideRows)
	}
}
