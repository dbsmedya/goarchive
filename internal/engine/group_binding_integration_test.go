//go:build integration

package engine

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
	"github.com/dbsmedya/goarchive/v3/internal/testestate"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
)

// The binding and the guard on a Group Replication group (spec §4.2, §8; GR C9). A run on an ONLINE member is bound to the
// group's name, any other source to its server's uuid, and the unfinished-work guard finds a job recorded on the group from
// every server that holds the group's data. They need a group source and skip on the plain estate; the group estate runs them.

// groupName is the group's name, from the library's fact on the group's primary.
func groupName(t *testing.T) string {
	t.Helper()
	g, err := dbsrepl.NewInspector(testestate.OpenUnfenced(t, "TEST_SOURCE", "")).GroupReplication(context.Background())
	if err != nil || g.GroupName == "" {
		t.Fatalf("the group's name: %q, %v", g.GroupName, err)
	}
	return g.GroupName
}

// serverUUID is a test server's own uuid.
func serverUUID(t *testing.T, prefix string) string {
	t.Helper()
	var u string
	if err := testestate.OpenUnfenced(t, prefix, "").QueryRowContext(context.Background(), "SELECT @@server_uuid").Scan(&u); err != nil {
		t.Fatal(err)
	}
	return u
}

// registeredValue is the source value job "j" is recorded under.
func registeredValue(t *testing.T, e env) string {
	t.Helper()
	var v string
	if err := e.dst.QueryRowContext(context.Background(), "SELECT source_server_uuid FROM `"+e.dstSchema+"`.archiver_job WHERE job_name = 'j'").Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

// setRegistered puts the value in job "j"'s row, as an operator or an earlier release left it.
func setRegistered(t *testing.T, e env, value string) {
	t.Helper()
	execAll(t, e.dst, "UPDATE `"+e.dstSchema+"`.archiver_job SET source_server_uuid = '"+value+"' WHERE job_name = 'j'")
}

// waitApplied waits until the server at prefix has applied everything the primary has committed, so that a run's source
// on it holds the test's schema and rows.
func waitApplied(t *testing.T, prefix string, db *sql.DB) {
	t.Helper()
	var gtids string
	if err := testestate.OpenUnfenced(t, "TEST_SOURCE", "").QueryRowContext(context.Background(), "SELECT @@GLOBAL.gtid_executed").Scan(&gtids); err != nil {
		t.Fatal(err)
	}
	var rc sql.NullInt64
	if err := db.QueryRowContext(context.Background(), "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 60)", gtids).Scan(&rc); err != nil || rc.Int64 != 0 {
		t.Fatalf("%s did not apply the primary's transactions: %v (%v)", prefix, rc, err)
	}
}

// settled is a job left with pending work on the primary, or with none, and the config of its next run.
type settled struct {
	e       env
	cfg     Config
	pending bool
}

// withWork builds a job of type typ on the group's primary: pending is true for the roots that bind it (copied for an
// archive, deleting for a purge, completed for a copy-only job), and false for a job that finished with nothing pending
// (an archive and a purge that ran to the end, a copy-only job on an empty source).
func withWork(t *testing.T, name string, typ jobspec.Type, pending bool) settled {
	t.Helper()
	switch {
	case pending && typ != jobspec.CopyOnly:
		e, cfg, _ := pendingWork(t, name, typ)
		return settled{e, cfg, true}
	case pending:
		e := newEnv(t, name+"co")
		e.both(t, numsDDL)
		seedNums(t, e.src, 20)
		cfg := e.config(t, job(t, typ, numsJob))
		e.mustRun(t, cfg)
		return settled{e, cfg, true}
	}
	e := newEnv(t, name+string(typ)+"clean")
	if typ == jobspec.Purge {
		execAll(t, e.src, numsDDL)
	} else {
		e.both(t, numsDDL)
	}
	if typ != jobspec.CopyOnly {
		seedNums(t, e.src, 20)
	}
	cfg := e.config(t, job(t, typ, numsJob))
	e.mustRun(t, cfg)
	return settled{e, cfg, false}
}

// TestMySQLAJobOnAMemberIsBoundToTheGroupName: the registry's value for a run on an ONLINE member is the group's name, for
// every job type, and never the member's own uuid.
func TestMySQLAJobOnAMemberIsBoundToTheGroupName(t *testing.T) {
	requireGroupMemberSource(t)
	group, own := groupName(t), serverUUID(t, "TEST_SOURCE")
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
		t.Run(string(typ), func(t *testing.T) {
			s := withWork(t, "grbound", typ, false)
			if got := registeredValue(t, s.e); got != group || got == own {
				t.Errorf("a %s job on the primary is recorded under %q, want the group's name %q and not the server's uuid %q", typ, got, group, own)
			}
		})
	}
}

// TestMySQLAUuidRowMeetsAnOnlineMember is spec §8's *Standalone, then a member* and the table's row for a 3.0 row: a registry
// row holding this server's own uuid (what a standalone run writes, M12) or another member's (what a 3.0 run on another
// member wrote), met by a run on the primary as an ONLINE member, is a BindingError with pending work, for an archive, a
// purge and a copy-only job, and nothing is replayed, copied or deleted; with nothing pending the row is rewritten to the
// group's name. Archive and purge are tested on the writable primary.
func TestMySQLAUuidRowMeetsAnOnlineMember(t *testing.T) {
	requireGroupMemberSource(t)
	group := groupName(t)
	for _, row := range []struct{ name, prefix string }{{"this server's own uuid", "TEST_SOURCE"}, {"another member's uuid", "TEST_GROUP_MEMBER2"}} {
		stored := serverUUID(t, row.prefix)
		for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
			t.Run(row.name+"/"+string(typ)+"/pending work is refused", func(t *testing.T) {
				s := withWork(t, "grrow", typ, true)
				setRegistered(t, s.e, stored)
				before, src := statuses(t, s.e.dst, s.e.dstSchema), tableRows(t, s.e, "nums")
				_, err := s.e.run(t, s.cfg)
				refused(t, err, tracking.PartSource)
				if got := registeredValue(t, s.e); got != stored {
					t.Errorf("a refused run rewrote the row to %q, want it kept as %q", got, stored)
				}
				if after := statuses(t, s.e.dst, s.e.dstSchema); !sameStatuses(before, after) {
					t.Errorf("the log changed in a refused run: %v, was %v", after, before)
				}
				if got := tableRows(t, s.e, "nums"); got != src {
					t.Errorf("a refused run changed the source: %d rows, was %d", got, src)
				}
			})
			t.Run(row.name+"/"+string(typ)+"/nothing pending is rewritten", func(t *testing.T) {
				s := withWork(t, "grrow", typ, false)
				setRegistered(t, s.e, stored)
				s.e.mustRun(t, s.cfg)
				if got := registeredValue(t, s.e); got != group {
					t.Errorf("the row is %q after a run with nothing pending, want it rewritten to the group's name %q", got, group)
				}
			})
		}
	}
}

// sameStatuses compares two status counts.
func sameStatuses(a, b map[int]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// tableRows counts a table's rows on the env's source.
func tableRows(t *testing.T, e env, table string) int64 {
	t.Helper()
	return count(t, e.src, "SELECT COUNT(*) FROM `"+table+"`")
}

// TestMySQLAGroupNameRowMeetsAnyOnlineMemberAndNoServerOutside: a copy-only job's completed roots pin its binding. Recorded
// on the group's name, a run of it on either secondary (a member, ONLINE, read-only, which copy-only is exempt from) passes
// without a re-bind; a run on the asynchronous replica of the primary, a server outside the group that carries the same data
// under its own uuid, is a BindingError, and the registry is left as it was.
func TestMySQLAGroupNameRowMeetsAnyOnlineMemberAndNoServerOutside(t *testing.T) {
	requireGroupMemberSource(t)
	group := groupName(t)
	e := newEnv(t, "grany")
	e.both(t, numsDDL)
	seedNums(t, e.src, 20)
	spec := job(t, jobspec.CopyOnly, numsJob+"    resume: true\n")
	e.mustRun(t, e.config(t, spec))
	if got := registeredValue(t, e); got != group {
		t.Fatalf("the first run recorded %q, want %q", got, group)
	}
	for _, prefix := range []string{"TEST_GROUP_MEMBER2", "TEST_GROUP_MEMBER3"} {
		t.Run(prefix, func(t *testing.T) {
			db := openServer(t, prefix, e.srcSchema, nil)
			waitApplied(t, prefix, db)
			cfg := e.config(t, spec)
			cfg.Source = db
			if rep := e.mustRun(t, cfg); rep.Skipped != 20 {
				t.Errorf("a rerun on %s skipped %d roots, want the 20 the first run completed: %+v", prefix, rep.Skipped, rep)
			}
			if got := registeredValue(t, e); got != group {
				t.Errorf("a run on %s left the row as %q, want it kept as the group's name %q", prefix, got, group)
			}
		})
	}
	t.Run("a server outside the group", func(t *testing.T) {
		db := openServer(t, "TEST_REPLICA", e.srcSchema, nil)
		waitApplied(t, "TEST_REPLICA", db)
		cfg := e.config(t, spec)
		cfg.Source = db
		_, err := e.run(t, cfg)
		refused(t, err, tracking.PartSource)
		if got := registeredValue(t, e); got != group {
			t.Errorf("a refused run left the row as %q, want %q", got, group)
		}
	})
}

// TestMySQLTheGuardFindsAJobRecordedOnTheGroupFromOutside is spec §8's *Guard from outside*: a job with pending work recorded
// on the group (copied roots, its name the group's) and a second job that shares its table, run against the asynchronous
// replica of the primary, which is outside the group and holds the group's transactions under the group's name in its
// gtid_executed: refused by the guard, naming the first.
func TestMySQLTheGuardFindsAJobRecordedOnTheGroupFromOutside(t *testing.T) {
	requireGroupMemberSource(t)
	e, _, _ := pendingWork(t, "grout", jobspec.Archive)
	if got := registeredValue(t, e); got != groupName(t) {
		t.Fatalf("the first job is recorded under %q, want the group's name", got)
	}
	db := openServer(t, "TEST_REPLICA", e.srcSchema, nil)
	waitApplied(t, "TEST_REPLICA", db)
	second := e.config(t, job(t, jobspec.CopyOnly, numsJob))
	second.Name, second.Source = "second", db
	_, err := e.run(t, second)
	oe := wantErr[*OverlapError](t, err)
	if !strings.Contains(oe.Error(), "\n  j: ") {
		t.Errorf("the refusal does not name the job recorded on the group:\n%s", oe)
	}
	if n := count(t, e.dst, "SELECT COUNT(*) FROM archiver_job WHERE job_name = 'second'"); n != 0 {
		t.Errorf("the refused job registered itself")
	}
}

// TestMySQLTheGuardOnAStoppedMember is spec §8's *Guard on a stopped member*: a job with pending work recorded on the
// primary; Group Replication is stopped on another member, which keeps its configuration and its data; a copy-only job
// sharing the first job's table, run against that member, is refused by the guard, naming the first. The member's group name
// stays configured, so the guard's rule 2 holds in a state that has no row of its own to read.
func TestMySQLTheGuardOnAStoppedMember(t *testing.T) {
	requireGroupMemberSource(t)
	e, _, _ := pendingWork(t, "grstop", jobspec.Archive)
	gr3 := testestate.OpenUnfenced(t, "TEST_GROUP_MEMBER3", "")
	waitApplied(t, "TEST_GROUP_MEMBER3", gr3)
	testestate.StopMember(t, gr3)
	t.Cleanup(func() { testestate.StartMember(t, gr3) })

	second := e.config(t, job(t, jobspec.CopyOnly, numsJob))
	second.Name, second.Source = "second", openServer(t, "TEST_GROUP_MEMBER3", e.srcSchema, nil)
	_, err := e.run(t, second)
	oe := wantErr[*OverlapError](t, err)
	if !strings.Contains(oe.Error(), "\n  j: ") || !strings.Contains(oe.Error(), "configured for Group Replication group") {
		t.Errorf("the refusal does not name the first job and the configured group:\n%s", oe)
	}
}

// TestMySQLAnUnconvertedMemberRowIsFoundOnAMemberAndNotFromOutside is spec §8's *An unconverted member row and the outside
// reader*: a pending job still recorded on the uuid of a member (what a 3.0 run on it wrote, left unconverted by the upgrade).
// On a member the guard's rule 2 finds it; the asynchronous replica of the primary, outside the group, does not find it
// unless its data holds the uuid: pinned as the documented, narrowed limit.
func TestMySQLAnUnconvertedMemberRowIsFoundOnAMemberAndNotFromOutside(t *testing.T) {
	requireGroupMemberSource(t)
	e, _, _ := pendingWork(t, "grunconv", jobspec.Archive)
	// a member that has left the group: its uuid is nowhere in the data any server holds
	setRegistered(t, e, "f11ed000-57a1-4c0d-9e5e-0000000000ee")
	second := e.config(t, job(t, jobspec.CopyOnly, numsJob))
	second.Name = "second"

	_, err := e.run(t, second)
	oe := wantErr[*OverlapError](t, err)
	if !strings.Contains(oe.Error(), "\n  j: ") {
		t.Errorf("on a member the guard does not name the unconverted job:\n%s", oe)
	}

	db := openServer(t, "TEST_REPLICA", e.srcSchema, nil)
	waitApplied(t, "TEST_REPLICA", db)
	second.Source = db
	if _, err := e.run(t, second); err != nil {
		var pe *preflight.Error
		if errors.As(err, &pe) {
			t.Fatalf("the run from outside was refused by preflight: %v", err)
		}
		if _, ok := err.(*OverlapError); ok {
			t.Errorf("the guard on the replica found a job recorded on a uuid its data does not hold; the documented limit says it cannot:\n%v", err)
		}
	}
}

// TestMySQLAFailoverKeepsTheJobsBinding is the first half of spec §8's *Failover*: an archive interrupted on the primary
// with copied roots and a purge with deleting roots; the primary is switched to gr2; the rerun on the new primary finishes
// each with no re-bind, and the registry row holds the group's name before and after.
func TestMySQLAFailoverKeepsTheJobsBinding(t *testing.T) {
	requireGroupMemberSource(t)
	group := groupName(t)
	jobs := map[jobspec.Type]settled{}
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge} {
		e, cfg, _ := pendingWork(t, "grfail", typ)
		if got := registeredValue(t, e); got != group {
			t.Fatalf("a %s job on the primary is recorded under %q, want %q", typ, got, group)
		}
		jobs[typ] = settled{e, cfg, true}
	}
	gr1, gr2 := testestate.OpenUnfenced(t, "TEST_SOURCE", ""), testestate.OpenUnfenced(t, "TEST_GROUP_MEMBER2", "")
	switchPrimaryTo(t, gr1, gr2)

	for typ, s := range jobs {
		t.Run(string(typ), func(t *testing.T) {
			cfg := s.cfg
			cfg.Source = openServer(t, "TEST_GROUP_MEMBER2", s.e.srcSchema, nil)
			s.e.mustRun(t, cfg)
			if got := registeredValue(t, s.e); got != group {
				t.Errorf("the row is %q after the failover, want it kept as %q: the job was re-bound", got, group)
			}
			if got := statuses(t, s.e.dst, s.e.dstSchema); len(got) != 1 || got[int(tracking.StatusCompleted)] != 20 {
				t.Errorf("the log after the rerun = %v, want 20 completed roots", got)
			}
			if n := tableRows(t, s.e, "nums"); n != 0 {
				t.Errorf("source rows = %d on the new primary after the rerun, want 0", n)
			}
		})
	}
}

// switchPrimaryTo switches the group's primary from from's server to to's and waits until the new primary is writable; the
// cleanup switches it back and waits for the canonical view.
func switchPrimaryTo(t *testing.T, from, to *sql.DB) {
	t.Helper()
	var fromUUID, toUUID string
	for db, dst := range map[*sql.DB]*string{from: &fromUUID, to: &toUUID} {
		if err := db.QueryRowContext(context.Background(), "SELECT @@server_uuid").Scan(dst); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		if testestate.MemberRole(t, to) == "PRIMARY" {
			if err := setPrimary(t, to, fromUUID); err != nil {
				t.Errorf("switch the primary back: %v", err)
			}
		}
		waitUntilTrue(t, 90*time.Second, "the primary to be the original again", func() bool { return testestate.MemberRole(t, from) == "PRIMARY" })
	})
	if err := setPrimary(t, from, toUUID); err != nil {
		t.Fatal(err)
	}
	waitUntilTrue(t, 60*time.Second, "the new primary to be writable", func() bool {
		return testestate.MemberRole(t, to) == "PRIMARY" && !testestate.SuperReadOnly(t, to)
	})
}

// TestMySQLAFailoverWithABacklogRefusesThenFinishes is the second half of *Failover*, with M5's server state: the new
// primary is read-only with its backlog held. The rerun of an archive and of a purge is refused by SOURCE_READ_ONLY_CHECK
// and nothing is replayed or marked completed, and the registry still holds the group's name; once the backlog is applied a
// rerun finishes the work, and the source, the destination and every status agree.
func TestMySQLAFailoverWithABacklogRefusesThenFinishes(t *testing.T) {
	requireGroupMemberSource(t)
	group := groupName(t)
	jobs := map[jobspec.Type]settled{}
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge} {
		e, cfg, _ := pendingWork(t, "grbacklog", typ)
		jobs[typ] = settled{e, cfg, true}
	}
	f := newlyElectedPrimary(t)
	onGr2 := func(s settled) Config {
		cfg := s.cfg
		cfg.Source = openServer(t, "TEST_GROUP_MEMBER2", s.e.srcSchema, nil)
		return cfg
	}
	for typ, s := range jobs {
		_, err := s.e.run(t, onGr2(s))
		if pe := wantErr[*preflight.Error](t, err); pe.Check != "SOURCE_READ_ONLY_CHECK" {
			t.Errorf("a %s rerun on the new primary = %v, want SOURCE_READ_ONLY_CHECK", typ, err)
		}
		if got := statuses(t, s.e.dst, s.e.dstSchema); len(got) != 1 || got[int(map[jobspec.Type]tracking.Status{jobspec.Archive: tracking.StatusCopied, jobspec.Purge: tracking.StatusDeleting}[typ])] != 20 {
			t.Errorf("the %s log = %v, want 20 roots still pending: none replayed or completed", typ, got)
		}
		if got := registeredValue(t, s.e); got != group {
			t.Errorf("the %s row is %q, want it kept as the group's name", typ, got)
		}
	}
	f.settle(t)
	for typ, s := range jobs {
		s.e.mustRun(t, onGr2(s))
		if got := statuses(t, s.e.dst, s.e.dstSchema); len(got) != 1 || got[int(tracking.StatusCompleted)] != 20 {
			t.Errorf("the %s log after the backlog = %v, want 20 completed roots", typ, got)
		}
		if got := registeredValue(t, s.e); got != group {
			t.Errorf("the %s row is %q after the rerun, want it kept as the group's name", typ, got)
		}
		if n := count(t, openServer(t, "TEST_GROUP_MEMBER2", s.e.srcSchema, nil), "SELECT COUNT(*) FROM nums"); n != 0 {
			t.Errorf("the %s source holds %d rows on the new primary, want 0", typ, n)
		}
		if typ == jobspec.Archive {
			if n := count(t, s.e.dst, "SELECT COUNT(*) FROM nums"); n != 20 {
				t.Errorf("the archive's destination holds %d rows, want 20", n)
			}
		}
	}
}
