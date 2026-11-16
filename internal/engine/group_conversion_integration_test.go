//go:build integration

package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
)

// upgradeGuide is docs/README_UPGRADING_3_1.md, *Moving from 3.0*: the procedure's own statements are run by the tests below,
// taken from the doc and not retyped, so the doc cannot drift from what the binary does with them.
func upgradeGuide(t *testing.T) (section string, sql []string) {
	t.Helper()
	raw, err := os.ReadFile("../../docs/README_UPGRADING_3_1.md")
	if err != nil {
		t.Fatal(err)
	}
	doc := string(raw)
	start := strings.Index(doc, "\n## Moving from 3.0\n")
	if start < 0 {
		t.Fatal("docs/README_UPGRADING_3_1.md has no heading \"Moving from 3.0\"")
	}
	section = doc[start+1:]
	if end := strings.Index(section[3:], "\n## "); end >= 0 {
		section = section[:end+3]
	}
	for _, m := range regexp.MustCompile("(?s)```sql\n(.*?)```").FindAllStringSubmatch(section, -1) {
		sql = append(sql, m[1])
	}
	return section, sql
}

// statementWith returns the doc's SQL block holding every one of parts.
func statementWith(t *testing.T, blocks []string, parts ...string) string {
	t.Helper()
	for _, b := range blocks {
		ok := true
		for _, p := range parts {
			ok = ok && strings.Contains(b, p)
		}
		if ok {
			return b
		}
	}
	t.Fatalf("no SQL block of the upgrade guide holds %q", parts)
	return ""
}

// twoPendingJobs builds a 3.0-labelled registry with two pending archive jobs, j on nums and k on nums2 (separate trees, so the
// unfinished-work guard does not set them against each other), both rows holding the primary's own uuid: j is "a genuine
// group job" in the operator's mapping and k "the row a standalone run wrote" (M12), which the data cannot tell apart.
func twoPendingJobs(t *testing.T, name string) (e env, j, k Config) {
	t.Helper()
	e = newEnv(t, name)
	nums2 := strings.Replace(numsDDL, "nums", "nums2", 1)
	e.both(t, numsDDL, nums2)
	seedNums(t, e.src, 20)
	execAll(t, e.src, "INSERT INTO nums2 SELECT id FROM nums")
	allow1, allow2 := stopDeletes(t, e.src, "nums"), stopDeletes(t, e.src, "nums2")
	j = e.config(t, job(t, jobspec.Archive, numsJob))
	k = e.config(t, job(t, jobspec.Archive, strings.Replace(numsJob, "nums", "nums2", 1)))
	k.Name = "k"
	for _, c := range []*Config{&j, &k} {
		c.Processing, c.ForceTriggers = config.ProcessingConfig{BatchSize: 20, BatchDeleteSize: 20}, true
		if _, err := e.run(t, *c); err == nil || !strings.Contains(err.Error(), "stopped by the test") {
			t.Fatalf("the first run of %s = %v, want it to end at the refused DELETE", c.Name, err)
		}
		c.ForceTriggers = false
	}
	allow1()
	allow2()
	own := serverUUID(t, "TEST_SOURCE")
	execAll(t, e.dst, "UPDATE `"+e.dstSchema+"`.archiver_job SET source_server_uuid = '"+own+"'",
		"UPDATE `"+e.dstSchema+"`.goarchive_meta SET schema_version = '3.0' WHERE id = 1")
	return e, j, k
}

// rowOf reads a job's registry row.
func rowOf(t *testing.T, e env, name string) (id int64, value string) {
	t.Helper()
	if err := e.dst.QueryRowContext(context.Background(), "SELECT id, source_server_uuid FROM `"+e.dstSchema+"`.archiver_job WHERE job_name = ?", name).Scan(&id, &value); err != nil {
		t.Fatal(err)
	}
	return id, value
}

// TestMySQLTheConversionOnAMixedRegistry is spec §8's *The conversion, on a mixed registry*: a 3.0-labelled registry with two
// pending jobs bound to the same member uuid, one of them a group job. 3.1 refuses it, naming the procedure, and changes
// nothing in it; the procedure's own statements (run as the doc gives them) convert the group job with an UPDATE that changes
// exactly one row and cannot change the other's, and relabel the schema; after that the group job continues, and the other is
// a BindingError with nothing replayed, skipped or deleted.
func TestMySQLTheConversionOnAMixedRegistry(t *testing.T) {
	requireGroupMemberSource(t)
	group, own := groupName(t), serverUUID(t, "TEST_SOURCE")
	section, blocks := upgradeGuide(t)
	perJob := statementWith(t, blocks, "SET source_server_uuid = '<group name>'", "WHERE id = <job id>", "AND source_server_uuid = '<old value>'", "AND source_database = '<source database>'")
	relabel := statementWith(t, blocks, "goarchive_meta SET schema_version = '3.1' WHERE id = 1")
	e, j, k := twoPendingJobs(t, "grconv")
	idJ, _ := rowOf(t, e, "j")
	idK, _ := rowOf(t, e, "k")

	// 3.1 refuses the schema as it is, and writes nothing
	_, err := e.run(t, j)
	var se *tracking.SchemaError
	if !errors.As(err, &se) || !strings.Contains(err.Error(), `docs/README_UPGRADING_3_1.md, "Moving from 3.0"`) {
		t.Fatalf("a run on a 3.0 registry = %v, want a SchemaError naming \"Moving from 3.0\"", err)
	}
	if got := count(t, e.dst, "SELECT COUNT(*) FROM goarchive_meta WHERE schema_version = '3.0'"); got != 1 {
		t.Errorf("the refused schema's label changed")
	}
	if _, v := rowOf(t, e, "j"); v != own {
		t.Errorf("the refusal rewrote a row: %q", v)
	}

	// the doc's statement, with the mapping's values: scoped to the row and to the expected old value
	apply := func(sqlText string, id int64, old, source string) int64 {
		stmt := strings.NewReplacer("<job_schema>", "`"+e.dstSchema+"`", "<group name>", group, "<job id>", fmt.Sprint(id), "<old value>", old,
			"<source database>", source).Replace(sqlText)
		stmt = strings.TrimSpace(stmt)
		stmt = regexp.MustCompile(`(?m)^\s*--.*$`).ReplaceAllString(stmt, "")
		res, err := e.dst.ExecContext(context.Background(), strings.TrimSpace(stmt))
		if err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
		n, _ := res.RowsAffected()
		return n
	}
	if n := apply(perJob, idJ, "f11ed000-57a1-4c0d-9e5e-00000000dead", e.srcSchema); n != 0 {
		t.Errorf("the statement with the wrong old value changed %d rows, want 0: it is scoped to the value the mapping recorded", n)
	}
	if n := apply(perJob, idJ, own, "another_database"); n != 0 {
		t.Errorf("the statement with the wrong source database changed %d rows, want 0", n)
	}
	if n := apply(perJob, idJ, own, e.srcSchema); n != 1 {
		t.Fatalf("the statement for j changed %d rows, want exactly 1", n)
	}
	if _, v := rowOf(t, e, "k"); v != own {
		t.Errorf("the statement for j changed k's row to %q, want it kept as %q", v, own)
	}
	if n := apply(perJob, idJ, own, e.srcSchema); n != 0 {
		t.Errorf("the statement repeated changed %d rows, want 0", n)
	}
	_ = idK

	apply(relabel, 0, "", "")
	beforeK, srcK := statuses(t, e.dst, e.dstSchema), tableRows(t, e, "nums2")

	// the group job continues
	e.mustRun(t, j)
	if _, v := rowOf(t, e, "j"); v != group {
		t.Errorf("j is recorded under %q after the conversion, want the group's name", v)
	}
	// the other is refused, and nothing is replayed, skipped or deleted
	_, err = e.run(t, k)
	refused(t, err, tracking.PartSource)
	if _, v := rowOf(t, e, "k"); v != own {
		t.Errorf("a refused run rewrote k's row to %q", v)
	}
	if got := tableRows(t, e, "nums2"); got != srcK {
		t.Errorf("a refused run changed k's source: %d rows, was %d", got, srcK)
	}
	_ = beforeK

	// the doc says the shortcut stands only under its condition
	flat := strings.Join(strings.Fields(section), " ")
	if !strings.Contains(flat, "is right only when the mapping shows that **every** row with that uuid is a group job") {
		t.Errorf("the doc does not state the by-uuid shortcut's condition")
	}
	if !strings.Contains(flat, `A row that is "not a group job" or "not known" is not converted.`) {
		t.Errorf("the doc does not say that a row that is not a group job is left as it is")
	}
}

// TestMySQLTheByUUIDShortcutConvertsEveryRowOfAMember: the doc's shortcut statement, run as the doc gives it where the mapping
// shows that every row with that uuid is a group job, converts them all, and after the relabel both jobs continue.
func TestMySQLTheByUUIDShortcutConvertsEveryRowOfAMember(t *testing.T) {
	requireGroupMemberSource(t)
	group, own := groupName(t), serverUUID(t, "TEST_SOURCE")
	_, blocks := upgradeGuide(t)
	shortcut := statementWith(t, blocks, "WHERE source_server_uuid = '<member uuid>'")
	relabel := statementWith(t, blocks, "goarchive_meta SET schema_version = '3.1' WHERE id = 1")
	e, j, k := twoPendingJobs(t, "grshort")
	run := func(sqlText string) int64 {
		stmt := strings.NewReplacer("<job_schema>", "`"+e.dstSchema+"`", "<group name>", group, "<member uuid>", own).Replace(sqlText)
		res, err := e.dst.ExecContext(context.Background(), strings.TrimSpace(stmt))
		if err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
		n, _ := res.RowsAffected()
		return n
	}
	if n := run(shortcut); n != 2 {
		t.Fatalf("the shortcut changed %d rows, want both jobs'", n)
	}
	run(relabel)
	e.mustRun(t, j)
	e.mustRun(t, k)
	for _, name := range []string{"j", "k"} {
		if _, v := rowOf(t, e, name); v != group {
			t.Errorf("%s is recorded under %q, want the group's name", name, v)
		}
	}
}
