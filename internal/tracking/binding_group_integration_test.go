//go:build integration

package tracking

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
)

// The binding rule of spec §4.2 with a group's data identity as its value (GR C9). The identity (the group name for an ONLINE
// member, else the server's uuid) is what a run hands the registry as its source, and the rule has no exception: the
// stored value equals the identity, or it is a different source. These are the table's five rows, for archive (copied),
// purge (deleting) and copy-only (completed): refused with pending work, rewritten without. Which identity a source has is
// preflight's function and the engine's tests; here the values are the strings the function would give.

const (
	groupName = "8a94f357-aab4-11df-86ab-c80aa9429563"
	thisUUID  = "11111111-1111-4111-8111-111111111111"
	otherUUID = "22222222-2222-4222-8222-222222222222"
)

func TestMySQLTheBindingTableOfAGroup(t *testing.T) {
	ctx := context.Background()
	schema, db := scratch(t, "grbind")
	exec(t, db, ordersDDL)
	root := rootKey(t, db, schema, "orders")
	s := mustOpen(t, db, schema)
	k := key.Key{[]byte("eu"), "2024-01-01 00:00:00.000", uint64(1)}
	storedValue := func(name string) string {
		var v string
		if err := db.QueryRow("SELECT source_server_uuid FROM archiver_job WHERE job_name = ?", name).Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	pending := map[jobspec.Type]Status{jobspec.Archive: StatusCopied, jobspec.Purge: StatusDeleting, jobspec.CopyOnly: StatusCompleted}
	for _, row := range []struct {
		name            string
		stored, current string
		refusedPending  bool // false: the row passes with pending work, and there is nothing to rewrite
	}{
		{"the group name, now any ONLINE member of that group", groupName, groupName, false},
		{"this server's uuid, the same server standalone", thisUUID, thisUUID, false},
		{"this server's uuid, the same server now an ONLINE member", thisUUID, groupName, true},
		{"another member's uuid (a 3.0 row), now an ONLINE member", otherUUID, groupName, true},
		{"the group name, a server outside the group or a member that is not ONLINE", groupName, thisUUID, true},
	} {
		for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
			t.Run(fmt.Sprintf("%s/%s", typ, row.name), func(t *testing.T) {
				name := strings.NewReplacer("-", "_", " ", "_", ",", "", "'", "").Replace(string(typ) + "_" + row.name)
				if len(name) > 60 {
					name = name[:60]
				}
				b := binding("1=1")
				b.SourceServerUUID = row.stored
				in := JobInput{Name: name, Type: typ, RootKey: root, Binding: b}
				j, err := s.Attach(ctx, in)
				if err != nil {
					t.Fatal(err)
				}
				// without pending work: rewritten to the current value, whatever the stored one was
				in.Binding.SourceServerUUID = row.current
				if _, err := s.Attach(ctx, in); err != nil {
					t.Fatalf("Attach() without pending work = %v, want it rewritten", err)
				}
				if got := storedValue(name); got != row.current {
					t.Errorf("without pending work the stored value is %q, want %q", got, row.current)
				}

				// with pending work: the row is put back as it was, and the table decides
				exec(t, db, fmt.Sprintf("UPDATE archiver_job SET source_server_uuid = '%s' WHERE id = %d", row.stored, j.ID))
				if err := inTx(t, db, true, func(tx *sql.Tx) error { return j.Insert(ctx, tx, pending[typ], []key.Key{k}) }); err != nil {
					t.Fatal(err)
				}
				in.Binding.SourceServerUUID = row.current
				_, err = s.Attach(ctx, in)
				var be *BindingError
				switch {
				case row.refusedPending && (!errors.As(err, &be) || be.Pending != 1 || len(be.Changed) == 0 || be.Changed[0] != PartSource):
					t.Fatalf("Attach() with pending work = %v, want a BindingError about the source with 1 pending root", err)
				case !row.refusedPending && err != nil:
					t.Fatalf("Attach() with pending work = %v, want it to pass", err)
				}
				if row.refusedPending {
					if got := storedValue(name); got != row.stored {
						t.Errorf("a refused run rewrote the stored value to %q, want it kept as %q", got, row.stored)
					}
				}
			})
		}
	}
}
