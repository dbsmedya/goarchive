package tracking

import (
	"slices"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

func base() Binding {
	return Binding{SourceServerUUID: "u1", SourceDatabase: "src", DestinationDatabase: "dst",
		Fingerprint: Fingerprint{Relations: []Edge{{Parent: "orders", Table: "lines", Pairs: [][2]string{{"order_id", "id"}}}}, Where: "1=1"}}
}

// TestChangedParts is spec §6.2's table, as GH #7 changed it: every type compares source and
// relations; archive and copy-only add the destination database; purge adds where instead.
func TestChangedParts(t *testing.T) {
	edits := map[Part]func(b *Binding){
		PartSource:      func(b *Binding) { b.SourceServerUUID = "u2" },
		PartRelations:   func(b *Binding) { b.Fingerprint.Relations[0].Table = "payments" },
		PartWhere:       func(b *Binding) { b.Fingerprint.Where = "id > 1" },
		PartDestination: func(b *Binding) { b.DestinationDatabase = "dst2" },
	}
	compared := map[jobspec.Type][]Part{
		jobspec.Archive:  {PartSource, PartRelations, PartDestination},
		jobspec.Purge:    {PartSource, PartRelations, PartWhere},
		jobspec.CopyOnly: {PartSource, PartRelations, PartDestination},
	}
	for typ, parts := range compared {
		for part, edit := range edits {
			cur := base()
			edit(&cur)
			got := changedParts(typ, base(), cur)
			want := []Part(nil)
			if slices.Contains(parts, part) {
				want = []Part{part}
			}
			if !slices.Equal(got, want) {
				t.Errorf("%s, %s changed: changedParts = %v, want %v", typ, part, got, want)
			}
		}
	}
	// The source database is part of the source identity.
	cur := base()
	cur.SourceDatabase = "src2"
	if got := changedParts(jobspec.Archive, base(), cur); !slices.Equal(got, []Part{PartSource}) {
		t.Errorf("source database changed: %v", got)
	}
	// A relation with another pairing is a relations change.
	cur = base()
	cur.Fingerprint.Relations[0].Pairs[0][1] = "code"
	if got := changedParts(jobspec.Archive, base(), cur); !slices.Equal(got, []Part{PartRelations}) {
		t.Errorf("pairing changed: %v", got)
	}
	if !sameBinding(base(), base()) || sameBinding(base(), cur) {
		t.Error("sameBinding is not exact")
	}
}

// TestPendingStatuses: archive and purge are pending on copied or deleting roots;
// copy-only on completed roots, whatever its resume setting (R27 B1).
func TestPendingStatuses(t *testing.T) {
	for typ, want := range map[jobspec.Type][]Status{
		jobspec.Archive:  {StatusCopied, StatusDeleting},
		jobspec.Purge:    {StatusCopied, StatusDeleting},
		jobspec.CopyOnly: {StatusCompleted},
	} {
		if got := pendingStatuses(typ); !slices.Equal(got, want) {
			t.Errorf("pendingStatuses(%s) = %v, want %v", typ, got, want)
		}
	}
}

// TestBindingErrorMessage: the refusal names the job, the number of pending roots, the
// statuses, the changed parts and the three ways out (spec §6.4).
func TestBindingErrorMessage(t *testing.T) {
	err := &BindingError{Job: "orders", Type: jobspec.Purge, Pending: 42, Status: []Status{StatusCopied, StatusDeleting},
		Changed: []Part{PartSource, PartWhere}}
	msg := err.Error()
	for _, want := range []string{
		`job "orders" (purge) has 42 root(s) in status copied or deleting`,
		"logged under a different source identity (server_uuid and database) and where",
		"restore the original config or source and run once",
		`"Abandoning logged roots"`,
		`"Re-binding a job after a failover"`,
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("message lacks %q:\n%s", want, msg)
		}
	}
}
