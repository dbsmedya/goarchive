package tracking

import (
	"fmt"
	"strings"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// Binding is the job binding (spec §6.2): the facts that give the job's log rows their
// meaning. The caller reads the source identity from the source pool (the server_uuid and
// DATABASE() of internal/database's identity probe) and the destination database from
// the destination pool.
type Binding struct {
	SourceServerUUID    string
	SourceDatabase      string
	DestinationDatabase string
	Fingerprint         Fingerprint
}

// Part names one compared part of a binding, as the refusal reports it.
type Part string

const (
	PartSource      Part = "source identity (server_uuid and database)"
	PartRelations   Part = "relations"
	PartWhere       Part = "where"
	PartDestination Part = "destination database"
)

// comparedParts is spec §6.2's table: which parts a job type's pending work depends on.
func comparedParts(t jobspec.Type) []Part {
	switch t {
	case jobspec.Archive:
		// Replaying copied roots deletes against the copy in the destination database, which
		// recovery checks (GH #7).
		return []Part{PartSource, PartRelations, PartDestination}
	case jobspec.Purge:
		// Replaying deleting roots deletes with no copy behind it.
		return []Part{PartSource, PartRelations, PartWhere}
	case jobspec.CopyOnly:
		// Completed roots authorize skips into one destination database (§6.6).
		return []Part{PartSource, PartRelations, PartDestination}
	default:
		panic(fmt.Sprintf("tracking: comparedParts(%q)", t))
	}
}

// pendingStatuses is spec §6.2's "a job has pending work": copied or deleting roots for
// archive and purge; completed roots for copy-only, whatever its resume setting (R27 B1).
func pendingStatuses(t jobspec.Type) []Status {
	if t == jobspec.CopyOnly {
		return []Status{StatusCompleted}
	}
	return []Status{StatusCopied, StatusDeleting}
}

// changedParts returns the compared parts of t that differ between stored and current,
// in comparedParts order.
func changedParts(t jobspec.Type, stored, current Binding) []Part {
	var out []Part
	for _, p := range comparedParts(t) {
		var same bool
		switch p {
		case PartSource:
			same = stored.SourceServerUUID == current.SourceServerUUID && stored.SourceDatabase == current.SourceDatabase
		case PartRelations:
			same = stored.Fingerprint.sameRelations(current.Fingerprint)
		case PartWhere:
			same = stored.Fingerprint.Where == current.Fingerprint.Where
		case PartDestination:
			same = stored.DestinationDatabase == current.DestinationDatabase
		}
		if !same {
			out = append(out, p)
		}
	}
	return out
}

// sameBinding reports whether nothing at all changed, compared or not, so no rebind
// write is needed.
func sameBinding(a, b Binding) bool {
	return a.SourceServerUUID == b.SourceServerUUID && a.SourceDatabase == b.SourceDatabase &&
		a.DestinationDatabase == b.DestinationDatabase && a.Fingerprint.Text() == b.Fingerprint.Text()
}

// BindingError refuses a run whose binding differs while the job has pending work (spec
// §6.4). Nothing has been replayed, copied, skipped or deleted when it is returned.
type BindingError struct {
	Job     string
	Type    jobspec.Type
	Pending int64 // roots in a pending status
	Status  []Status
	Changed []Part
}

func (e *BindingError) Error() string {
	parts := make([]string, len(e.Changed))
	for i, p := range e.Changed {
		parts[i] = string(p)
	}
	statuses := make([]string, len(e.Status))
	for i, s := range e.Status {
		statuses[i] = s.String()
	}
	return fmt.Sprintf("job %q (%s) has %d root(s) in status %s, logged under a different %s. "+
		"Running now would replay or skip them under the changed config, so nothing was done.\n"+
		"Ways out:\n"+
		"  - restore the original config or source and run once, to finish the logged work;\n"+
		"  - abandon those roots with the procedure in docs/README_JOBS_SCHEMA.md, \"Abandoning logged roots\";\n"+
		"  - after a source failover to a replica holding the same data under a new server_uuid, re-bind the job "+
		"with the UPDATE in docs/README_JOBS_SCHEMA.md, \"Re-binding a job after a failover\", once you have confirmed it is the same data",
		e.Job, e.Type, e.Pending, strings.Join(statuses, " or "), strings.Join(parts, " and "))
}
