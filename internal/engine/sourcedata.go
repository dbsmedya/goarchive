package engine

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/dbsmedya/dbsgomysql/pkg/replication"
)

// sourceFacts are the library facts that decide whose unfinished work a run must respect
// (unfinished-work guard spec §3a): the source's Group Replication state and its GTID mode
// and executed set.
type sourceFacts struct {
	group replication.GroupReplication
	gtid  replication.GTIDStatus
}

// readSourceData reads sourceFacts from the source. It is a variable so a test can make the
// read fail (spec §11, I12): a unit test may not script the library's SQL (make
// consumer-policy).
var readSourceData = func(ctx context.Context, db *sql.DB) (sourceFacts, error) {
	insp := replication.NewInspector(db)
	group, err := insp.GroupReplication(ctx)
	if err != nil {
		return sourceFacts{}, err
	}
	gtid, err := insp.GTIDStatus(ctx)
	if err != nil {
		return sourceFacts{}, err
	}
	return sourceFacts{group: group, gtid: gtid}, nil
}

// wellFormedUUID is the shape of a server_uuid (spec §3a, Fable M1). Only a uuid of this
// shape may be looked for in a GTID set by substring: a set's other tokens (tags, intervals,
// separators) cannot contain it, so a match is a whole uuid of the set.
var wellFormedUUID = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// sameData is spec §3a: whether a job recorded on source server stored holds its work in the
// data this source, with uuid self, holds. Every answer that is not a proof of "another
// server's data" is "the same data", the safe direction; why says on what grounds, for the
// refusal ("" when stored is this source).
func sameData(self string, facts sourceFacts, stored string) (same bool, why string) {
	switch {
	case strings.EqualFold(stored, self):
		return true, ""
	case facts.group.GroupName != "":
		// A group logs under its name, never under a member's uuid: the set answers by
		// accident on a member, so the database name alone decides (Fable I1).
		return true, "this source is a Group Replication member, so every server's database of this name in the registry is treated as one"
	case facts.gtid.Mode != "ON":
		return true, fmt.Sprintf("gtid_mode is %s, not ON, so every server's database of this name in the registry is treated as one", facts.gtid.Mode)
	case !wellFormedUUID.MatchString(stored):
		return true, "its recorded source_server_uuid is not a well-formed uuid"
	case strings.Contains(strings.ToLower(facts.gtid.Executed), strings.ToLower(stored)):
		return true, "whose data this server holds (its uuid is in this server's gtid_executed)"
	}
	return false, ""
}
