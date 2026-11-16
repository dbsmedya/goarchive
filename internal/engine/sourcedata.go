package engine

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/dbsmedya/dbsgomysql/pkg/replication"
)

// sourceWho is what the unfinished-work guard knows of the source it runs on, from preflight and the pool (spec §4.2, GR C9):
// the guard reads no Group Replication state of its own. identity is the source's data identity (the group's name for an
// ONLINE member, the server's uuid otherwise: the value the registry is bound to), serverUUID is the server's own uuid, and
// groupName is the group name configured on the server, "" when none: 3.0's own test, true in any member state.
type sourceWho struct {
	identity, serverUUID, groupName string
}

// readSourceData reads the source's GTID mode and executed set, under the source table locks: the one fact that changes
// between preflight and the guard. It is a variable so a test can make the read fail (spec §11, I12): a unit test may not
// script the library's SQL (make consumer-policy).
var readSourceData = func(ctx context.Context, db *sql.DB) (replication.GTIDStatus, error) {
	return replication.NewInspector(db).GTIDStatus(ctx)
}

// wellFormedUUID is the shape of a server_uuid (spec §3a, Fable M1). Only a uuid of this
// shape may be looked for in a GTID set by substring: a set's other tokens (tags, intervals,
// separators) cannot contain it, so a match is a whole uuid of the set.
var wellFormedUUID = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// sameData is spec §4.2's guard: whether a job recorded on source server stored holds its work in the data this source
// holds. Every answer that is not a proof of "another server's data" is "the same data", the safe direction; why says on
// what grounds, for the refusal ("" when stored is this source's own value). The rules, in order:
//
//  1. stored equals the identity, this server's own uuid or its configured group name: this source.
//  2. a group name is configured, in any member state: every server's database of this name counts as one, as in 3.0. A stored
//     uuid of another member need not be in any server's gtid_executed, so the set cannot answer there.
//  3. gtid_mode is not ON.
//  4. stored is not a well-formed uuid.
//  5. stored is in this server's gtid_executed. This is the gain: a server outside the group finds a job recorded on the group,
//     because the stored value is the group's name and the group's transactions carry it.
func sameData(who sourceWho, gtid replication.GTIDStatus, stored string) (same bool, why string) {
	switch {
	case strings.EqualFold(stored, who.identity), strings.EqualFold(stored, who.serverUUID),
		who.groupName != "" && strings.EqualFold(stored, who.groupName):
		return true, ""
	case who.groupName != "":
		// A group logs under its name, never under a member's uuid: the set answers by accident on a member, so the database
		// name alone decides, whether the member is ONLINE, RECOVERING, OFFLINE or has no row of its own (Fable I1).
		return true, fmt.Sprintf("this server is configured for Group Replication group %s, so every server's database of this name in the registry is treated as one", who.groupName)
	case gtid.Mode != "ON":
		return true, fmt.Sprintf("gtid_mode is %s, not ON, so every server's database of this name in the registry is treated as one", gtid.Mode)
	case !wellFormedUUID.MatchString(stored):
		return true, "its recorded source_server_uuid is not a well-formed uuid"
	case strings.Contains(strings.ToLower(gtid.Executed), strings.ToLower(stored)):
		return true, "whose data this server holds (its uuid is in this server's gtid_executed)"
	}
	return false, ""
}
