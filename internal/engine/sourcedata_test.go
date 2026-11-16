package engine

import (
	"strings"
	"testing"

	"github.com/dbsmedya/dbsgomysql/pkg/replication"
)

const (
	thisServer = "7fcbfd08-c636-11f1-9db5-5ee215af872f"
	otherOne   = "f11ed000-57a1-4c0d-9e5e-000000000001"
	groupID    = "8a94f357-aab4-11df-86ab-c80aa9429563"
)

func gtidOn(executed string) replication.GTIDStatus {
	return replication.GTIDStatus{Mode: "ON", Executed: executed}
}

// TestSameData is the unfinished-work guard of spec §4.2 (GR C9), rule by rule. The source's identity is the group name when
// it is an ONLINE member and its uuid otherwise; the configured group name is 3.0's own test, in any member state. A stored
// value is another server's data only when none of the five rules holds: the answer is "the same data" unless it is proved
// otherwise.
func TestSameData(t *testing.T) {
	member := sourceWho{identity: groupID, serverUUID: thisServer, groupName: groupID}
	// a member that is not ONLINE: the identity is its uuid and its group name is configured all the same
	notOnline := sourceWho{identity: thisServer, serverUUID: thisServer, groupName: groupID}
	standalone := sourceWho{identity: thisServer, serverUUID: thisServer}
	for _, tc := range []struct {
		name   string
		who    sourceWho
		gtid   replication.GTIDStatus
		stored string
		same   bool
		why    bool // a reason is given: the stored value is not this source's own
	}{
		// rule 1: the identity, this server's own uuid, or its configured group name
		{"the identity, an ONLINE member's group name", member, gtidOn(thisServer + ":1-9"), groupID, true, false},
		{"this server's own uuid, on an ONLINE member", member, gtidOn(thisServer + ":1-9"), thisServer, true, false},
		{"this server's own uuid, other case", standalone, gtidOn(thisServer + ":1-9"), "7FCBFD08-C636-11F1-9DB5-5EE215AF872F", true, false},
		{"the configured group name on a member that is not ONLINE", notOnline, gtidOn(thisServer + ":1-9"), groupID, true, false},
		{"a standalone server's own uuid", standalone, gtidOn(thisServer + ":1-9"), thisServer, true, false},
		// rule 2: a group name is configured, in any member state: every server's database of this name counts as one
		{"another member's uuid absent from gtid_executed, ONLINE", member, gtidOn(thisServer + ":1-9"), otherOne, true, true},
		{"another member's uuid absent from gtid_executed, not ONLINE", notOnline, gtidOn(thisServer + ":1-9"), otherOne, true, true},
		{"another member's uuid, a stopped member with no row of its own", notOnline, replication.GTIDStatus{Mode: "ON"}, otherOne, true, true},
		// rule 3: gtid_mode is not ON
		{"gtid_mode OFF", sourceWho{identity: thisServer, serverUUID: thisServer}, replication.GTIDStatus{Mode: "OFF"}, otherOne, true, true},
		{"gtid_mode OFF_PERMISSIVE", standalone, replication.GTIDStatus{Mode: "OFF_PERMISSIVE"}, otherOne, true, true},
		{"gtid_mode ON_PERMISSIVE", standalone, replication.GTIDStatus{Mode: "ON_PERMISSIVE", Executed: thisServer + ":1-9"}, otherOne, true, true},
		// rule 4: the stored value is not a well-formed uuid
		{"malformed: 35 characters", standalone, gtidOn(thisServer + ":1-9"), otherOne[:35], true, true},
		{"malformed: trailing space", standalone, gtidOn(thisServer + ":1-9"), otherOne + " ", true, true},
		{"malformed: quoted", standalone, gtidOn(thisServer + ":1-9"), "'" + otherOne + "'", true, true},
		{"empty", standalone, gtidOn(thisServer + ":1-9"), "", true, true},
		// rule 5: the stored value is in this server's gtid_executed
		{"in a two-part set", standalone, gtidOn(thisServer + ":1-9,\n" + otherOne + ":1"), otherOne, true, true},
		{"in a tagged (8.4) set", standalone, gtidOn(thisServer + ":1-9,\n" + otherOne + ":mysqlsh:1-4"), otherOne, true, true},
		{"in the set, other case", standalone, gtidOn(thisServer + ":1-9,\n" + "F11ED000-57A1-4C0D-9E5E-000000000001:1"), otherOne, true, true},
		// absent from every rule: another server's data
		{"another server's data", standalone, gtidOn(thisServer + ":1-9"), otherOne, false, false},
		{"plugin active with no group name: the ON rule", standalone, gtidOn(thisServer + ":1-9"), otherOne, false, false},
		// the gain: a server outside the group (a promoted asynchronous replica) carries the group's transactions, and the stored value
		// is the group name, so a job recorded on the group is found there
		{"outside the group, the group's name in gtid_executed", standalone, gtidOn(thisServer + ":1-9,\n" + groupID + ":1-500"), groupID, true, true},
		{"outside the group, the group's name in a tagged set", standalone, gtidOn(thisServer + ":1-9,\n" + groupID + ":mysqlsh:1-4"), groupID, true, true},
		{"outside the group, the group's name absent", standalone, gtidOn(thisServer + ":1-9"), groupID, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, why := sameData(tc.who, tc.gtid, tc.stored)
			if got != tc.same {
				t.Errorf("sameData(%q) = %v (%q), want %v", tc.stored, got, why, tc.same)
			}
			if got && tc.why && why == "" {
				t.Errorf("sameData(%q) is the same data without a reason for the refusal", tc.stored)
			}
			if got && !tc.why && why != "" {
				t.Errorf("sameData(%q) gives the reason %q for this source's own value", tc.stored, why)
			}
		})
	}
}

// TestSameDataSaysWhyAConfiguredGroupCounts: rule 2's reason names the group and says that every server's database of that
// name is one, and does not claim that the source is a running member, which a stopped one is not.
func TestSameDataSaysWhyAConfiguredGroupCounts(t *testing.T) {
	_, why := sameData(sourceWho{identity: thisServer, serverUUID: thisServer, groupName: groupID}, gtidOn(""), otherOne)
	for _, want := range []string{"configured for Group Replication group " + groupID, "every server's database of this name"} {
		if !strings.Contains(why, want) {
			t.Errorf("the reason %q lacks %q", why, want)
		}
	}
	if strings.Contains(why, "is a Group Replication member") {
		t.Errorf("the reason %q claims membership that a configured, stopped server does not have", why)
	}
}
