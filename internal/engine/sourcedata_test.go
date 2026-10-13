package engine

import (
	"strings"
	"testing"

	"github.com/dbsmedya/dbsgomysql/pkg/replication"
)

// TestSameData (U3, unfinished-work guard spec §3a): a stored uuid is another server's data
// only when this source is not a Group Replication member, runs gtid_mode ON, and the uuid is
// well-formed, not this source's, and not in this source's gtid_executed. Every other answer
// is "the same data".
func TestSameData(t *testing.T) {
	const self = "7fcbfd08-c636-11f1-9db5-5ee215af872f"
	const other = "f11ed000-57a1-4c0d-9e5e-000000000001"
	on := func(executed string) sourceFacts {
		return sourceFacts{gtid: replication.GTIDStatus{Mode: "ON", Executed: executed}}
	}
	for _, tc := range []struct {
		name   string
		facts  sourceFacts
		stored string
		same   bool
	}{
		{"this source", on(self + ":1-9"), self, true},
		{"this source, other case", on(self + ":1-9"), "7FCBFD08-C636-11F1-9DB5-5EE215AF872F", true},
		{"in a two-part set", on(self + ":1-9,\n" + other + ":1"), other, true},
		{"in a tagged (8.4) set", on(self + ":1-9,\n" + other + ":mysqlsh:1-4"), other, true},
		{"in the set, other case", on(self + ":1-9,\n" + "F11ED000-57A1-4C0D-9E5E-000000000001:1"), other, true},
		{"absent: another server's data", on(self + ":1-9"), other, false},
		{"malformed: 35 characters", on(self + ":1-9"), other[:35], true},
		{"malformed: trailing space", on(self + ":1-9"), other + " ", true},
		{"malformed: quoted", on(self + ":1-9"), "'" + other + "'", true},
		{"empty", on(self + ":1-9"), "", true},
		{"gtid_mode OFF", sourceFacts{gtid: replication.GTIDStatus{Mode: "OFF"}}, other, true},
		{"gtid_mode OFF_PERMISSIVE", sourceFacts{gtid: replication.GTIDStatus{Mode: "OFF_PERMISSIVE"}}, other, true},
		{"gtid_mode ON_PERMISSIVE", sourceFacts{gtid: replication.GTIDStatus{Mode: "ON_PERMISSIVE", Executed: self + ":1-9"}}, other, true},
		{"a Group Replication member, uuid absent", sourceFacts{group: replication.GroupReplication{PluginStatus: "ACTIVE",
			GroupName: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}, gtid: replication.GTIDStatus{Mode: "ON", Executed: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-9"}}, other, true},
		{"plugin active, no group name: the ON rule", sourceFacts{group: replication.GroupReplication{PluginStatus: "ACTIVE"},
			gtid: replication.GTIDStatus{Mode: "ON", Executed: self + ":1-9"}}, other, false},
		{"plugin disabled: the ON rule", sourceFacts{group: replication.GroupReplication{PluginStatus: "DISABLED"},
			gtid: replication.GTIDStatus{Mode: "ON", Executed: self + ":1-9"}}, other, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, why := sameData(self, tc.facts, tc.stored)
			if got != tc.same {
				t.Errorf("sameData(%q) = %v (%q), want %v", tc.stored, got, why, tc.same)
			}
			if got && !strings.EqualFold(tc.stored, self) && why == "" {
				t.Errorf("sameData(%q) is the same data without a reason for the refusal", tc.stored)
			}
		})
	}
}
