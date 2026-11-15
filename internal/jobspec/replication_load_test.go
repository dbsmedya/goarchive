package jobspec

import (
	"errors"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/config"
)

// The load rules of the replication block's `type: group` (spec §4.1, §8; GR C5), through the loader
// as a file meets them: a refusal comes as config.ValidationErrors, each with its own message.

const groupLoadHead = `
source: {host: s, user: u, database: src}
destination: {host: d, user: u, database: dst}
jobs:
  j:
    job_type: archive
    root_table: orders
    primary_key: id
    where: "1=1"
`

// loadReplication parses groupLoadHead with the replication block block.
func loadReplication(t *testing.T, block string) (*Document, config.ValidationErrors) {
	t.Helper()
	doc, err := Parse([]byte(groupLoadHead + block))
	if err == nil {
		return doc, nil
	}
	var ve config.ValidationErrors
	if !errors.As(err, &ve) {
		t.Fatalf("Parse() = %v, want config.ValidationErrors", err)
	}
	return nil, ve
}

// TestReplicationTypeValuesLoad: an absent type is async, async and group load, and any other value is
// refused naming both.
func TestReplicationTypeValuesLoad(t *testing.T) {
	const bounds = "  group_applier_queue_within: 1000\n  group_certifier_queue_within: 1000\n"
	for _, tc := range []struct {
		name, server, want string
	}{
		{"absent", "{host: r1, user: m}", "async"},
		{"async", "{host: r1, user: m, type: async}", "async"},
		{"group", "{host: r1, user: m, type: group}", "group"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc, ve := loadReplication(t, "replication:\n  enabled: true\n  check_interval: 5\n"+bounds+"  servers:\n    - "+tc.server+"\n")
			if ve != nil {
				t.Fatalf("Parse() refused: %v", ve)
			}
			if got := doc.Shared.Replication.Servers[0].Type; got != tc.want {
				t.Errorf("type = %q, want %q", got, tc.want)
			}
			if tc.want == "group" {
				r := doc.Shared.Replication
				if r.GroupApplierQueueWithin == nil || *r.GroupApplierQueueWithin != 1000 || r.GroupCertifierQueueWithin == nil || *r.GroupCertifierQueueWithin != 1000 {
					t.Errorf("bounds = %v, %v, want 1000 and 1000", r.GroupApplierQueueWithin, r.GroupCertifierQueueWithin)
				}
			}
		})
	}
	_, ve := loadReplication(t, "replication:\n  enabled: true\n  check_interval: 5\n  servers:\n    - {host: r1, user: m, type: galera}\n")
	if !hasError(ve, "replication.servers[0].type", `unsupported replication type "galera" for server r1:3306; supported: async, group`) {
		t.Errorf("an unknown type = %v, want a refusal naming async and group", ve)
	}
}

// TestAGroupServerWithChannelsIsRefusedAtLoad: channels select asynchronous channels and a group has none.
func TestAGroupServerWithChannelsIsRefusedAtLoad(t *testing.T) {
	_, ve := loadReplication(t, "replication:\n  enabled: true\n  check_interval: 5\n  group_applier_queue_within: 10\n  group_certifier_queue_within: 10\n"+
		"  servers:\n    - {host: r1, user: m, type: group, channels: [\"\"]}\n")
	if !hasError(ve, "replication.servers[0].channels", "is type group and takes no channels") {
		t.Errorf("a group server with channels = %v, want its own refusal", ve)
	}
}

// TestTheTwoGroupBoundsAreRequiredWithAGroupServer: each is required, with its own message, a negative one is
// refused, and without a group server both may be absent.
func TestTheTwoGroupBoundsAreRequiredWithAGroupServer(t *testing.T) {
	group := "  servers:\n    - {host: r1, user: m, type: group}\n"
	head := "replication:\n  enabled: true\n  check_interval: 5\n"
	_, ve := loadReplication(t, head+group)
	if !hasError(ve, "replication.group_applier_queue_within", "group_applier_queue_within is required when a server is type group") ||
		!hasError(ve, "replication.group_certifier_queue_within", "group_certifier_queue_within is required when a server is type group") {
		t.Errorf("a group server with no bounds = %v, want one refusal for each", ve)
	}
	_, ve = loadReplication(t, head+"  group_applier_queue_within: -1\n  group_certifier_queue_within: 5\n"+group)
	if !hasError(ve, "replication.group_applier_queue_within", "cannot be negative") {
		t.Errorf("a negative bound = %v, want it refused", ve)
	}
	if doc, ve := loadReplication(t, head+"  group_applier_queue_within: 0\n  group_certifier_queue_within: 0\n"+group); ve != nil {
		t.Errorf("zero bounds = %v, want them accepted (0 tolerates no queue)", ve)
	} else if *doc.Shared.Replication.GroupApplierQueueWithin != 0 {
		t.Errorf("a bound of 0 loaded as %d", *doc.Shared.Replication.GroupApplierQueueWithin)
	}
	if _, ve := loadReplication(t, head+"  servers:\n    - {host: r1, user: m}\n"); ve != nil {
		t.Errorf("no group server and no bounds = %v, want it accepted", ve)
	}
}
