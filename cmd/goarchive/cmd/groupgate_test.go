package cmd

import (
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/config"
)

// The interim refusal of GR C5: the replication gate does not probe a `type: group` server until C6, so a run
// with one listed is refused where the gate would be built, and no group server goes unjudged. validate and
// dry-run build no gate and are not affected. C6 removes this refusal with its test.

func fleetOf(enabled bool, types ...string) config.ReplicationConfig {
	cfg := config.ReplicationConfig{Enabled: enabled}
	for i, typ := range types {
		cfg.Servers = append(cfg.Servers, config.ReplicationServerConfig{Host: "r" + string(rune('1'+i)), Port: 3306, Type: typ})
	}
	return cfg
}

func TestAGroupServerIsRefusedWhereTheGateIsBuilt(t *testing.T) {
	err := refuseGroupServers(fleetOf(true, "async", "group", "group"))
	if err == nil {
		t.Fatal("refuseGroupServers = nil, want a refusal")
	}
	const want = "replication.servers[1] r2:3306 is listed with type: group, which the replication gate does not probe yet (not built yet): " +
		"a run with a group server listed is refused, so that no group server goes unjudged. Remove it from replication.servers to run; " +
		"validate and dry-run are not affected"
	if err.Error() != want {
		t.Errorf("message:\n got %s\nwant %s", err, want)
	}
	if strings.Contains(err.Error(), "r3") {
		t.Errorf("the refusal names a second server: the first is enough: %v", err)
	}

	for name, cfg := range map[string]config.ReplicationConfig{
		"asynchronous servers only":          fleetOf(true, "async", "async"),
		"no server":                          fleetOf(true),
		"a group server in a disabled block": fleetOf(false, "group"),
	} {
		if err := refuseGroupServers(cfg); err != nil {
			t.Errorf("%s: refuseGroupServers = %v, want none: no gate is built or no group server is in it", name, err)
		}
	}
}
