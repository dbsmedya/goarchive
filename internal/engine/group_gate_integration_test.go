//go:build integration

package engine

import (
	"context"
	"strings"
	"testing"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// orderingGate is a gate that records, in one list with the engine's telling of the source, when it was asked.
type orderingGate struct{ events *[]string }

func (g orderingGate) WaitForLag(context.Context, <-chan struct{}) error {
	*g.events = append(*g.events, "gate")
	return nil
}

// ownMemberID is the id the engine should hand the gate for this source: its own member id when it is an ONLINE member
// of a group, and "" for every other source. It reads through the library, as preflight does.
func ownMemberID(t *testing.T, e env) string {
	t.Helper()
	insp := dbsrepl.NewInspector(e.src)
	g, err := insp.GroupReplication(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if g.PluginStatus != "ACTIVE" || g.GroupName == "" {
		return ""
	}
	members, err := insp.GroupMembers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range members {
		if m.Self && m.State == "ONLINE" && m.MemberID != "" {
			return m.MemberID
		}
	}
	return ""
}

// TestMySQLTheGateIsToldTheSourceOnceBeforeItsFirstCall (spec §4.3, plan §2; GR C6): when preflight returns, the engine
// hands the gate whether the source is a member and its id, once, before the run's first gate call, recovery's
// included. The gate is built before preflight has read the source's facts, so this is how a group server is never
// judged without the source. On the plain estate the telling is "not a member"; on the group estate it is the source's
// own id. The telling is a function of the engine's Config, which the command supplies when it has built a gate.
func TestMySQLTheGateIsToldTheSourceOnceBeforeItsFirstCall(t *testing.T) {
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge} {
		t.Run(string(typ), func(t *testing.T) {
			e := newEnv(t, "tellgate"+string(typ))
			if typ == jobspec.Purge {
				execAll(t, e.src, numsDDL)
			} else {
				e.both(t, numsDDL)
			}
			seedNums(t, e.src, 6)
			var events []string
			cfg := e.config(t, job(t, typ, numsJob))
			cfg.Lag = orderingGate{&events}
			cfg.TellSource = func(id string) { events = append(events, "tell:"+id) }
			if _, err := e.run(t, cfg); err != nil {
				t.Fatalf("Run() = %v", err)
			}
			want := "tell:" + ownMemberID(t, e)
			if len(events) < 2 || events[0] != want {
				t.Fatalf("events = %v, want the telling %q first, then the gate's calls", events, want)
			}
			tells, gates := 0, 0
			for _, ev := range events {
				switch {
				case strings.HasPrefix(ev, "tell:"):
					tells++
				case ev == "gate":
					gates++
				}
			}
			if tells != 1 || gates < 1 {
				t.Errorf("events = %v, want one telling and at least one gate call", events)
			}
		})
	}

	t.Run("the engine tells whatever the gate field is: the function is what the command supplies", func(t *testing.T) {
		e := newEnv(t, "tellnogate")
		e.both(t, numsDDL)
		seedNums(t, e.src, 4)
		cfg := e.config(t, job(t, jobspec.Archive, numsJob))
		told := false
		cfg.TellSource = func(string) { told = true }
		if _, err := e.run(t, cfg); err != nil {
			t.Fatalf("Run() = %v", err)
		}
		if !told {
			t.Error("the engine did not hand the source over")
		}
	})
}
