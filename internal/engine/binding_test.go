package engine

import (
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/database"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
)

// TestTheBindingTakesTheSourcesDataIdentity (spec §4.2, GR C9): the registry's source value is the identity preflight
// decided, the group's name for an ONLINE member and the server's uuid otherwise, and never the bare server_uuid of the pool.
// An empty identity is refused, before any lock: a job cannot be bound to nothing.
func TestTheBindingTakesTheSourcesDataIdentity(t *testing.T) {
	src := database.ServerIdentity{ServerUUID: thisServer, Schema: "sakila"}
	dst := database.ServerIdentity{ServerUUID: otherOne, Schema: "archive"}
	spec := &jobspec.Spec{Type: jobspec.Archive}
	for _, tc := range []struct {
		name     string
		identity string
		want     string
		refused  bool
	}{
		{"an ONLINE member is bound to its group", groupID, groupID, false},
		{"a standalone server is bound to its uuid", thisServer, thisServer, false},
		{"an empty identity is refused", "", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := &preflight.Result{Group: preflight.Group{Identity: tc.identity}}
			b, err := bindingOf(spec, src, dst, res)
			if tc.refused {
				if err == nil || !strings.Contains(err.Error(), "data identity") || !strings.Contains(err.Error(), "did nothing") {
					t.Fatalf("bindingOf = %+v, %v, want a refusal naming the empty identity", b, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if b.SourceServerUUID != tc.want || b.SourceDatabase != "sakila" || b.DestinationDatabase != "archive" {
				t.Errorf("binding = %+v, want source value %q, source sakila, destination archive", b, tc.want)
			}
		})
	}
}
