//go:build integration

package engine

import (
	"context"
	"database/sql"
	"net"
	"strconv"
	"strings"
	"testing"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
	"github.com/dbsmedya/goarchive/v3/internal/testestate"
)

// G9's cause on an ordinary secondary (spec §4.5's first row, GR C8): an archive or a purge whose source is a SECONDARY is
// refused by SOURCE_READ_ONLY_CHECK, and the refusal names the group and the primary's host and port as the group's own
// view gives them. A copy-only run there is not warned: it reads what the secondary has applied, which is what reading
// one has always meant, and the server holds nothing.
func TestMySQLAReadOnlySecondaryNamesThePrimaryAndCopyOnlyIsNotWarned(t *testing.T) {
	requireGroupMemberSource(t)
	e := newEnv(t, "grcause")
	e.both(t, numsDDL)
	seedNums(t, e.src, 20)
	admin := testestate.OpenUnfenced(t, "TEST_SOURCE", "")

	// the group's own words for the primary and its name, from the library's facts
	group, err := dbsrepl.NewInspector(admin).GroupReplication(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	members, err := dbsrepl.NewInspector(admin).GroupMembers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var primary string
	for _, m := range members {
		if m.Role == "PRIMARY" {
			primary = m.Host
			if m.Port.Valid {
				primary = net.JoinHostPort(m.Host, strconv.FormatInt(m.Port.Int64, 10))
			}
		}
	}
	if primary == "" || group.GroupName == "" {
		t.Fatalf("the group's view names no primary (%q) or the plugin no group (%q)", primary, group.GroupName)
	}

	// gr3, a secondary, holds the schema before it is read
	var gtids string
	if err := admin.QueryRowContext(context.Background(), "SELECT @@GLOBAL.gtid_executed").Scan(&gtids); err != nil {
		t.Fatal(err)
	}
	gr3 := testestate.OpenUnfenced(t, "TEST_GROUP_MEMBER3", "")
	var rc sql.NullInt64
	if err := gr3.QueryRowContext(context.Background(), "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 60)", gtids).Scan(&rc); err != nil || rc.Int64 != 0 {
		t.Fatalf("gr3 did not apply the primary's transactions: %v (%v)", rc, err)
	}

	want := "The source is a SECONDARY of group " + group.GroupName + "; the primary is " + primary + "."
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge} {
		t.Run(string(typ), func(t *testing.T) {
			cfg := e.config(t, job(t, typ, numsJob))
			cfg.Source = openServer(t, "TEST_GROUP_MEMBER3", e.srcSchema, nil)
			_, err := Run(context.Background(), cfg)
			pe := wantErr[*preflight.Error](t, err)
			if pe.Check != "SOURCE_READ_ONLY_CHECK" || !strings.Contains(pe.Message, want) {
				t.Errorf("Run() = %v, want SOURCE_READ_ONLY_CHECK ending in %q", err, want)
			}
		})
	}

	t.Run("copy-only", func(t *testing.T) {
		var banners []string
		cfg := e.config(t, job(t, jobspec.CopyOnly, numsJob))
		cfg.Source = openServer(t, "TEST_GROUP_MEMBER3", e.srcSchema, nil)
		cfg.Banner = func(b string) { banners = append(banners, b) }
		e.mustRun(t, cfg)
		for _, b := range banners {
			if strings.Contains(b, "read-only") {
				t.Errorf("a copy-only run on a secondary was warned of a read-only primary: %s", b)
			}
		}
		if got := count(t, e.dst, "SELECT COUNT(*) FROM nums"); got != 20 {
			t.Errorf("destination rows = %d, want the 20 the secondary holds", got)
		}
	})
}
