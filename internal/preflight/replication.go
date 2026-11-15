package preflight

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"
)

// The replication block's `type: group` servers and the banner of a member source that none watches (spec §4.1;
// GR C5). Both are stages for an archive and a purge with replication enabled: copy-only deletes nothing, so the
// gate is never asked and a listed server can neither hold it nor be missing from it.

// groupTypeAdvice ends the refusal of REPLICATION_TYPE_CHECK.
const groupTypeAdvice = ". A server listed with type: group must be configured for the source's group; " +
	"an asynchronous replica is listed without a type, or with type: async."

// noGroupServerBanner is the banner of a member source that no `type: group` server watches. Gating only an
// asynchronous replica is a legitimate choice, so it is a banner and not a refusal.
const noGroupServerBanner = "WARNING: replication: the source is a Group Replication member and no server of type group is " +
	"monitored: the group's backlog does not hold this job"

// poolGroupRead reads the GroupReplication fact of server i of the replication block through its pool, which the
// connection phase opened (database.Manager.Replicas, index-paired with the block's servers). The fact needs no
// privilege.
func poolGroupRead(pools []*sql.DB) func(context.Context, int) (dbsrepl.GroupReplication, error) {
	return func(ctx context.Context, i int) (dbsrepl.GroupReplication, error) {
		if i >= len(pools) || pools[i] == nil {
			return dbsrepl.GroupReplication{}, errors.New("no connection was opened to this server")
		}
		return dbsrepl.NewInspector(pools[i]).GroupReplication(ctx)
	}
}

// warn gives a warning the visibility of a run banner when the caller supplied one (stderr and the log, past the
// level), and the log's otherwise.
func (f *facts) warn(msg string) {
	switch {
	case f.cfg.Banner != nil:
		f.cfg.Banner(msg)
	case f.cfg.Logger != nil:
		f.cfg.Logger.Warn(msg)
	}
}

// checkReplicationType is REPLICATION_TYPE_CHECK (spec §4.1): every server listed with `type: group` must have
// the Group Replication plugin ACTIVE with a group name, and, when the source is configured for a group, the
// same group. The refusal names each wrong server, the type written and what was found. A server whose read
// fails is not judged: one warning says so, and the gate decides about it. (A server that is down at the start
// never gets here: the connection phase fails first, for every monitored server.)
func checkReplicationType(ctx context.Context, f *facts) error {
	var problems []string
	for i, s := range f.cfg.Replication.Servers {
		if s.Type != "group" {
			continue
		}
		who := fmt.Sprintf("replication.servers[%d] %s is listed with type: group", i, s.Addr())
		gr, err := f.cfg.groupRead(ctx, i)
		if err != nil {
			f.warn(fmt.Sprintf("WARNING: REPLICATION_TYPE_CHECK: %s, and its Group Replication state could not be read (%v): "+
				"it is not judged here, and the replication gate decides", who, err))
			continue
		}
		switch {
		case gr.PluginStatus == "":
			problems = append(problems, who+", but the Group Replication plugin is not installed on it")
		case gr.PluginStatus != "ACTIVE":
			problems = append(problems, fmt.Sprintf("%s, but the Group Replication plugin is %s on it", who, gr.PluginStatus))
		case gr.GroupName == "":
			problems = append(problems, who+", but the Group Replication plugin is ACTIVE on it and no group name is set")
		case configured(f.group.Source) && gr.GroupName != f.group.Source.GroupName:
			problems = append(problems, fmt.Sprintf("%s, but it is configured for group %s and the source for group %s",
				who, gr.GroupName, f.group.Source.GroupName))
		}
	}
	if len(problems) > 0 {
		return &Error{Check: "REPLICATION_TYPE_CHECK", Message: strings.Join(problems, "; ") + groupTypeAdvice}
	}
	return nil
}

// warnUnmonitoredGroup gives the banner for an ONLINE member source whose replication block lists no
// `type: group` server (spec §4.1). It is information, so no flag skips it.
func warnUnmonitoredGroup(_ context.Context, f *facts) error {
	if !onlineOwn(f.group.Members) {
		return nil
	}
	for _, s := range f.cfg.Replication.Servers {
		if s.Type == "group" {
			return nil
		}
	}
	f.warn(noGroupServerBanner)
	return nil
}
