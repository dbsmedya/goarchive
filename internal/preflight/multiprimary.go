package preflight

import (
	"context"
	"fmt"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"
)

// GROUP_MULTI_PRIMARY_CHECK (G10, spec §4.6). GoArchive serializes runs with advisory locks, and an advisory
// lock is local to one member, so in multi-primary mode two runs connected to different members are not
// serialized: the mistake the locks exist to catch goes through silently. A multi-primary group is refused by
// default, with one bypass, --allow-multi-primary, which is a command-line flag and has no configuration key.

// modeVariable is the variable the mode fact reads, named in a message so that an operator can look at it.
const modeVariable = "group_replication_single_primary_mode"

// The two spellings of the variable that the check knows, as the library reports them: ON is single-primary
// and OFF is multi-primary. Any other text is reported as the server sent it.
const (
	modeSingle = "ON"
	modeMulti  = "OFF"
)

const (
	multiPrimaryWhy = "GoArchive serializes runs with advisory locks, and an advisory lock is local to one member, so in multi-primary mode " +
		"two runs connected to different members are not serialized. GoArchive 3.1 supports single-primary groups."
	multiPrimaryRemedy = "Run the job against a single-primary group, or accept this with --allow-multi-primary (command line only)."
	multiPrimaryRetry  = "Run the job once the mode can be read, or accept this with --allow-multi-primary (command line only)."
)

// modeWord is how the report spells a side's mode (spec §4.7): "mode …" for a source whose own row is ONLINE,
// where it is the group's, and "configured …" for a source that is not running and for a destination, whose
// view is not read, where it is the server's own setting. A fact that is not known, or is another text, is
// "mode unknown" on either.
func modeWord(g dbsrepl.GroupReplication, running bool) string {
	prefix := "configured "
	if running {
		prefix = "mode "
	}
	switch {
	case g.SinglePrimaryModeKnown && g.SinglePrimaryMode == modeSingle:
		return prefix + "single-primary"
	case g.SinglePrimaryModeKnown && g.SinglePrimaryMode == modeMulti:
		return prefix + "multi-primary"
	}
	return "mode unknown"
}

// modeRefusal is what is wrong with a side's mode, in words that stand alone, and the way out; refused is false when
// nothing is: the mode is known and ON. The words follow the fact (spec §4.6, rev 5.3): the value is the server's
// own setting, which is the group's mode in force only for a member that is running in a group, so a source that
// is not running and a destination are said to be configured for it, and no more.
func modeRefusal(side string, g dbsrepl.GroupReplication, running bool) (what, remedy string, refused bool) {
	switch {
	case !g.SinglePrimaryModeKnown:
		return fmt.Sprintf("the %s's group mode could not be read: the server returned no value for %s", side, modeVariable), multiPrimaryRetry, true
	case g.SinglePrimaryMode == modeSingle:
		return "", "", false
	case g.SinglePrimaryMode == modeMulti && running:
		return fmt.Sprintf("the %s is a member of Group Replication group %s, and the group is in multi-primary mode (%s is %s)",
			side, g.GroupName, modeVariable, modeMulti), multiPrimaryRemedy, true
	case g.SinglePrimaryMode == modeMulti:
		return fmt.Sprintf("the %s is configured for multi-primary mode (%s is %s)", side, modeVariable, modeMulti), multiPrimaryRemedy, true
	}
	return fmt.Sprintf("the %s's group mode could not be read: %s is %q, which is neither %s nor %s",
		side, modeVariable, g.SinglePrimaryMode, modeSingle, modeMulti), multiPrimaryRetry, true
}

// checkMultiPrimary is GROUP_MULTI_PRIMARY_CHECK: the library's mode fact of each side whose plugin is ACTIVE with
// a group name, the source first, for every job type. It judges the fact and no role in the view: a count of
// primaries is the members' present roles, not the group's mode (a multi-primary group with one member ONLINE and a
// peer recovering shows one primary). Under --allow-multi-primary each refusal becomes a banner that says which case
// it covered; nothing is said when nothing was refused. It runs under every skip policy: no flag but the one skips it.
func checkMultiPrimary(_ context.Context, f *facts) error {
	g := f.group
	for _, s := range []struct {
		name    string
		fact    dbsrepl.GroupReplication
		running bool
	}{
		{"source", g.Source, onlineOwn(g.Members)},
		{"destination", g.Destination, false}, // its view is not read, so no running member is claimed
	} {
		if !configured(s.fact) {
			continue
		}
		what, remedy, refused := modeRefusal(s.name, s.fact, s.running)
		if !refused {
			continue
		}
		if f.cfg.AllowMultiPrimary {
			f.warn("WARNING: --allow-multi-primary: " + what + ". Advisory locks are local to one member, so two runs connected to " +
				"different members are not serialized; GoArchive 3.1 supports single-primary groups.")
			continue
		}
		return &Error{Check: "GROUP_MULTI_PRIMARY_CHECK", Message: what + ". " + multiPrimaryWhy + " " + remedy}
	}
	return nil
}
