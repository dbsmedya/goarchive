package replication

import (
	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"
)

// evaluate applies the gate's policy to one server's snapshot and returns
// every reason the job must hold. It is pure: no I/O, no clock, no logger.
//
// Assembly order is part of the contract:
//
//  1. one CHANNEL_NOT_FOUND per configured-but-absent name, in selection order;
//  2. CheckReplicationConfigured over the filtered snapshot — configured-first
//     is load-bearing, because an empty filtered slice (an unconfigured server,
//     or a selection that removed everything) must fail, and the per-channel
//     checks below iterate nothing on it;
//  3. CheckReplicationChannelsRunning;
//  4. CheckSecondsBehindSourceWithin at the caller's tolerance, which is never
//     clamped or defaulted here.
//
// Each library batch keeps the library's own emission order.
func evaluate(snapshot []dbsrepl.ChannelStatus, selected []string, tolerance int64) []problem {
	filtered, missing := selectChannels(snapshot, selected)

	var ps []problem
	if len(missing) > 0 {
		observed := observedNames(snapshot)
		for _, name := range missing {
			ps = append(ps, newChannelNotFound(name, observed))
		}
	}

	for _, batch := range [][]dbsrepl.Finding{
		dbsrepl.CheckReplicationConfigured(filtered),
		dbsrepl.CheckReplicationChannelsRunning(filtered),
		dbsrepl.CheckSecondsBehindSourceWithin(filtered, tolerance),
	} {
		for _, f := range batch {
			ps = append(ps, fromFinding(f))
		}
	}

	return ps
}

// selectChannels splits the snapshot into the channels this server gates and
// the configured names the server never reported. An empty selection gates
// every channel; otherwise the match is exact, and "" is a name — the default
// channel — rather than an absence.
func selectChannels(
	snapshot []dbsrepl.ChannelStatus,
	selected []string,
) (filtered []dbsrepl.ChannelStatus, missing []string) {
	if len(selected) == 0 {
		return snapshot, nil
	}

	present := make(map[string]struct{}, len(snapshot))
	for i := range snapshot {
		present[snapshot[i].ChannelName] = struct{}{}
	}

	wanted := make(map[string]struct{}, len(selected))
	for _, name := range selected {
		if _, ok := present[name]; !ok {
			missing = append(missing, name)

			continue
		}
		wanted[name] = struct{}{}
	}

	// Ranged over the snapshot rather than over the selection, so the filtered
	// slice keeps the server's own order for the library checks.
	for i := range snapshot {
		if _, ok := wanted[snapshot[i].ChannelName]; ok {
			filtered = append(filtered, snapshot[i])
		}
	}

	return filtered, missing
}

// observedNames lists the channel names the server actually reported, in
// server order, so a missing-channel problem can show what was there instead.
func observedNames(snapshot []dbsrepl.ChannelStatus) []string {
	names := make([]string, 0, len(snapshot))
	for i := range snapshot {
		names = append(names, snapshot[i].ChannelName)
	}

	return names
}
