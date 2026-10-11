package preflight

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
)

// clientMaxAllowedPacket is the driver's own packet limit: internal/database's DSN keeps
// go-sql-driver's default, 64 MiB, so a statement above it fails in the client whatever
// the server allows.
const clientMaxAllowedPacket = 64 << 20

// knob is a processing setting that bounds how many keys a statement carries.
type knob struct {
	name  string
	value int
}

// bound is one kind of key-addressed statement as checkMaxAllowedPacket sizes it.
type bound struct {
	server, statement, table string
	cap                      int    // the statement cap
	mem                      uint64 // the server's range_optimizer_max_mem_size
	knobs                    []knob // the settings that bound it below its cap
	bytes                    func(tuples int) uint64
}

// sized is a bound over its server's limit: at tuples key tuples it needs size bytes.
type sized struct {
	bound
	tuples int
	size   uint64
}

// checkMaxAllowedPacket (MAX_ALLOWED_PACKET_CHECK, M7c F3) refuses a run when a
// key-addressed statement could exceed a server's max_allowed_packet: MySQL would reject it
// with error 1153 in mid-run. It bounds the source's statements by key and by foreign_key
// (binding the parent's key values), the destination's by key, and the tracking log's
// statements on the destination: its lookups and transitions (the root key in an IN list,
// up to two statuses) and its INSERT (the root key and a status per row, in VALUES text).
// Each statement is bounded at the most keys the engine sends in it: its cap, or the
// run's batch_size when that is smaller, or for the source's by-key statements, which
// include the deletes (except under copy-only), the larger of batch_size and
// batch_delete_size (M7c-F4; engine byKeyChunk, discoveryChunk, deleteChunk; the log sees
// one batch of roots). Each bound counts the prepared text and the bytes the driver sends
// (key.StatementBytes, key.InsertBytes; Fable M7c-1 review I1). The copy's INSERTs carry
// rows, not keys, and are not bounded here.
func checkMaxAllowedPacket(ctx context.Context, f *facts) error {
	specs, err := f.sourceSpecs(ctx)
	if err != nil {
		return inspectionError("max_allowed_packet", err)
	}
	batch := knob{"batch_size", f.cfg.Processing.BatchSize}
	deletes := knob{"batch_delete_size", f.cfg.Processing.BatchDeleteSize}
	var bounds []bound
	for _, c := range f.caps {
		other := 1024
		for _, col := range specs[c.Table].Columns {
			other += 4*len(col.Name) + 128 // the select list and its hash wrappers
		}
		knobs, mem := []knob{batch}, f.dstMem
		if c.Server == "source" {
			mem = f.srcMem
			// The source's by-key statements include the deletes, except for copy-only,
			// which never deletes (Fable M7c-2 review F3).
			if c.Statement == "by key" && f.cfg.Spec.Type != jobspec.CopyOnly {
				knobs = []knob{batch, deletes}
			}
		}
		bounds = append(bounds, bound{c.Server, c.Statement, c.Table, c.Tuples, mem, knobs,
			func(n int) uint64 { return key.StatementBytes(c.Columns, c.binds(), n, 0, other) }})
	}
	root := f.keys[f.cfg.Spec.RootTable]
	parts := max(len(root.Columns), 1)
	logNames := make([]string, len(root.Columns))
	other := 1024 // the log table, its status columns and the statement around the keys
	for i := range logNames {
		logNames[i] = fmt.Sprintf("k%d", i+1)
		other += len(logNames[i]) + 4
	}
	members := min(key.MaxTuples(root.Columns, f.dstMem), (65535-2)/parts)
	rows := min(members, 65535/(parts+1))
	bounds = append(bounds,
		bound{"destination", "tracking log", f.cfg.Spec.RootTable, members, f.dstMem, []knob{batch},
			func(n int) uint64 { return key.StatementBytes(logNames, root.Columns, n, 2, other) }},
		bound{"destination", "tracking log insert", f.cfg.Spec.RootTable, rows, f.dstMem, []knob{batch},
			func(n int) uint64 { return key.InsertBytes(root.Columns, n, 1, other) }})

	for _, side := range []struct {
		name   string
		server server
	}{{"source", f.src}, {"destination", f.dst}} {
		packet, err := side.server.MaxAllowedPacket(ctx)
		if err != nil {
			return inspectionError("max_allowed_packet", err)
		}
		limit := min(packet, clientMaxAllowedPacket)
		var over []sized
		for _, b := range bounds {
			if b.server != side.name {
				continue
			}
			n := min(b.cap, sendsAtMost(b.knobs))
			if bytes := b.bytes(n); bytes > limit {
				over = append(over, sized{b, n, bytes})
			}
		}
		if len(over) == 0 {
			continue
		}
		worst := over[0]
		for _, o := range over[1:] {
			if o.size > worst.size {
				worst = o
			}
		}
		return &Error{Check: "MAX_ALLOWED_PACKET_CHECK", Message: fmt.Sprintf(
			"%s max_allowed_packet is %d bytes; a %s statement on %s at %d key tuples can need %d bytes. %s.",
			side.name, packet, worst.statement, worst.table, worst.tuples, worst.size,
			packetRemedy(side.name, packet, limit, worst.size, over)),
			Tables: []string{worst.table}}
	}
	return nil
}

// sendsAtMost is the most keys the engine puts in one statement bounded by knobs: the
// largest of their values.
func sendsAtMost(knobs []knob) int {
	n := 0
	for _, k := range knobs {
		n = max(n, k.value)
	}
	return n
}

// packetRemedy says how to make every statement in over fit limit: raise the server's
// packet to the worst one's size (when the client's 64 MiB is not what binds); lower each
// processing setting above the largest key count that fits every statement it bounds; and,
// when a statement is at its cap, shrink the cap through range_optimizer_max_mem_size
// (set one at 0, which is unlimited: Fable M7c-1 review M4(b)).
func packetRemedy(server string, packet, limit, worst uint64, over []sized) string {
	var ways []string
	if packet < clientMaxAllowedPacket {
		ways = append(ways, fmt.Sprintf("Raise max_allowed_packet on the %s to at least %d", server, worst))
	}
	fits := map[string]int{} // setting → the largest key count that fits every statement it bounds
	atCap, unlimited := false, false
	for _, o := range over {
		// The largest key count that fits: the size grows with the count.
		fit := sort.Search(o.tuples, func(n int) bool { return o.bytes(n+1) > limit })
		for _, k := range o.knobs {
			if f, seen := fits[k.name]; !seen || fit < f {
				fits[k.name] = fit
			}
		}
		if o.tuples == o.cap {
			atCap, unlimited = true, unlimited || o.mem == 0
		}
	}
	var lower []string
	for _, o := range over {
		for _, k := range o.knobs {
			if f := fits[k.name]; f > 0 && k.value > f && !slices.Contains(lower, k.name) {
				lower = append(lower, k.name)
			}
		}
	}
	for i, name := range lower {
		lower[i] = fmt.Sprintf("%s to at most %d", name, fits[name])
	}
	if len(lower) > 0 {
		ways = append(ways, "lower "+strings.Join(lower, " and "))
	}
	if atCap {
		verb := "lower"
		if unlimited {
			verb = "set"
		}
		ways = append(ways, fmt.Sprintf("%s range_optimizer_max_mem_size on the %s to shrink the cap", verb, server))
	}
	remedy := strings.Join(ways, "; or ")
	if packet >= clientMaxAllowedPacket {
		remedy = "The statement exceeds the client's 64 MiB packet limit: " + remedy
	}
	return remedy
}
