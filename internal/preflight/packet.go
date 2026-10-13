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
	// continuation is a statement after the last key (key.After): it binds every prefix of
	// that key, whatever the batch size, so neither the knobs nor the cap shrink it (Codex
	// M7c-2 review B1).
	continuation bool
}

// sized is a bound over its server's limit: at tuples key tuples it needs size bytes.
type sized struct {
	bound
	tuples int
	size   uint64
}

// checkMaxAllowedPacket (MAX_ALLOWED_PACKET_CHECK, M7c F3) refuses a run when a
// key-addressed statement could exceed a server's max_allowed_packet: it would fail in
// mid-run. The driver reads each server's value at connect (internal/database BuildDSN,
// M7c-F1), so the server's value is the only limit. It bounds the source's statements by key and by foreign_key
// (binding the parent's key values), the destination's by key, and the tracking log's
// statements on the destination: its lookups and transitions (the root key in an IN list,
// up to two statuses) and its INSERT (the root key and a status per row, in VALUES text).
// Each statement is bounded at the most keys the engine sends in it: its cap, or the
// run's batch_size when that is smaller, or for the source's by-key statements, which
// include the deletes (except under copy-only), the larger of batch_size and
// batch_delete_size (M7c-F4; engine byKeyChunk, discoveryChunk, deleteChunk; the log sees
// one batch of roots). The continuations after the last key, the root fetch and recovery's
// log pages, bind every prefix of that key at any batch size and are bounded as such
// (key.AfterBytes; Codex M7c-2 review B1). Each bound counts the prepared text and the
// bytes the driver sends (key.StatementBytes, key.InsertBytes; Fable M7c-1 review I1). The
// copy's INSERTs carry rows, not keys, and are not bounded here: the driver sends their
// large values as long data, so what can still fail them is one value larger than the
// destination's packet, or rarely the whole statement (engine PacketError).
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
		bounds = append(bounds, bound{server: c.Server, statement: c.Statement, table: c.Table, cap: c.Tuples, mem: mem, knobs: knobs,
			bytes: func(n int) uint64 { return key.StatementBytes(c.Columns, c.binds(), n, 0, other) }})
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
		bound{server: "destination", statement: "tracking log", table: f.cfg.Spec.RootTable, cap: members, mem: f.dstMem, knobs: []knob{batch},
			bytes: func(n int) uint64 { return key.StatementBytes(logNames, root.Columns, n, 2, other) }},
		bound{server: "destination", statement: "tracking log insert", table: f.cfg.Spec.RootTable, cap: rows, mem: f.dstMem, knobs: []knob{batch},
			bytes: func(n int) uint64 { return key.InsertBytes(root.Columns, n, 1, other) }})

	// The continuations (Codex M7c-2 review B1): the root fetch after each batch binds every
	// prefix of the last root key and its LIMIT (engine fetchSQL); archive and purge
	// recovery read later log pages the same way over k1…kN, with a status (tracking
	// (*Job).Roots). The fetch's text carries the job's where.
	fetchOther := 1024 + len(f.cfg.Spec.Where)
	for _, c := range root.Columns {
		fetchOther += 2 * (4*len(c.Name) + 128) // the projection and ORDER BY
	}
	logKey := key.Def{Table: "log", Columns: make([]key.Column, len(root.Columns))}
	for i, c := range root.Columns {
		c.Name = logNames[i]
		logKey.Columns[i] = c
	}
	bounds = append(bounds, bound{server: "source", statement: "root fetch", table: f.cfg.Spec.RootTable, mem: f.srcMem,
		continuation: true, bytes: func(int) uint64 { return key.AfterBytes(root, 1, fetchOther) }})
	if typ := f.cfg.Spec.Type; typ == jobspec.Archive || typ == jobspec.Purge {
		bounds = append(bounds, bound{server: "destination", statement: "tracking log page", table: f.cfg.Spec.RootTable, mem: f.dstMem,
			continuation: true, bytes: func(int) uint64 { return key.AfterBytes(logKey, 1, other) }})
	}

	for _, side := range []struct {
		name   string
		server server
	}{{"source", f.src}, {"destination", f.dst}} {
		packet, err := side.server.MaxAllowedPacket(ctx)
		if err != nil {
			return inspectionError("max_allowed_packet", err)
		}
		var over []sized
		for _, b := range bounds {
			if b.server != side.name {
				continue
			}
			n := min(b.cap, sendsAtMost(b.knobs))
			if b.continuation {
				n = 1 // the last key
			}
			if bytes := b.bytes(n); bytes > packet {
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
		shape := fmt.Sprintf("at %d key tuples", worst.tuples)
		if worst.continuation {
			shape = "after the last key, binding every prefix of it,"
		}
		return &Error{Check: "MAX_ALLOWED_PACKET_CHECK", Message: fmt.Sprintf(
			"%s max_allowed_packet is %d bytes; a %s statement on %s %s can need %d bytes. %s.",
			side.name, packet, worst.statement, worst.table, shape, worst.size,
			packetRemedy(side.name, packet, worst.size, over)),
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

// packetRemedy says how to make every statement in over fit packet: raise the server's
// packet to the worst one's size; unless a continuation is over, which only the packet
// helps, lower each processing setting above the largest key count that fits every
// statement it bounds, and, when a statement is at its cap, shrink the cap through
// range_optimizer_max_mem_size (set one at 0, which is unlimited: Fable M7c-1 review M4(b)).
func packetRemedy(server string, packet, worst uint64, over []sized) string {
	ways := []string{fmt.Sprintf("Raise max_allowed_packet on the %s to at least %d", server, worst)}
	// A continuation does not shrink with a batch size or the cap: only the packet helps.
	for _, o := range over {
		if o.continuation {
			return ways[0]
		}
	}
	fits := map[string]int{} // setting → the largest key count that fits every statement it bounds
	atCap, unlimited := false, false
	for _, o := range over {
		// The largest key count that fits: the size grows with the count.
		fit := sort.Search(o.tuples, func(n int) bool { return o.bytes(n+1) > packet })
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
	return strings.Join(ways, "; or ")
}
