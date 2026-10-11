package preflight

import (
	"context"
	"fmt"

	"github.com/dbsmedya/goarchive/v3/internal/key"
)

// clientMaxAllowedPacket is the driver's own packet limit: internal/database's DSN keeps
// go-sql-driver's default, 64 MiB, so a statement above it fails in the client whatever
// the server allows.
const clientMaxAllowedPacket = 64 << 20

// checkMaxAllowedPacket (MAX_ALLOWED_PACKET_CHECK, M7c F3) refuses a run when a
// key-addressed statement at its statement cap could exceed a server's max_allowed_packet:
// MySQL would reject it with error 1153 in mid-run. It bounds the source's statements by
// key and by foreign_key (binding the parent's key values), the destination's by key, and
// the tracking log's statements on the destination: its lookups and transitions (the root
// key in an IN list, up to two statuses) and its INSERT (the root key and a status per
// row, in VALUES text), at the caps
// internal/tracking derives from the root key ((*Store).newJob). Each bound counts the
// prepared text and the bytes the driver sends (key.StatementBytes, key.InsertBytes; Fable
// M7c-1 review I1). The copy's INSERTs carry rows, not keys, and are not bounded here.
func checkMaxAllowedPacket(ctx context.Context, f *facts) error {
	specs, err := f.sourceSpecs(ctx)
	if err != nil {
		return inspectionError("max_allowed_packet", err)
	}
	type bound struct {
		server, statement, table string
		tuples                   int
		bytes                    uint64
	}
	var bounds []bound
	for _, c := range f.caps {
		other := 1024
		for _, col := range specs[c.Table].Columns {
			other += 4*len(col.Name) + 128 // the select list and its hash wrappers
		}
		bounds = append(bounds, bound{c.Server, c.Statement, c.Table, c.Tuples,
			key.StatementBytes(c.Columns, c.binds(), c.Tuples, 0, other)})
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
		bound{"destination", "tracking log", f.cfg.Spec.RootTable, members, key.StatementBytes(logNames, root.Columns, members, 2, other)},
		bound{"destination", "tracking log insert", f.cfg.Spec.RootTable, rows, key.InsertBytes(root.Columns, rows, 1, other)})

	for _, side := range []struct {
		name   string
		server server
	}{{"source", f.src}, {"destination", f.dst}} {
		packet, err := side.server.MaxAllowedPacket(ctx)
		if err != nil {
			return inspectionError("max_allowed_packet", err)
		}
		var worst bound
		for _, b := range bounds {
			if b.server == side.name && b.bytes > worst.bytes {
				worst = b
			}
		}
		if worst.bytes <= min(packet, clientMaxAllowedPacket) {
			continue
		}
		remedy := fmt.Sprintf("Raise max_allowed_packet on the %s to at least %d", side.name, worst.bytes)
		if packet >= clientMaxAllowedPacket {
			remedy = "The statement exceeds the client's 64 MiB packet limit; lower range_optimizer_max_mem_size to shrink the cap"
		}
		return &Error{Check: "MAX_ALLOWED_PACKET_CHECK", Message: fmt.Sprintf(
			"%s max_allowed_packet is %d bytes; a %s statement on %s at its cap of %d key tuples can need %d bytes. %s.",
			side.name, packet, worst.statement, worst.table, worst.tuples, worst.bytes, remedy), Tables: []string{worst.table}}
	}
	return nil
}
