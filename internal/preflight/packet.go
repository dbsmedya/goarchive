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
// key and by foreign_key, the destination's by key, and the tracking log's statements on
// the destination, whose cap internal/tracking derives from the root key
// ((*Store).Attach). INSERTs carry rows, not keys, and are not bounded here.
func checkMaxAllowedPacket(ctx context.Context, f *facts) error {
	specs, err := f.sourceSpecs(ctx)
	if err != nil {
		return inspectionError("max_allowed_packet", err)
	}
	root := f.keys[f.cfg.Spec.RootTable]
	logNames := make([]string, len(root.Columns))
	for i := range logNames {
		logNames[i] = fmt.Sprintf("k%d", i+1)
	}
	logCap := Cap{Table: f.cfg.Spec.RootTable, Server: "destination", Statement: "tracking log", Columns: logNames,
		Tuples: min(key.MaxTuples(root.Columns, f.dstMem), (65535-2)/max(len(root.Columns), 1)), parts: root.Columns}
	for _, side := range []struct {
		name   string
		server server
	}{{"source", f.src}, {"destination", f.dst}} {
		packet, err := side.server.MaxAllowedPacket(ctx)
		if err != nil {
			return inspectionError("max_allowed_packet", err)
		}
		var worst Cap
		var need uint64
		for _, c := range append(f.caps[:len(f.caps):len(f.caps)], logCap) {
			if c.Server != side.name {
				continue
			}
			other := 1024
			for _, col := range specs[c.Table].Columns {
				other += 4*len(col.Name) + 128 // the select list and its hash wrappers
			}
			if n := key.StatementBytes(c.Columns, c.parts, c.Tuples, other); n > need {
				worst, need = c, n
			}
		}
		limit := min(packet, clientMaxAllowedPacket)
		if need <= limit {
			continue
		}
		remedy := fmt.Sprintf("Raise max_allowed_packet on the %s to at least %d", side.name, need)
		if packet >= clientMaxAllowedPacket {
			remedy = "The statement exceeds the client's 64 MiB packet limit; lower range_optimizer_max_mem_size to shrink the cap"
		}
		return &Error{Check: "MAX_ALLOWED_PACKET_CHECK", Message: fmt.Sprintf(
			"%s max_allowed_packet is %d bytes; a %s statement on %s at its cap of %d key tuples can need %d bytes. %s.",
			side.name, packet, worst.Statement, worst.Table, worst.Tuples, need, remedy), Tables: []string{worst.Table}}
	}
	return nil
}
