package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// serverIdentityProbe is GoArchive-owned SQL, not a dbsgomysql fact (see
// docs/README_TESTING.md, "Retained application-owned MySQL probes"). It
// inspects nothing in the metadata catalog — only what the session reports
// about itself: the server's server_uuid, the schema the session selected, and
// hostname/port for the refusal message.
//
// server_uuid is read by the server from auto.cnf in its data directory and
// generated only when that file is absent. Two servers cloned from one data
// directory therefore share it; that is a documented limitation with a
// documented remedy (docs/README_LIMITATIONS.md), not something this probe can
// tell apart.
const serverIdentityProbe = "SELECT @@GLOBAL.server_uuid, DATABASE(), @@GLOBAL.hostname, @@GLOBAL.port"

// ServerIdentity is what one pool reports about the database it reaches.
type ServerIdentity struct {
	ServerUUID string
	Schema     string // DATABASE(); never empty on success
	Hostname   string
	Port       int
}

func (id ServerIdentity) endpoint() string { return fmt.Sprintf("%s:%d", id.Hostname, id.Port) }

// readServerIdentity runs the probe on one pool.
//
// DATABASE() is NULL when the session selected no schema. GoArchive's DSN
// always selects one (config requires `database:`), so NULL — or an empty
// name — means a proxy or server rewrote the session. Comparing an empty
// schema would let two unrelated schemas look equal, so it is refused instead.
func readServerIdentity(ctx context.Context, db *sql.DB) (ServerIdentity, error) {
	var (
		id     ServerIdentity
		schema sql.NullString
	)
	if err := db.QueryRowContext(ctx, serverIdentityProbe).Scan(&id.ServerUUID, &schema, &id.Hostname, &id.Port); err != nil {
		return ServerIdentity{}, fmt.Errorf("identity probe failed: %w", err)
	}
	if !schema.Valid || schema.String == "" {
		return ServerIdentity{}, fmt.Errorf("session has no selected schema (DATABASE() is NULL) although the DSN names one; a proxy or server is rewriting the session")
	}
	if id.ServerUUID == "" {
		return ServerIdentity{}, fmt.Errorf("server reported an empty server_uuid")
	}
	id.Schema = schema.String
	return id, nil
}

// sameDatabase reports whether two identities name one database: byte-equal
// server_uuid and case-folded schema name.
//
// MySQL looks schema names up case-insensitively under lower_case_table_names
// 1 and 2 (its Windows and macOS defaults), where App and app are one schema.
// Folding on every server refuses App→app on a case-sensitive server too; that
// is the safe direction and is documented as a deliberate conservative refusal.
func sameDatabase(a, b ServerIdentity) bool {
	return a.ServerUUID == b.ServerUUID && strings.EqualFold(a.Schema, b.Schema)
}

// SameDatabaseError is the SRC_DEST_IDENTITY_CHECK refusal: source and
// destination are one database. It is its own type (not a preflight error —
// this package cannot import archiver) so callers can errors.As it and the
// message keeps a stable, grep-able category.
type SameDatabaseError struct {
	Source, Destination ServerIdentity
}

func (e *SameDatabaseError) Error() string {
	return fmt.Sprintf("SRC_DEST_IDENTITY_CHECK: source and destination are the same database "+
		"(server_uuid=%s; source schema=%s at %s, destination schema=%s at %s). "+
		"GoArchive refuses: it would verify the table against itself and delete the only copy. "+
		"Point the destination at a different server or schema. "+
		"If the two schema names differ only in letter case, GoArchive treats them as one schema on every server; rename one or use a different destination. "+
		"If these are two servers cloned from one data directory they share auto.cnf: on the standalone archive server stop mysqld, remove auto.cnf from the data directory and start it again to get a new server_uuid. "+
		"See docs/README_LIMITATIONS.md, \"Known Limits & Caution\"",
		e.Source.ServerUUID, e.Source.Schema, e.Source.endpoint(), e.Destination.Schema, e.Destination.endpoint())
}

// assertDistinctDatabases is the runtime guard for issue #13. Connect calls it
// after both pools are open and before anything else happens — before
// preflight, before any tracking-table write, and out of reach of every skip
// flag — so no command can archive a database into itself. A probe failure on
// either side is a refusal too (fail closed), attributed to that side and
// wrapped so cancellation and driver errors stay inspectable.
func assertDistinctDatabases(ctx context.Context, source, destination *sql.DB) error {
	src, err := readServerIdentity(ctx, source)
	if err != nil {
		return fmt.Errorf("source identity check failed: %w", err)
	}
	dst, err := readServerIdentity(ctx, destination)
	if err != nil {
		return fmt.Errorf("destination identity check failed: %w", err)
	}
	if sameDatabase(src, dst) {
		return &SameDatabaseError{Source: src, Destination: dst}
	}
	return nil
}
