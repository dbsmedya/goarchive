// Package lock holds GoArchive's MySQL user-level locks: the source table locks and the
// destination job lock of a run (source-table-locks spec §3, §4).
package lock

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// Every lock name is a fixed prefix and 32 hex characters (128 bits) of a SHA-256 over two
// identifiers, each folded to lower case, joined by a NUL byte. The name is 44 characters
// whatever the identifiers' lengths, so it never meets MySQL's 64-character limit (error
// 4163, probe L1). The fold keeps MySQL's own case-insensitive comparison of lock names
// (probe L2), which a hash would otherwise lose; the NUL keeps ("ab", "c") and ("a", "bc")
// apart. A collision only makes a free run look busy. MySQL computes the same name with
// CONCAT('goarchive:j:', LEFT(SHA2(CONCAT(LOWER(a), CHAR(0), LOWER(b)), 256), 32)) (probe L8).
const (
	tablePrefix = "goarchive:t:"
	jobPrefix   = "goarchive:j:"
)

// TableLockName is the name of the source lock on database.table.
func TableLockName(database, table string) string {
	return hashed(tablePrefix, database, table)
}

// JobLockName is the name of the destination lock on job in the tracking schema jobSchema.
func JobLockName(jobSchema, job string) string {
	return hashed(jobPrefix, jobSchema, job)
}

func hashed(prefix, a, b string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(a) + "\x00" + strings.ToLower(b)))
	return prefix + hex.EncodeToString(sum[:16])
}
