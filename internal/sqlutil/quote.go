// Package sqlutil provides SQL utility functions for GoArchive.
package sqlutil

import (
	"regexp"
	"strings"
)

// QuoteIdentifier quotes a MySQL identifier (table name, column name) with backticks.
// It escapes any existing backticks by doubling them.
// Example: "my_table" -> "`my_table`"
// Example: "my`table" -> "`my“table`"
func QuoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// validIdentifierRegex matches valid MySQL identifier characters.
// MySQL identifiers can contain: alphanumeric, underscore, $ (though $ is non-standard)
// For safety, we restrict to alphanumeric and underscore only.
var validIdentifierRegex = regexp.MustCompile("^[a-zA-Z0-9_]+$")

// IsValidIdentifier checks if a name is a valid MySQL identifier.
// It validates that the name only contains alphanumeric characters and underscores.
// This is a defense-in-depth measure against SQL injection.
func IsValidIdentifier(name string) bool {
	return validIdentifierRegex.MatchString(name)
}
