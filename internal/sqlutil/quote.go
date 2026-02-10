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

// QuoteIdentifierSafe quotes a MySQL identifier after validating it.
// Returns an error if the identifier contains invalid characters.
// Use this when identifiers might come from untrusted sources.
func QuoteIdentifierSafe(name string) (string, error) {
	if !IsValidIdentifier(name) {
		return "", &InvalidIdentifierError{Name: name}
	}
	return QuoteIdentifier(name), nil
}

// InvalidIdentifierError is returned when an identifier contains invalid characters.
type InvalidIdentifierError struct {
	Name string
}

func (e *InvalidIdentifierError) Error() string {
	return "invalid identifier: " + e.Name + " (must contain only alphanumeric characters and underscores)"
}
