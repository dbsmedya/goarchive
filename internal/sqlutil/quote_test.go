package sqlutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuoteIdentifier_Valid(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Simple table name",
			input:    "users",
			expected: "`users`",
		},
		{
			name:     "Table with underscore",
			input:    "order_items",
			expected: "`order_items`",
		},
		{
			name:     "Mixed case",
			input:    "MyTable",
			expected: "`MyTable`",
		},
		{
			name:     "Numeric characters",
			input:    "table123",
			expected: "`table123`",
		},
		{
			name:     "Empty string",
			input:    "",
			expected: "``",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := QuoteIdentifier(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestQuoteIdentifier_EscapeBackticks(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Single backtick",
			input:    "my`table",
			expected: "`my``table`",
		},
		{
			name:     "Multiple backticks",
			input:    "ta`bl`e",
			expected: "`ta``bl``e`",
		},
		{
			name:     "Backtick at start",
			input:    "`table",
			expected: "```table`",
		},
		{
			name:     "Backtick at end",
			input:    "table`",
			expected: "`table```",
		},
		{
			name:     "Only backticks",
			input:    "```",
			expected: "````````",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := QuoteIdentifier(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsValidIdentifier_Valid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "Simple name", input: "users"},
		{name: "With underscore", input: "order_items"},
		{name: "Mixed case", input: "MyTable"},
		{name: "Numeric", input: "table123"},
		{name: "Only underscores", input: "___"},
		{name: "Uppercase", input: "CUSTOMERS"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.True(t, IsValidIdentifier(tt.input))
		})
	}
}

func TestIsValidIdentifier_Invalid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "Empty string", input: ""},
		{name: "With space", input: "my table"},
		{name: "With hyphen", input: "my-table"},
		{name: "With dot", input: "db.table"},
		{name: "With backtick", input: "my`table"},
		{name: "With special chars", input: "table@123"},
		{name: "SQL injection attempt", input: "users; DROP TABLE users--"},
		{name: "With dollar sign", input: "table$name"},
		{name: "With parentheses", input: "table(1)"},
		{name: "With quotes", input: "table'name"},
		{name: "With asterisk", input: "table*"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.False(t, IsValidIdentifier(tt.input))
		})
	}
}

func TestQuoteIdentifierSafe_Valid(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Simple name",
			input:    "users",
			expected: "`users`",
		},
		{
			name:     "With underscore",
			input:    "order_items",
			expected: "`order_items`",
		},
		{
			name:     "Mixed case",
			input:    "MyTable",
			expected: "`MyTable`",
		},
		{
			name:     "Numeric",
			input:    "table123",
			expected: "`table123`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := QuoteIdentifierSafe(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestQuoteIdentifierSafe_Invalid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "Empty string", input: ""},
		{name: "With space", input: "my table"},
		{name: "With hyphen", input: "my-table"},
		{name: "With backtick", input: "my`table"},
		{name: "SQL injection", input: "users; DROP TABLE users--"},
		{name: "With special chars", input: "table@name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := QuoteIdentifierSafe(tt.input)
			assert.Error(t, err)
			assert.Empty(t, result)
			assert.IsType(t, &InvalidIdentifierError{}, err)
			assert.Contains(t, err.Error(), "invalid identifier")
			assert.Contains(t, err.Error(), tt.input)
		})
	}
}

func TestInvalidIdentifierError_Error(t *testing.T) {
	err := &InvalidIdentifierError{Name: "bad@table"}
	expected := "invalid identifier: bad@table (must contain only alphanumeric characters and underscores)"
	assert.Equal(t, expected, err.Error())
}
