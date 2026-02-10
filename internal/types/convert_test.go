package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToInt64_IntTypes(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected int64
	}{
		{
			name:     "int64",
			input:    int64(42),
			expected: 42,
		},
		{
			name:     "int",
			input:    int(100),
			expected: 100,
		},
		{
			name:     "int32",
			input:    int32(200),
			expected: 200,
		},
		{
			name:     "int16",
			input:    int16(300),
			expected: 300,
		},
		{
			name:     "int8",
			input:    int8(127),
			expected: 127,
		},
		{
			name:     "uint",
			input:    uint(500),
			expected: 500,
		},
		{
			name:     "uint64",
			input:    uint64(1000),
			expected: 1000,
		},
		{
			name:     "uint32",
			input:    uint32(2000),
			expected: 2000,
		},
		{
			name:     "uint16",
			input:    uint16(3000),
			expected: 3000,
		},
		{
			name:     "uint8",
			input:    uint8(255),
			expected: 255,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToInt64(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestToInt64_FloatTypes(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected int64
	}{
		{
			name:     "float64 integer value",
			input:    float64(42.0),
			expected: 42,
		},
		{
			name:     "float64 with decimals truncates",
			input:    float64(42.9),
			expected: 42,
		},
		{
			name:     "float32 integer value",
			input:    float32(100.0),
			expected: 100,
		},
		{
			name:     "float32 with decimals truncates",
			input:    float32(99.7),
			expected: 99,
		},
		{
			name:     "float64 large value",
			input:    float64(999999.0),
			expected: 999999,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToInt64(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestToInt64_NegativeValues(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected int64
	}{
		{
			name:     "Negative int64",
			input:    int64(-42),
			expected: -42,
		},
		{
			name:     "Negative int",
			input:    int(-100),
			expected: -100,
		},
		{
			name:     "Negative int8",
			input:    int8(-128),
			expected: -128,
		},
		{
			name:     "Negative float64",
			input:    float64(-50.5),
			expected: -50,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToInt64(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestToInt64_ZeroValues(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected int64
	}{
		{
			name:     "Zero int64",
			input:    int64(0),
			expected: 0,
		},
		{
			name:     "Zero int",
			input:    int(0),
			expected: 0,
		},
		{
			name:     "Zero float64",
			input:    float64(0.0),
			expected: 0,
		},
		{
			name:     "Zero uint64",
			input:    uint64(0),
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToInt64(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestToInt64_UnsupportedTypes(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
	}{
		{
			name:  "nil",
			input: nil,
		},
		{
			name:  "string",
			input: "42",
		},
		{
			name:  "bool",
			input: true,
		},
		{
			name:  "slice",
			input: []int{1, 2, 3},
		},
		{
			name:  "map",
			input: map[string]int{"key": 42},
		},
		{
			name:  "struct",
			input: struct{ Value int }{Value: 42},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToInt64(tt.input)
			assert.Equal(t, int64(0), result, "Unsupported types should return 0")
		})
	}
}
