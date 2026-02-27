package types

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ToInt64 converts an interface{} to int64.
// Supports int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float types, []byte, and string.
func ToInt64(v interface{}) (int64, error) {
	switch i := v.(type) {
	case int64:
		return i, nil
	case int:
		return int64(i), nil
	case int32:
		return int64(i), nil
	case int16:
		return int64(i), nil
	case int8:
		return int64(i), nil
	case uint:
		if uint64(i) > math.MaxInt64 {
			return 0, fmt.Errorf("uint value overflows int64: %d", i)
		}
		return int64(i), nil
	case uint64:
		if i > math.MaxInt64 {
			return 0, fmt.Errorf("uint64 value overflows int64: %d", i)
		}
		return int64(i), nil
	case uint32:
		return int64(i), nil
	case uint16:
		return int64(i), nil
	case uint8:
		return int64(i), nil
	case float64:
		return int64(i), nil
	case float32:
		return int64(i), nil
	case []byte:
		return parseInt64String(string(i))
	case string:
		return parseInt64String(i)
	case nil:
		return 0, fmt.Errorf("cannot convert nil to int64")
	default:
		return 0, fmt.Errorf("unsupported type for int64 conversion: %T", v)
	}
}

func parseInt64String(s string) (int64, error) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return 0, fmt.Errorf("cannot convert empty string to int64")
	}

	if parsed, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		return parsed, nil
	}

	parsed, err := strconv.ParseUint(trimmed, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse %q as int64: %w", trimmed, err)
	}
	if parsed > math.MaxInt64 {
		return 0, fmt.Errorf("string uint value overflows int64: %s", trimmed)
	}
	return int64(parsed), nil
}
