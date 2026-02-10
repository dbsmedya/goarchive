package types

// ToInt64 converts an interface{} to int64.
// Supports int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, and float64.
func ToInt64(v interface{}) int64 {
	switch i := v.(type) {
	case int64:
		return i
	case int:
		return int64(i)
	case int32:
		return int64(i)
	case int16:
		return int64(i)
	case int8:
		return int64(i)
	case uint:
		return int64(i)
	case uint64:
		return int64(i)
	case uint32:
		return int64(i)
	case uint16:
		return int64(i)
	case uint8:
		return int64(i)
	case float64:
		return int64(i)
	case float32:
		return int64(i)
	default:
		return 0
	}
}
