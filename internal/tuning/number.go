package tuning

import (
	"fmt"
	"math"
	"strconv"
)

// The number of a request has already met the protocol's lexical form (digits, with one point
// at most); the store judges it again, because it is the one place that turns text into a
// value, and a value from anywhere else must not get further than a strconv would let it:
// no sign, no exponent, no space, no digit but 0 to 9.

// digits reports whether s is one or more ASCII digits.
func digits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// decimalText reports whether s is digits, or digits, a point and digits.
func decimalText(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			return digits(s[:i]) && digits(s[i+1:])
		}
	}
	return digits(s)
}

// parseWhole reads the number of a whole-number setting.
func parseWhole(k Key, text string) (int, *Refusal) {
	if !digits(text) {
		return 0, refuse(ReasonInvalid, fmt.Sprintf("%s must be a whole number of plain digits, not %s", k, quoted(text)))
	}
	n, err := strconv.Atoi(text)
	if err != nil || n > math.MaxInt32 {
		return 0, refuse(ReasonInvalid, fmt.Sprintf("%s %s is too large", k, text))
	}
	return n, nil
}

// parseDecimal reads the number of a sleep.
func parseDecimal(k Key, text string) (float64, *Refusal) {
	if !decimalText(text) {
		return 0, refuse(ReasonInvalid, fmt.Sprintf("%s must be a number of plain digits, with at most one point, not %s", k, quoted(text)))
	}
	v, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return 0, refuse(ReasonInvalid, fmt.Sprintf("%s %s is too large", k, text))
	}
	return v, nil
}

// quoted shows a candidate on one line, cut to 64 bytes (spec §4.10).
func quoted(s string) string {
	if len(s) > 64 {
		s = s[:64] + "..."
	}
	return strconv.Quote(s)
}
