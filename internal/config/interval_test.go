package config

import (
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
)

// maxIntervalSeconds is the largest whole number of seconds that converts to a duration:
// math.MaxInt64 nanoseconds is 9223372036.854775807 seconds.
var maxIntervalSeconds int64 = math.MaxInt64 / int64(time.Second)

// TestCheckIntervalDuration (TEL-1 C4b, the coordinator's call from C4a's finding): one
// conversion of a configured check_interval to a duration, used by the validator and by the
// gate, so the bound and the conversion cannot drift. A value ValidateCheckInterval accepts
// converts to a duration that has not wrapped.
func TestCheckIntervalDuration(t *testing.T) {
	for _, tc := range []struct {
		seconds int
		want    time.Duration
	}{
		{1, time.Second},
		{5, 5 * time.Second},
		{3600, time.Hour},
	} {
		if got := CheckIntervalDuration(tc.seconds); got != tc.want {
			t.Errorf("CheckIntervalDuration(%d) = %v, want %v", tc.seconds, got, tc.want)
		}
	}
	if strconv.IntSize < 64 {
		t.Skip("an int of this size cannot reach the duration bound")
	}
	if d := CheckIntervalDuration(int(maxIntervalSeconds)); d <= 0 || d != time.Duration(maxIntervalSeconds)*time.Second {
		t.Errorf("CheckIntervalDuration(%d) = %v: the largest accepted interval wrapped", maxIntervalSeconds, d)
	}
}

// TestCheckIntervalBound (TEL-1 C4b): check_interval must fit a duration once converted, in the
// file and in a setter alike, because both call ValidateCheckInterval. The largest whole number
// of seconds that fits is accepted; the next, and the largest int, are refused with a message
// that says why, and the file reports the validator's own text. Before this bound
// time.Duration(n) * time.Second wrapped for a large n (a negative or short hold interval).
func TestCheckIntervalBound(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("an int of this size cannot reach the duration bound")
	}
	limit := int(maxIntervalSeconds)
	if err := ValidateCheckInterval(limit); err != nil {
		t.Errorf("ValidateCheckInterval(%d) = %v, want the largest interval that fits accepted", limit, err)
	}
	for _, v := range []int{limit + 1, math.MaxInt} {
		err := ValidateCheckInterval(v)
		if err == nil {
			t.Errorf("ValidateCheckInterval(%d) accepted a value whose duration does not fit", v)
			continue
		}
		if !strings.Contains(err.Error(), "check_interval") || !strings.Contains(err.Error(), "too large") {
			t.Errorf("ValidateCheckInterval(%d) = %q, want a message naming check_interval and saying it is too large", v, err)
		}
		r := ReplicationConfig{Enabled: true, SecondsBehindSourceWithin: 10, CheckInterval: v, Servers: []ReplicationServerConfig{{Host: "r", Port: 3306, User: "u"}}}
		file, _ := sharedWith(r).ValidateShared().(ValidationErrors)
		if got, want := messageOn(file, "replication.check_interval"), err.Error(); got != want {
			t.Errorf("check_interval %d: the file says %q, ValidateCheckInterval says %q", v, got, want)
		}
	}
	// The existing rule is unchanged: positive, judged first.
	if err := ValidateCheckInterval(0); err == nil || !strings.Contains(err.Error(), "positive") {
		t.Errorf("ValidateCheckInterval(0) = %v, want the positive rule", err)
	}
}
