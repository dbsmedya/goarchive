package tuning

import (
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
)

// The whole-number settings are ints in the file and in Settings. The number parser turns text
// into an int and nothing more: the bound of each setting belongs to the per-value validators and
// the ceilings, so that the file and the setter refuse exactly the same values and a getter's
// text, which is a value the run holds, can always be set back (spec §7 *Getter round trip*).

// intervalMax is the largest check_interval the file accepts: the last whole number of seconds
// that converts to a duration.
var intervalMax = int64(math.MaxInt64 / int64(time.Second))

// asInt converts at run time, so that a 32-bit build compiles; the tests skip there.
func asInt(n int64) int { return int(n) }

// TestWholeNumberGettersRoundTrip: for each whole-number setting, a value the file accepts above
// MaxInt32 round-trips: the getter's text, set back, is `unchanged` and not refused. The values
// are the first one past MaxInt32, one in between, and the largest the file accepts (the largest
// int, except check_interval's, which must fit a duration once converted).
func TestWholeNumberGettersRoundTrip(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("an int of this size has no value above MaxInt32")
	}
	past32 := int64(math.MaxInt32) + 1
	for _, tc := range []struct {
		key    Key
		values []int64
	}{
		{BatchSize, []int64{past32, 3_000_000_000, math.MaxInt64}},
		{BatchDeleteSize, []int64{past32, 3_000_000_000, math.MaxInt64}},
		{SecondsBehindSourceWithin, []int64{past32, 3_000_000_000, math.MaxInt64}},
		{CheckInterval, []int64{past32, 3_000_000_000, intervalMax}},
		{GroupApplierQueueWithin, []int64{past32, 3_000_000_000, math.MaxInt64}},
		{GroupCertifierQueueWithin, []int64{past32, 3_000_000_000, math.MaxInt64}},
	} {
		for _, v := range tc.values {
			// The file accepts the value, so a run can start at it.
			st := start.withWhole(tc.key, asInt(v))
			s := newStore(func(o *Options) { o.Start = st })
			text := s.Load().Text(tc.key)
			c, err := s.Set(tc.key, text, peer)
			if err != nil {
				t.Errorf("%s: the getter's text %q is refused: %v", tc.key, text, err)
				continue
			}
			if !c.Unchanged {
				t.Errorf("%s %s: set back = %+v, want unchanged", tc.key, text, c)
			}
		}
	}
}

// TestSettersRefuseWhatTheFileRefuses: past the old MaxInt32 cap the setter is exactly the
// file's validators: a value they refuse is refused with their text, and digits that overflow an
// int stay `invalid`, however many of them.
func TestSettersRefuseWhatTheFileRefuses(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("an int of this size has no value above MaxInt32")
	}
	over := strconv.FormatInt(intervalMax+1, 10)
	r := refusalOf(t, newStore(), CheckInterval, over)
	want := config.ValidateCheckInterval(asInt(intervalMax + 1))
	if want == nil {
		t.Fatalf("the file accepts check_interval %s", over)
	}
	if r.Reason != ReasonInvalid || r.Message != want.Error() {
		t.Errorf("check_interval %s: %s %q, want invalid with the file's %q", over, r.Reason, r.Message, want)
	}
	most := strconv.Itoa(math.MaxInt)
	if r := refusalOf(t, newStore(), CheckInterval, most); r.Reason != ReasonInvalid || r.Message != config.ValidateCheckInterval(math.MaxInt).Error() {
		t.Errorf("check_interval %s: %s %q, want invalid with the file's message", most, r.Reason, r.Message)
	}

	for _, k := range []Key{BatchSize, BatchDeleteSize, SecondsBehindSourceWithin, CheckInterval, GroupApplierQueueWithin, GroupCertifierQueueWithin} {
		for _, text := range []string{"9223372036854775808", "18446744073709551616", "99999999999999999999"} {
			if r := refusalOf(t, newStore(), k, text); r.Reason != ReasonInvalid {
				t.Errorf("%s %s: reason %s, want invalid: the digits overflow an int", k, text, r.Reason)
			}
		}
	}

	// A size above the run's starting value is still the ceiling's, however large.
	if r := refusalOf(t, newStore(), BatchSize, "3000000000"); r.Reason != ReasonCeiling {
		t.Errorf("batch_size 3000000000 over a start of %d: %s, want ceiling", start.BatchSize, r.Reason)
	}
}

// TestToleranceStepDoesNotOverflow: the largest value one step above the current is the sum
// saturated at the largest int, not a wrapped one. A configured tolerance of 5e18 and a round
// completed: the step is 5e18, the sum is above the largest int, and a raise to the largest
// int is within one step.
func TestToleranceStepDoesNotOverflow(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("an int of this size has no tolerance this large")
	}
	st := start
	st.SecondsBehindSourceWithin = asInt(5_000_000_000_000_000_000)
	s := newStore(func(o *Options) { o.Start = st })
	s.RoundCompleted(0)
	c := mustSet(t, s, SecondsBehindSourceWithin, strconv.Itoa(math.MaxInt))
	if c.Unchanged || tolerance(s) != math.MaxInt {
		t.Errorf("raise to the largest int = %+v, tolerance %d, want it accepted", c, tolerance(s))
	}
}
