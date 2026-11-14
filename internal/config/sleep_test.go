package config

import (
	"math"
	"strings"
	"testing"
	"time"
)

// sleepLimit is the largest duration as a float64: math.MaxInt64 nanoseconds is not
// representable, and rounds up to 2^63. A converted value below it fits a time.Duration.
var sleepLimit = math.Ldexp(1, 63)

// nanoseconds is the conversion the engine makes of a configured sleep (engine.seconds):
// s * float64(time.Second). The rule is judged on this value, not on a constant in seconds.
func nanoseconds(s float64) float64 { return s * float64(time.Second) }

// largestSleep finds the largest float64 whose converted value is still below sleepLimit: the
// largest sleep that converts in range. Floats just below 2^63/1e9 can still round up to 2^63
// when multiplied, so the boundary is found by walking, not computed.
func largestSleep(t *testing.T) float64 {
	t.Helper()
	s := sleepLimit / float64(time.Second)
	for nanoseconds(s) >= sleepLimit {
		s = math.Nextafter(s, 0)
	}
	for {
		next := math.Nextafter(s, math.Inf(1))
		if nanoseconds(next) >= sleepLimit {
			return s
		}
		s = next
	}
}

// TestSleepSecondsBounds is spec §7 *SLP-1* (D14), the file's side: a sleep must be finite,
// not negative, and fit a duration, judged on the converted value. The setter's side joins it in
// C4a through the same per-value functions. Every case runs through both sleeps and through the
// file's validator, whose message must be the per-value function's, so that the file and the
// socket cannot drift apart (spec §7 *Shared validators*).
func TestSleepSecondsBounds(t *testing.T) {
	top := largestSleep(t)
	above1 := math.Nextafter(top, math.Inf(1))
	above2 := math.Nextafter(above1, math.Inf(1))

	cases := []struct {
		name string
		v    float64
		want string // "" accepted; otherwise a substring of the refusal
	}{
		{"zero", 0, ""},
		{"negative zero", math.Copysign(0, -1), ""},
		{"one nanosecond", 1e-9, ""},
		{"a fraction", 0.25, ""},
		{"one second", 1, ""},
		{"a day", 86400, ""},
		{"thirty years", 1e9, ""},
		{"the largest whole second that fits", 9223372036, ""},
		{"the largest float that converts in range", top, ""},

		{"negative", -1, "cannot be negative"},
		{"a negative nanosecond", -1e-9, "cannot be negative"},
		{"positive infinity", math.Inf(1), "finite"},
		{"negative infinity", math.Inf(-1), "cannot be negative"},
		{"not a number", math.NaN(), "finite"},
		{"over the largest duration", 1e10, "too large"},
		{"far over", 1e300, "too large"},
		{"the largest float64", math.MaxFloat64, "too large"},
		{"the float that rounds up to the largest duration", above1, "too large"},
		{"the next float above it", above2, "too large"},
	}
	sleeps := []struct {
		key      string
		validate func(float64) error
		set      func(*ProcessingConfig, float64)
	}{
		{"sleep_seconds", ValidateSleepSeconds, func(p *ProcessingConfig, v float64) { p.SleepSeconds = v }},
		{"delete_sleep_seconds", ValidateDeleteSleepSeconds, func(p *ProcessingConfig, v float64) { p.DeleteSleepSeconds = v }},
	}
	for _, sl := range sleeps {
		for _, c := range cases {
			t.Run(sl.key+"/"+c.name, func(t *testing.T) {
				err := sl.validate(c.v)
				p := ProcessingConfig{BatchSize: 1, BatchDeleteSize: 1}
				sl.set(&p, c.v)
				var fileMsg string
				for _, e := range ValidateProcessingConfig("processing", p) {
					if e.Field == "processing."+sl.key {
						fileMsg = e.Message
					}
				}

				if c.want == "" {
					if err != nil || fileMsg != "" {
						t.Fatalf("%v refused: %v (file: %q)", c.v, err, fileMsg)
					}
					if d := time.Duration(nanoseconds(c.v)); d < 0 {
						t.Fatalf("%v was accepted but converts out of range: %v", c.v, d)
					}
					return
				}
				if err == nil {
					t.Fatalf("%v accepted, want a refusal saying %q", c.v, c.want)
				}
				if !strings.Contains(err.Error(), c.want) || !strings.Contains(err.Error(), sl.key) {
					t.Fatalf("%v: %q, want it to name %q and say %q", c.v, err, sl.key, c.want)
				}
				if fileMsg != err.Error() {
					t.Fatalf("the file says %q, the per-value function says %q", fileMsg, err)
				}
			})
		}
	}
}

// TestSleepSecondsBoundary walks the floats on both sides of the largest duration: a float is
// accepted exactly when its converted value is below 2^63, and everything accepted converts to
// a duration that has not wrapped.
func TestSleepSecondsBoundary(t *testing.T) {
	top := largestSleep(t)
	s := top
	for range 64 {
		s = math.Nextafter(s, 0)
	}
	prev := time.Duration(-1)
	for k := -64; k <= 64; k++ {
		in := nanoseconds(s) < sleepLimit
		err := ValidateSleepSeconds(s)
		if in && err != nil {
			t.Fatalf("%d floats from the top: %v converts in range but was refused: %v", k, s, err)
		}
		if !in && err == nil {
			t.Fatalf("%d floats from the top: %v rounds up to the largest duration but was accepted", k, s)
		}
		if err == nil {
			d := time.Duration(nanoseconds(s))
			if d < prev {
				t.Fatalf("%v converts to %v, below the previous float's %v: it wrapped", s, d, prev)
			}
			prev = d
		}
		s = math.Nextafter(s, math.Inf(1))
	}
}
