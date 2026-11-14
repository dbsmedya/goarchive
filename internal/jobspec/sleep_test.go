package jobspec

import (
	"math"
	"strconv"
	"testing"
	"time"
)

// largestSleep is the largest float64 sleep that still converts to a time.Duration in range:
// its value in nanoseconds (s * float64(time.Second), as the engine converts it) is below 2^63.
func largestSleep() float64 {
	limit := math.Ldexp(1, 63)
	ns := float64(time.Second)
	s := limit / ns
	for s*ns >= limit {
		s = math.Nextafter(s, 0)
	}
	for {
		next := math.Nextafter(s, math.Inf(1))
		if next*ns >= limit {
			return s
		}
		s = next
	}
}

func yamlFloat(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }

// TestSleepSecondsBoundsThroughLoad is spec §7 *SLP-1* as the file meets it: `.inf`, `.nan`, a
// negative and an over-large sleep are refused, in the global processing block and in a job's
// override, and the largest float that converts in range is accepted while the float that rounds
// up to the largest duration and the next one are refused.
func TestSleepSecondsBoundsThroughLoad(t *testing.T) {
	top := largestSleep()
	above1 := math.Nextafter(top, math.Inf(1))
	above2 := math.Nextafter(above1, math.Inf(1))
	job := func(processing string) string {
		return "\njobs:\n  j:\n    job_type: archive\n    root_table: t\n    primary_key: id\n    where: \"1=1\"\n" + processing
	}

	refused := []struct{ name, value, msg string }{
		{"positive infinity", ".inf", "finite"},
		{"negative infinity", "-.inf", "cannot be negative"},
		{"not a number", ".nan", "finite"},
		{"negative", "-1", "cannot be negative"},
		{"over the largest duration", "1e10", "too large"},
		{"the float that rounds up to the largest duration", yamlFloat(above1), "too large"},
		{"the next float above it", yamlFloat(above2), "too large"},
	}
	accepted := []struct{ name, value string }{
		{"zero", "0"},
		{"a fraction", "0.25"},
		{"the largest whole second that fits", "9223372036"},
		{"the largest float that converts in range", yamlFloat(top)},
	}
	for _, key := range []string{"sleep_seconds", "delete_sleep_seconds"} {
		for _, tc := range refused {
			t.Run("global/"+key+"/"+tc.name, func(t *testing.T) {
				errs := mustFail(t, "\nprocessing:\n  "+key+": "+tc.value+"\n"+job(""))
				if !hasError(errs, "processing."+key, tc.msg) {
					t.Fatalf("want processing.%s: %q, got:\n%v", key, tc.msg, errs)
				}
			})
			t.Run("job/"+key+"/"+tc.name, func(t *testing.T) {
				errs := mustFail(t, job("    processing:\n      "+key+": "+tc.value+"\n"))
				if !hasError(errs, "jobs.j.processing."+key, tc.msg) {
					t.Fatalf("want jobs.j.processing.%s: %q, got:\n%v", key, tc.msg, errs)
				}
			})
		}
		for _, tc := range accepted {
			t.Run("global/"+key+"/"+tc.name, func(t *testing.T) {
				if _, err := parse(t, "\nprocessing:\n  "+key+": "+tc.value+"\n"+job("")); err != nil {
					t.Fatalf("%s: %s refused: %v", key, tc.value, err)
				}
			})
			t.Run("job/"+key+"/"+tc.name, func(t *testing.T) {
				d, err := parse(t, job("    processing:\n      "+key+": "+tc.value+"\n"))
				if err != nil {
					t.Fatalf("%s: %s refused: %v", key, tc.value, err)
				}
				p := d.Jobs["j"].EffectiveProcessing(d.Shared.Processing)
				got := p.SleepSeconds
				if key == "delete_sleep_seconds" {
					got = p.DeleteSleepSeconds
				}
				want, _ := strconv.ParseFloat(tc.value, 64)
				if got != want {
					t.Fatalf("%s = %v, want %v", key, got, want)
				}
			})
		}
	}

	// A global refusal stands when a job overrides something else: the file is refused, and the
	// error names the global block.
	errs := mustFail(t, "\nprocessing:\n  sleep_seconds: .inf\n"+job("    processing:\n      batch_size: 10\n"))
	if !hasError(errs, "processing.sleep_seconds", "finite") {
		t.Fatalf("a global .inf must be refused, got:\n%v", errs)
	}
}
