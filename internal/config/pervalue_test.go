package config

import (
	"math"
	"testing"
	"time"
)

// messageOn returns the message of the file's error on field, "" when there is none.
func messageOn(errs ValidationErrors, field string) string {
	for _, e := range errs {
		if e.Field == field {
			return e.Message
		}
	}
	return ""
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// sharedWith returns a Config whose only possible errors are the replication block's.
func sharedWith(r ReplicationConfig) *Config {
	c := DefaultConfig()
	c.Source.Host, c.Source.User, c.Source.Database = "s", "u", "sakila"
	c.Destination.Host, c.Destination.User, c.Destination.Database = "d", "u", "archive"
	c.Replication = r
	return c
}

// TestPerValueValidatorsAreTheFiles (TEL-1 C4a, spec §4.5 *Validation*, §7 *Shared validators*):
// each tunable setting has one per-value function, ValidateX(v) error, and the file's
// validators call it: for every value, the message the file reports for the setting is the
// function's, so a runtime setter that calls the function reports what the file would. Accepted
// values are accepted by both.
func TestPerValueValidatorsAreTheFiles(t *testing.T) {
	ints := []int{math.MinInt32, -5, -1, 0, 1, 2, 10, 1000, math.MaxInt32}

	for _, v := range ints {
		file := ValidateProcessingConfig("processing", ProcessingConfig{BatchSize: v, BatchDeleteSize: 1})
		if got, want := messageOn(file, "processing.batch_size"), errText(ValidateBatchSize(v)); got != want {
			t.Errorf("batch_size %d: the file says %q, ValidateBatchSize says %q", v, got, want)
		}
		file = ValidateProcessingConfig("processing", ProcessingConfig{BatchSize: 1, BatchDeleteSize: v})
		if got, want := messageOn(file, "processing.batch_delete_size"), errText(ValidateBatchDeleteSize(v)); got != want {
			t.Errorf("batch_delete_size %d: the file says %q, ValidateBatchDeleteSize says %q", v, got, want)
		}
		r := ReplicationConfig{Enabled: true, SecondsBehindSourceWithin: v, CheckInterval: 5, Servers: []ReplicationServerConfig{{Host: "r", Port: 3306, User: "u"}}}
		file, _ = sharedWith(r).ValidateShared().(ValidationErrors)
		if got, want := messageOn(file, "replication.seconds_behind_source_within"), errText(ValidateSecondsBehindSourceWithin(v)); got != want {
			t.Errorf("seconds_behind_source_within %d: the file says %q, ValidateSecondsBehindSourceWithin says %q", v, got, want)
		}
		r = ReplicationConfig{Enabled: true, SecondsBehindSourceWithin: 10, CheckInterval: v, Servers: []ReplicationServerConfig{{Host: "r", Port: 3306, User: "u"}}}
		file, _ = sharedWith(r).ValidateShared().(ValidationErrors)
		if got, want := messageOn(file, "replication.check_interval"), errText(ValidateCheckInterval(v)); got != want {
			t.Errorf("check_interval %d: the file says %q, ValidateCheckInterval says %q", v, got, want)
		}
	}

	// What each function refuses (the file's rules, unchanged by this change).
	for name, tc := range map[string]struct {
		fn     func(int) error
		refuse []int
		accept []int
	}{
		"batch_size":                   {ValidateBatchSize, []int{-1, 0}, []int{1, 1000}},
		"batch_delete_size":            {ValidateBatchDeleteSize, []int{-1, 0}, []int{1, 500}},
		"seconds_behind_source_within": {ValidateSecondsBehindSourceWithin, []int{-1}, []int{0, 1, 10}},
		"check_interval":               {ValidateCheckInterval, []int{-1, 0}, []int{1, 5}},
	} {
		for _, v := range tc.refuse {
			if err := tc.fn(v); err == nil {
				t.Errorf("%s: %d accepted", name, v)
			}
		}
		for _, v := range tc.accept {
			if err := tc.fn(v); err != nil {
				t.Errorf("%s: %d refused: %v", name, v, err)
			}
		}
	}
}

// TestGroupBoundValidatorsAreTheFiles (GR C7s, spec §4.3 *The two bounds as runtime settings*): each group
// bound has one per-value function, which validateGroupBounds calls for a value that is set, so that the file and
// a setter refuse the same values with the same words. The file keeps its own presence checks: a missing bound
// with a group server is not the function's to judge.
func TestGroupBoundValidatorsAreTheFiles(t *testing.T) {
	servers := []ReplicationServerConfig{{Host: "g", Port: 3306, User: "u", Type: "group"}}
	for _, v := range []int{math.MinInt32, -5, -1, 0, 1, 2, 10, 1000, math.MaxInt32, math.MaxInt} {
		a, c := v, 1000
		r := ReplicationConfig{Enabled: true, SecondsBehindSourceWithin: 10, CheckInterval: 5, Servers: servers, GroupApplierQueueWithin: &a, GroupCertifierQueueWithin: &c}
		file, _ := sharedWith(r).ValidateShared().(ValidationErrors)
		if got, want := messageOn(file, "replication.group_applier_queue_within"), errText(ValidateGroupApplierQueueWithin(v)); got != want {
			t.Errorf("group_applier_queue_within %d: the file says %q, ValidateGroupApplierQueueWithin says %q", v, got, want)
		}
		a, c = 1000, v
		file, _ = sharedWith(r).ValidateShared().(ValidationErrors)
		if got, want := messageOn(file, "replication.group_certifier_queue_within"), errText(ValidateGroupCertifierQueueWithin(v)); got != want {
			t.Errorf("group_certifier_queue_within %d: the file says %q, ValidateGroupCertifierQueueWithin says %q", v, got, want)
		}
	}

	for name, tc := range map[string]struct {
		fn      func(int) error
		message string
	}{
		"group_applier_queue_within":   {ValidateGroupApplierQueueWithin, "group_applier_queue_within cannot be negative"},
		"group_certifier_queue_within": {ValidateGroupCertifierQueueWithin, "group_certifier_queue_within cannot be negative"},
	} {
		if err := tc.fn(-1); err == nil || err.Error() != tc.message {
			t.Errorf("%s: -1 gives %v, want %q", name, err, tc.message)
		}
		for _, v := range []int{0, 1, 1000, math.MaxInt} {
			if err := tc.fn(v); err != nil {
				t.Errorf("%s: %d refused: %v", name, v, err)
			}
		}
	}

	// A bound the file does not set is the presence check's, whatever the function says.
	r := ReplicationConfig{Enabled: true, SecondsBehindSourceWithin: 10, CheckInterval: 5, Servers: servers}
	file, _ := sharedWith(r).ValidateShared().(ValidationErrors)
	if got := messageOn(file, "replication.group_applier_queue_within"); got != "group_applier_queue_within is required when a server is type group" {
		t.Errorf("a missing applier bound: the file says %q", got)
	}
	if got := messageOn(file, "replication.group_certifier_queue_within"); got != "group_certifier_queue_within is required when a server is type group" {
		t.Errorf("a missing certifier bound: the file says %q", got)
	}
}

// TestSleepDuration (coordinator's call at C2): one conversion of a configured sleep, used by
// the validator and by the engine, so the bound and the conversion cannot drift. It is the
// expression s * float64(time.Second), and a value ValidateSleepSeconds accepts converts to a
// duration that has not wrapped.
func TestSleepDuration(t *testing.T) {
	for _, tc := range []struct {
		s    float64
		want time.Duration
	}{
		{0, 0},
		{1, time.Second},
		{0.25, 250 * time.Millisecond},
		{1e-9, time.Nanosecond},
		{3600, time.Hour},
		{9223372036, 9223372036 * time.Second},
	} {
		if got := SleepDuration(tc.s); got != tc.want {
			t.Errorf("SleepDuration(%v) = %v, want %v", tc.s, got, tc.want)
		}
	}

	// The largest sleep the validator accepts converts in range; walking up from it the first
	// float the validator refuses is the first whose conversion would not be.
	s := sleepLimit / float64(time.Second)
	for ValidateSleepSeconds(s) != nil {
		s = math.Nextafter(s, 0)
	}
	if d := SleepDuration(s); d <= 0 {
		t.Fatalf("SleepDuration(%v) = %v: the largest accepted sleep wrapped", s, d)
	}
	for range 8 {
		s = math.Nextafter(s, math.Inf(1))
		if ValidateSleepSeconds(s) == nil {
			t.Fatalf("%v accepted above the largest accepted sleep", s)
		}
	}
}
