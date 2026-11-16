package tuning

import (
	"slices"
	"strconv"
)

// Key names a runtime setting. The names are the configuration keys (D5): one vocabulary across
// the file, dry-run, the status snapshot, the log and the socket.
type Key string

const (
	BatchSize                 Key = "batch_size"
	BatchDeleteSize           Key = "batch_delete_size"
	SleepSeconds              Key = "sleep_seconds"
	DeleteSleepSeconds        Key = "delete_sleep_seconds"
	SecondsBehindSourceWithin Key = "seconds_behind_source_within"
	CheckInterval             Key = "check_interval"
	// The two group bounds are settings only where the run's gate has a server of type group (GR C7s, spec §4.3).
	GroupApplierQueueWithin   Key = "group_applier_queue_within"
	GroupCertifierQueueWithin Key = "group_certifier_queue_within"
)

var keys = [...]Key{BatchSize, BatchDeleteSize, SleepSeconds, DeleteSleepSeconds, SecondsBehindSourceWithin, CheckInterval, GroupApplierQueueWithin, GroupCertifierQueueWithin}

// Keys returns the eight settings in the order help and status list them: the two group bounds follow
// check_interval, so the six that existed keep their places.
func Keys() []Key { return slices.Clone(keys[:]) }

// ParseKey returns the key a name spells, and false for anything else: cache_ttl, for one, is
// startup configuration and not a setting.
func ParseKey(name string) (Key, bool) {
	k := Key(name)
	return k, slices.Contains(keys[:], k)
}

// Effect says when a change of the setting takes effect (spec §4.5, D6), for the reply and the
// log. It is empty for a name that is not a setting.
func (k Key) Effect() string {
	switch k {
	case BatchSize:
		return "from the next batch or recovery page"
	case BatchDeleteSize:
		return "from the next DELETE"
	case SleepSeconds, DeleteSleepSeconds:
		return "at once"
	case SecondsBehindSourceWithin, CheckInterval, GroupApplierQueueWithin, GroupCertifierQueueWithin:
		return "from the next gate decision"
	}
	return ""
}

// gate reports whether the setting is one of the replication gate's: the tolerance, the interval and the two group
// bounds. They share one gate revision.
func (k Key) gate() bool { return k == SecondsBehindSourceWithin || k == CheckInterval || k.group() }

// group reports whether the setting is one of the two group bounds.
func (k Key) group() bool { return k == GroupApplierQueueWithin || k == GroupCertifierQueueWithin }

// stepped reports whether a raise of the setting above its configured value follows D15's step rule (the
// tolerance and the two group bounds, each with its own configured value as the step).
func (k Key) stepped() bool { return k == SecondsBehindSourceWithin || k.group() }

// size reports whether the setting is a size with a ceiling (D7).
func (k Key) size() bool { return k == BatchSize || k == BatchDeleteSize }

// whole reports whether the setting takes a whole number (the sleeps take a decimal).
func (k Key) whole() bool { return k != SleepSeconds && k != DeleteSleepSeconds }

// Settings are the eight values. The two group bounds mean something only where the run's gate has a server of type
// group; elsewhere they are zero and no setting.
type Settings struct {
	BatchSize                 int
	BatchDeleteSize           int
	SleepSeconds              float64
	DeleteSleepSeconds        float64
	SecondsBehindSourceWithin int
	CheckInterval             int
	GroupApplierQueueWithin   int
	GroupCertifierQueueWithin int
}

// Text renders a setting as the getter's body does: a whole number in digits, a sleep as the
// shortest plain decimal that reads back to the same value, never with an exponent. The text of
// a setting is itself a line a setter accepts and sets to the same value.
func (s Settings) Text(k Key) string {
	switch k {
	case BatchSize:
		return strconv.Itoa(s.BatchSize)
	case BatchDeleteSize:
		return strconv.Itoa(s.BatchDeleteSize)
	case SleepSeconds:
		return strconv.FormatFloat(s.SleepSeconds, 'f', -1, 64)
	case DeleteSleepSeconds:
		return strconv.FormatFloat(s.DeleteSleepSeconds, 'f', -1, 64)
	case SecondsBehindSourceWithin:
		return strconv.Itoa(s.SecondsBehindSourceWithin)
	case CheckInterval:
		return strconv.Itoa(s.CheckInterval)
	case GroupApplierQueueWithin:
		return strconv.Itoa(s.GroupApplierQueueWithin)
	case GroupCertifierQueueWithin:
		return strconv.Itoa(s.GroupCertifierQueueWithin)
	}
	return ""
}

// whole returns a whole-number setting's value.
func (s Settings) whole(k Key) int {
	switch k {
	case BatchSize:
		return s.BatchSize
	case BatchDeleteSize:
		return s.BatchDeleteSize
	case SecondsBehindSourceWithin:
		return s.SecondsBehindSourceWithin
	case CheckInterval:
		return s.CheckInterval
	case GroupApplierQueueWithin:
		return s.GroupApplierQueueWithin
	case GroupCertifierQueueWithin:
		return s.GroupCertifierQueueWithin
	}
	return 0
}

// withWhole returns s with one whole-number setting replaced.
func (s Settings) withWhole(k Key, v int) Settings {
	switch k {
	case BatchSize:
		s.BatchSize = v
	case BatchDeleteSize:
		s.BatchDeleteSize = v
	case SecondsBehindSourceWithin:
		s.SecondsBehindSourceWithin = v
	case CheckInterval:
		s.CheckInterval = v
	case GroupApplierQueueWithin:
		s.GroupApplierQueueWithin = v
	case GroupCertifierQueueWithin:
		s.GroupCertifierQueueWithin = v
	}
	return s
}

// withDecimal returns s with one sleep replaced.
func (s Settings) withDecimal(k Key, v float64) Settings {
	if k == DeleteSleepSeconds {
		s.DeleteSleepSeconds = v
	} else {
		s.SleepSeconds = v
	}
	return s
}

// Applicability says which settings a run uses, by facts that do not change during it (spec
// §4.4): batch_size and sleep_seconds always apply; the two delete settings unless the job is
// copy-only; the two gate settings only when the run uses the replication gate; the two group
// bounds only when that gate has a server of type group (spec §4.3).
type Applicability struct {
	Delete bool
	Gate   bool
	// Group says that the gate lists at least one server of type group. It means nothing without Gate.
	Group bool
}

// applies reports whether the run uses the setting and, when it does not, why.
func (a Applicability) applies(k Key) (bool, string) {
	switch {
	case (k == BatchDeleteSize || k == DeleteSleepSeconds) && !a.Delete:
		return false, "a copy-only job deletes nothing"
	case k.gate() && !a.Gate:
		return false, "this run does not use the replication gate"
	case k.group() && !a.Group:
		return false, "this run's gate has no server of type group"
	}
	return true, ""
}
