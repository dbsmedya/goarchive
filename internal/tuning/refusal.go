package tuning

// Reason is the word a refusal carries on the wire (spec §4.3). The store gives the ones it
// can judge; the server judges the others (denied, malformed, unknown) before it asks the store.
type Reason string

const (
	// ReasonInapplicable: the key exists and this run does not use it.
	ReasonInapplicable Reason = "inapplicable"
	// ReasonEnding: the run is ending; nothing is changed any more.
	ReasonEnding Reason = "ending"
	// ReasonInvalid: the number is not a value this setting can take.
	ReasonInvalid Reason = "invalid"
	// ReasonCeiling: a size above the run's starting value.
	ReasonCeiling Reason = "ceiling"
	// ReasonStep: a tolerance raise that breaks the step rule.
	ReasonStep Reason = "step"
	// ReasonBusy: the audit queue has no place; nothing was changed.
	ReasonBusy Reason = "busy"
)

// Refusal is a request the store did not accept. Nothing was changed.
type Refusal struct {
	Reason  Reason
	Message string
}

func (r *Refusal) Error() string { return r.Message }

func refuse(reason Reason, message string) *Refusal {
	return &Refusal{Reason: reason, Message: message}
}

var (
	errEnding = refuse(ReasonEnding, "the run is ending; nothing was changed")
	errBusy   = refuse(ReasonBusy, "the log is not keeping up; nothing was changed")
)
