package jobspec

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// Verification is how a job's copy is checked before anything is deleted (R41, R42). It
// is one value, globally or per job; the command line's --verification overrides both.
type Verification string

const (
	// VerifySHA256 compares a SHA-256 hash of every copied row on both servers: it
	// detects a changed value. It is the default.
	VerifySHA256 Verification = "sha256"
	// VerifyCount compares row counts by key. It proves which keys landed, not their
	// values, so a charset change on any column is refused under it.
	VerifyCount Verification = "count"
	// VerifyNone skips verification: a deliberate act (spec §3).
	VerifyNone Verification = "none"
)

// Valid reports whether v is one of the three values.
func (v Verification) Valid() bool {
	return v == VerifySHA256 || v == VerifyCount || v == VerifyNone
}

// UnmarshalYAML takes one scalar. The value is checked with the job rules, so every bad
// value is reported with the others. The 2.x block (method, skip_verification) is refused
// here, with a pointer to the upgrade guide.
func (v *Verification) UnmarshalYAML(n *yaml.Node) error {
	switch {
	case n.Kind == yaml.MappingNode:
		return fmt.Errorf("line %d: verification is one value, sha256, count or none; the method and "+
			"skip_verification block was replaced in 3.0, see %s", n.Line, UpgradeGuide)
	case n.Kind != yaml.ScalarNode || n.Tag == "!!null":
		return fmt.Errorf("line %d: verification needs a value: sha256, count or none", n.Line)
	}
	*v = Verification(n.Value)
	return nil
}
