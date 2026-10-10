package tracking

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// Fingerprint is archiver_job.config_fingerprint: the two parts of the config whose edit
// changes what a log row means (spec §6.2, R18, R21).
type Fingerprint struct {
	// Relations are the job's relations, depth-first in config order.
	Relations []Edge `json:"relations"`
	// Where is the job's where, trimmed and otherwise verbatim: no SQL normalization,
	// so any edit counts.
	Where string `json:"where"`
}

// Edge is one relation: the child table under its parent, with its (foreign_key column,
// references column) pairs sorted by the child column. Pairs are a set, as in spec §8.3:
// writing an omitted references out, or permuting foreign_key and references together,
// selects the same rows and is not a change.
type Edge struct {
	Parent string      `json:"parent"`
	Table  string      `json:"table"`
	Pairs  [][2]string `json:"pairs"`
}

// FingerprintOf builds the fingerprint of a loaded job. jobspec has already filled every
// omitted references in with the parent's key column.
func FingerprintOf(s *jobspec.Spec) Fingerprint {
	f := Fingerprint{Relations: []Edge{}, Where: strings.TrimSpace(s.Where)}
	for _, n := range jobspec.NewTree(s).CopyOrder() {
		if n.Parent == nil {
			continue
		}
		e := Edge{Parent: n.Parent.Table, Table: n.Table, Pairs: make([][2]string, len(n.ForeignKey))}
		for i := range n.ForeignKey {
			e.Pairs[i] = [2]string{n.ForeignKey[i], n.References[i]}
		}
		slices.SortFunc(e.Pairs, func(a, b [2]string) int { return strings.Compare(a[0], b[0]) })
		f.Relations = append(f.Relations, e)
	}
	return f
}

// Text is the stored form: JSON, which keeps any identifier and any where text apart
// without an escaping rule of its own. HTML escaping is off, so a DBA reads the where
// text as written.
func (f Fingerprint) Text() string {
	if f.Relations == nil {
		f.Relations = []Edge{} // stored as [], never null, which parseFingerprint refuses
	}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(f); err != nil {
		panic(fmt.Sprintf("tracking: marshal fingerprint: %v", err)) // only strings and slices
	}
	return strings.TrimSuffix(b.String(), "\n")
}

// parseFingerprint reads a stored fingerprint. A text it cannot read exactly fails
// closed: the binding check must never compare against a guess.
func parseFingerprint(text string) (Fingerprint, error) {
	var f Fingerprint
	dec := json.NewDecoder(bytes.NewReader([]byte(text)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return Fingerprint{}, fmt.Errorf("tracking: unreadable config_fingerprint %q: %w", text, err)
	}
	if dec.More() {
		return Fingerprint{}, fmt.Errorf("tracking: unreadable config_fingerprint %q: trailing data", text)
	}
	if f.Relations == nil {
		return Fingerprint{}, fmt.Errorf("tracking: unreadable config_fingerprint %q: no relations part", text)
	}
	return f, nil
}

func (f Fingerprint) sameRelations(g Fingerprint) bool {
	return slices.EqualFunc(f.Relations, g.Relations, func(a, b Edge) bool {
		return a.Parent == b.Parent && a.Table == b.Table && slices.Equal(a.Pairs, b.Pairs)
	})
}
