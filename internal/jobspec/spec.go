// Package jobspec is the GoArchive 3.0 job model (spec §5): the job type, the documented
// keys, the relationship columns, and the table tree they describe (spec §7.3).
//
// It reads a configuration file strictly: unknown keys and missing required keys are
// refused. The shared blocks (source, destination, replication, processing, safety,
// verification, logging) keep their 2.x types and validators from internal/config.
//
// The documented primary_key is relationship documentation only (R31). SQL is built from
// the key MySQL reports (internal/key); preflight (M3) checks that the two agree.
package jobspec

import (
	"fmt"
	"slices"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"gopkg.in/yaml.v3"
)

// Type is a job's job_type (spec §5, R4).
type Type string

// The three job types.
const (
	Archive  Type = "archive"
	Purge    Type = "purge"
	CopyOnly Type = "copy-only"
)

// Valid reports whether t is one of the three job types.
func (t Type) Valid() bool {
	return t == Archive || t == Purge || t == CopyOnly
}

// Columns is a list of column names. YAML may write one column as a scalar and several as
// a list (spec §5).
type Columns []string

// UnmarshalYAML accepts a scalar or a sequence of scalars. A null leaves the list empty,
// which validation reports as missing where the key is required.
func (c *Columns) UnmarshalYAML(n *yaml.Node) error {
	switch n.Kind {
	case yaml.ScalarNode:
		if n.Tag == "!!null" {
			*c = nil
			return nil
		}
		*c = Columns{n.Value}
		return nil
	case yaml.SequenceNode:
		out := make(Columns, 0, len(n.Content))
		for _, item := range n.Content {
			if item.Kind != yaml.ScalarNode || item.Tag == "!!null" {
				return fmt.Errorf("line %d: a column list holds only column names", item.Line)
			}
			out = append(out, item.Value)
		}
		*c = out
		return nil
	default:
		return fmt.Errorf("line %d: expected a column name or a list of column names", n.Line)
	}
}

// Spec is one job (spec §5).
type Spec struct {
	Type         Type                        `yaml:"job_type"`
	RootTable    string                      `yaml:"root_table"`
	PrimaryKey   Columns                     `yaml:"primary_key"`
	Where        string                      `yaml:"where"`
	Resume       *bool                       `yaml:"resume"`
	Relations    []Relation                  `yaml:"relations"`
	Processing   *config.ProcessingOverrides `yaml:"processing"`
	Verification *Verification               `yaml:"verification"`
	Logging      *config.LoggingConfig       `yaml:"logging"`
}

// Relation is one child table and the columns that tie it to its parent (spec §5, §7.3).
// ForeignKey[i] in this table pairs with References[i] in the parent. Load fills an
// omitted References with the parent's one documented key column.
type Relation struct {
	Table          string     `yaml:"table"`
	PrimaryKey     Columns    `yaml:"primary_key"`
	ForeignKey     Columns    `yaml:"foreign_key"`
	References     Columns    `yaml:"references"`
	DependencyType string     `yaml:"dependency_type"`
	Relations      []Relation `yaml:"relations"`
}

// ResumeEnabled reports the copy-only resume setting; it defaults to false (spec §6.6).
func (s *Spec) ResumeEnabled() bool {
	return s.Resume != nil && *s.Resume
}

// EffectiveProcessing merges the job's processing overrides over global.
func (s *Spec) EffectiveProcessing(global config.ProcessingConfig) config.ProcessingConfig {
	return (&config.JobConfig{Processing: s.Processing}).GetJobProcessing(global)
}

// EffectiveVerification is the job's verification, or global when the job sets none.
func (s *Spec) EffectiveVerification(global Verification) Verification {
	if s.Verification != nil {
		return *s.Verification
	}
	return global
}

// EffectiveLogging merges the job's logging overrides over global.
func (s *Spec) EffectiveLogging(global config.LoggingConfig) config.LoggingConfig {
	return (&config.JobConfig{Logging: s.Logging}).GetJobLogging(global)
}

// Document is a loaded, validated configuration file.
type Document struct {
	// Shared holds every block except jobs and verification; Shared.Jobs is always empty
	// and Shared.Verification is unused. Never call its job-aware methods (GetJob,
	// GetJobProcessing, GetJobVerification, GetJobLogging, or ApplyOverrides): with no jobs
	// they silently fall back to the global blocks. Use the Spec's Effective* methods; M6
	// applies --verification over Spec.EffectiveVerification.
	Shared *config.Config
	// Verification is the file's global verification, sha256 unless set (R41, R42).
	Verification Verification
	Jobs         map[string]*Spec
}

// Job returns the named job. A non-empty cmd is the subcommand being run; a job whose
// job_type differs is refused before any connection is opened (spec §5, R4). Commands that
// accept every type (validate, plan, dry-run, list-jobs) pass "".
func (d *Document) Job(name string, cmd Type) (*Spec, error) {
	s, ok := d.Jobs[name]
	if !ok {
		return nil, fmt.Errorf("job %q not found in configuration", name)
	}
	if cmd != "" && s.Type != cmd {
		return nil, fmt.Errorf("job %q has job_type %s; run it with \"goarchive %s\", not \"goarchive %s\"",
			name, s.Type, s.Type, cmd)
	}
	return s, nil
}

// JobNames returns the job names in sorted order.
func (d *Document) JobNames() []string {
	names := make([]string, 0, len(d.Jobs))
	for name := range d.Jobs {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}
