package jobspec

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"gopkg.in/yaml.v3"
)

// UpgradeGuide is the document a 2.x configuration is pointed to (spec §5).
const UpgradeGuide = "docs/README_UPGRADING_3_0.md"

// document is the file's shape. Every key the file may hold is a field here; unknownKeys
// refuses the rest. The 2.0 replica block and safety's lag keys are absent on purpose:
// 3.0 refuses them as unknown keys.
type document struct {
	Source       config.DatabaseConfig     `yaml:"source"`
	Destination  config.DatabaseConfig     `yaml:"destination"`
	Replication  config.ReplicationConfig  `yaml:"replication"`
	Jobs         map[string]Spec           `yaml:"jobs"`
	Processing   config.ProcessingConfig   `yaml:"processing"`
	Safety       safetyBlock               `yaml:"safety"`
	Verification config.VerificationConfig `yaml:"verification"`
	Logging      config.LoggingConfig      `yaml:"logging"`
}

type safetyBlock struct {
	DisableForeignKeyChecks bool `yaml:"disable_foreign_key_checks"`
}

// Load reads and validates the configuration file at path.
func Load(path string) (*Document, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	return Parse(data)
}

// Parse validates a configuration held in memory. Every problem found is reported at once,
// as config.ValidationErrors; a 2.x-shaped file also gets a pointer to UpgradeGuide.
func Parse(data []byte) (*Document, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var root yaml.Node
	if err := dec.Decode(&root); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("config: %w", err)
	}
	if root.Kind != yaml.DocumentNode || len(root.Content) != 1 || root.Content[0].Kind != yaml.MappingNode {
		return nil, errors.New("config: the file must hold one YAML mapping")
	}
	// yaml.Unmarshal would read the first document only, so a job written after a "---"
	// line would vanish without an error.
	var extra yaml.Node
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("config: the file must hold one YAML document; remove every \"---\" separator")
	}
	top := root.Content[0]

	defaults := config.DefaultConfig()
	doc := document{
		Source:       defaults.Source,
		Destination:  defaults.Destination,
		Replication:  defaults.Replication,
		Processing:   defaults.Processing,
		Safety:       safetyBlock{DisableForeignKeyChecks: defaults.Safety.DisableForeignKeyChecks},
		Verification: defaults.Verification,
		Logging:      defaults.Logging,
	}
	// Decode before walking for unknown keys: the decoder refuses excessive aliasing, which
	// would otherwise make the walk below expand every alias first.
	if err := top.Decode(&doc); err != nil {
		return nil, config.ValidationErrors{{Field: "config", Message: err.Error()}}
	}
	errs := unknownKeys(top, reflect.TypeFor[document](), "")

	shared := &config.Config{
		Source:       doc.Source,
		Destination:  doc.Destination,
		Replication:  doc.Replication,
		Processing:   doc.Processing,
		Safety:       config.SafetyConfig{DisableForeignKeyChecks: doc.Safety.DisableForeignKeyChecks},
		Verification: doc.Verification,
		Logging:      doc.Logging,
	}
	shared.Normalize()
	if err := shared.ValidateShared(); err != nil {
		var ve config.ValidationErrors
		if !errors.As(err, &ve) {
			return nil, err
		}
		errs = append(errs, ve...)
	}

	d := &Document{Shared: shared, Jobs: make(map[string]*Spec, len(doc.Jobs))}
	for name, s := range doc.Jobs {
		d.Jobs[name] = &s
	}
	if len(d.Jobs) == 0 {
		errs = append(errs, config.ValidationError{Field: "jobs", Message: "at least one job must be defined"})
	}
	errs = append(errs, validateJobNames(d.JobNames())...)
	for _, name := range d.JobNames() {
		errs = append(errs, validateJob(shared, name, d.Jobs[name])...)
	}
	if len(errs) > 0 {
		return nil, withGuide(errs)
	}
	for _, s := range d.Jobs {
		fillReferences(s.PrimaryKey, s.Relations)
	}
	return d, nil
}

// maxJobNameLength is the width of archiver_job.job_name, VARCHAR(255).
const maxJobNameLength = 255

// validateJobNames refuses names the job registry cannot keep apart. archiver_job.job_name
// is unique under the schema's collation, by default utf8mb4_0900_ai_ci, which ignores
// case and accents: "Foo" and "foo", or "Café" and "cafe", would share one registry row
// and one log. Go cannot reproduce MySQL's Unicode folding, so names are ASCII, and two
// names equal under ASCII case folding are refused. 2.x never met this: viper lowercased
// every job name.
func validateJobNames(names []string) config.ValidationErrors {
	var errs config.ValidationErrors
	first := make(map[string]string, len(names))
	for _, name := range names {
		field := "jobs." + name
		switch {
		case name == "":
			errs = append(errs, config.ValidationError{Field: "jobs", Message: "a job name must not be empty"})
			continue
		case strings.IndexFunc(name, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0:
			errs = append(errs, config.ValidationError{Field: field, Message: fmt.Sprintf("job name %q must not contain spaces or control characters", name)})
		case strings.IndexFunc(name, func(r rune) bool { return r > unicode.MaxASCII }) >= 0:
			errs = append(errs, config.ValidationError{Field: field, Message: fmt.Sprintf("job name %q must be ASCII; the job registry compares names ignoring case and accents", name)})
		case utf8.RuneCountInString(name) > maxJobNameLength:
			errs = append(errs, config.ValidationError{Field: field, Message: fmt.Sprintf("a job name must not exceed %d characters", maxJobNameLength)})
		}
		folded := strings.ToLower(name)
		if prev, dup := first[folded]; dup {
			errs = append(errs, config.ValidationError{Field: field, Message: fmt.Sprintf(
				"job name %q differs from %q only in case; the job registry treats them as one job", name, prev)})
			continue
		}
		first[folded] = name
	}
	return errs
}

// withGuide appends the upgrade pointer when an error is a sign of a 2.x configuration: a
// job without job_type, or a key 3.0 removed. A 3.0 user's typo gets no pointer.
func withGuide(errs config.ValidationErrors) error {
	for _, e := range errs {
		if is2xSignature(e) {
			return append(errs, config.ValidationError{
				Field:   "config",
				Message: "GoArchive 3.0 changed the configuration format; see " + UpgradeGuide,
			})
		}
	}
	return errs
}

func is2xSignature(e config.ValidationError) bool {
	if strings.HasSuffix(e.Field, ".job_type") && strings.HasPrefix(e.Message, "job_type is required") {
		return true
	}
	if !strings.HasPrefix(e.Message, "unknown key") {
		return false
	}
	// The walk reports an unknown block once and does not descend into it, so "replica"
	// stands for the whole removed block.
	switch e.Field {
	case "replica", "safety.lag_threshold", "safety.check_interval":
		return true
	}
	return false
}

var unmarshalerType = reflect.TypeFor[yaml.Unmarshaler]()

// unknownKeys walks n against the Go type t and reports every mapping key that t does not
// declare, with its path and line. Aliases and merge keys (<<) are followed.
func unknownKeys(n *yaml.Node, t reflect.Type, path string) config.ValidationErrors {
	for n.Kind == yaml.AliasNode {
		n = n.Alias
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if reflect.PointerTo(t).Implements(unmarshalerType) {
		return nil
	}
	var errs config.ValidationErrors
	switch t.Kind() {
	case reflect.Struct:
		if n.Kind != yaml.MappingNode {
			return nil // the decoder reports the type mismatch
		}
		fields := yamlFields(t)
		for i := 0; i+1 < len(n.Content); i += 2 {
			k, v := n.Content[i], n.Content[i+1]
			if k.Tag == "!!merge" {
				errs = append(errs, mergedKeys(v, t, path)...)
				continue
			}
			p := joinPath(path, k.Value)
			ft, ok := fields[k.Value]
			if !ok {
				errs = append(errs, config.ValidationError{Field: p, Message: fmt.Sprintf("unknown key (line %d)", k.Line)})
				continue
			}
			errs = append(errs, unknownKeys(v, ft, p)...)
		}
	case reflect.Map:
		if n.Kind != yaml.MappingNode {
			return nil
		}
		for i := 0; i+1 < len(n.Content); i += 2 {
			errs = append(errs, unknownKeys(n.Content[i+1], t.Elem(), joinPath(path, n.Content[i].Value))...)
		}
	case reflect.Slice:
		if n.Kind != yaml.SequenceNode {
			return nil
		}
		for i, item := range n.Content {
			errs = append(errs, unknownKeys(item, t.Elem(), fmt.Sprintf("%s[%d]", path, i))...)
		}
	}
	return errs
}

// mergedKeys checks the mapping (or list of mappings) a merge key pulls in.
func mergedKeys(v *yaml.Node, t reflect.Type, path string) config.ValidationErrors {
	for v.Kind == yaml.AliasNode {
		v = v.Alias
	}
	if v.Kind == yaml.SequenceNode {
		var errs config.ValidationErrors
		for _, item := range v.Content {
			errs = append(errs, unknownKeys(item, t, path)...)
		}
		return errs
	}
	return unknownKeys(v, t, path)
}

// yamlFields maps each yaml tag name of struct t to its field type.
func yamlFields(t reflect.Type) map[string]reflect.Type {
	out := make(map[string]reflect.Type, t.NumField())
	for i := range t.NumField() {
		f := t.Field(i)
		name, _, _ := strings.Cut(f.Tag.Get("yaml"), ",")
		if name == "" || name == "-" {
			continue
		}
		out[name] = f.Type
	}
	return out
}

func joinPath(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}
