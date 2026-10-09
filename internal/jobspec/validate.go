package jobspec

import (
	"fmt"
	"strings"

	"github.com/dbsmedya/dbsgomysql/pkg/sqlutil"
	"github.com/dbsmedya/goarchive/v3/internal/config"
)

// maxRelationDepth is kept from 2.x.
const maxRelationDepth = 10

// validateJob checks one job's own settings and its relations. It needs no connection:
// everything here follows from the file alone (spec §5). Whether the documented keys match
// MySQL's is preflight's check (spec §8.2, M3).
func validateJob(shared *config.Config, name string, s *Spec) config.ValidationErrors {
	p := "jobs." + name
	var errs config.ValidationErrors
	add := func(field, msg string, args ...any) {
		errs = append(errs, config.ValidationError{Field: field, Message: fmt.Sprintf(msg, args...)})
	}

	switch {
	case s.Type == "":
		add(p+".job_type", "job_type is required: archive, purge or copy-only")
	case !s.Type.Valid():
		add(p+".job_type", "job_type %q is not one of archive, purge, copy-only", string(s.Type))
	}
	errs = append(errs, validateTable(p+".root_table", s.RootTable)...)
	errs = append(errs, validateColumns(p+".primary_key", s.PrimaryKey, true)...)
	if strings.TrimSpace(s.Where) == "" {
		add(p+".where", `where is required; use where: "1=1" explicitly to process the entire table`)
	}

	// Each job type accepts only the settings it uses (spec §5).
	if s.Type == Purge && s.Verification != nil {
		add(p+".verification", "a purge job copies nothing, so there is nothing to verify; remove this block")
	}
	if s.Type == CopyOnly && s.Processing != nil {
		if s.Processing.BatchDeleteSize != nil {
			add(p+".processing.batch_delete_size", "a copy-only job deletes nothing; remove this setting")
		}
		if s.Processing.DeleteSleepSeconds != nil {
			add(p+".processing.delete_sleep_seconds", "a copy-only job deletes nothing; remove this setting")
		}
	}
	if s.Resume != nil && s.Type != CopyOnly {
		add(p+".resume", "resume applies only to copy-only jobs")
	}

	if s.Processing != nil {
		errs = append(errs, config.ValidateProcessingConfig(p+".processing", s.EffectiveProcessing(shared.Processing))...)
	}
	if s.Verification != nil && s.Type != Purge && !s.Verification.Valid() {
		errs = append(errs, invalidVerification(p+".verification", *s.Verification))
	}
	if s.Logging != nil {
		errs = append(errs, config.ValidateLoggingConfig(p+".logging", s.EffectiveLogging(shared.Logging))...)
	}

	seen := map[string]string{}
	if s.RootTable != "" {
		seen[strings.ToLower(s.RootTable)] = p + ".root_table"
	}
	for i := range s.Relations {
		errs = append(errs, validateRelation(fmt.Sprintf("%s.relations[%d]", p, i), &s.Relations[i], s.PrimaryKey, 1, seen)...)
	}
	return errs
}

func validateRelation(p string, r *Relation, parentKey Columns, depth int, seen map[string]string) config.ValidationErrors {
	var errs config.ValidationErrors
	add := func(field, msg string, args ...any) {
		errs = append(errs, config.ValidationError{Field: field, Message: fmt.Sprintf(msg, args...)})
	}
	if depth > maxRelationDepth {
		add(p, "relation nesting exceeds the maximum depth of %d", maxRelationDepth)
		return errs
	}

	errs = append(errs, validateTable(p+".table", r.Table)...)
	if r.Table != "" {
		// MySQL table names are case-insensitive under lower_case_table_names=1 or 2, so a
		// second spelling is refused too. This tightens 2.x, whose graph builder compared
		// names exactly.
		if where, dup := seen[strings.ToLower(r.Table)]; dup {
			add(p+".table", "table %q already appears at %s; each table appears once in a job", r.Table, where)
		} else {
			seen[strings.ToLower(r.Table)] = p + ".table"
		}
	}
	errs = append(errs, validateColumns(p+".primary_key", r.PrimaryKey, true)...)
	errs = append(errs, validateColumns(p+".foreign_key", r.ForeignKey, true)...)
	errs = append(errs, validateColumns(p+".references", r.References, false)...)

	// references pairs with foreign_key by position and lists the parent's key columns,
	// each exactly once, in any order (spec §5, §7.3). It may be omitted only when the
	// parent key has one column. The parent key here is the documented one; preflight
	// checks that it is MySQL's (R31).
	refs := r.References
	switch {
	case len(parentKey) == 0:
		// The parent's missing primary_key is already reported.
	case len(refs) == 0 && len(parentKey) > 1:
		add(p+".references", "references is required: the parent key has %d columns (%s); list them in the order that pairs with foreign_key",
			len(parentKey), strings.Join(parentKey, ", "))
	case len(refs) == 0:
		refs = Columns{parentKey[0]}
		fallthrough
	default:
		if len(refs) > 0 && !isPermutation(refs, parentKey) {
			add(p+".references", "references (%s) must list each parent key column (%s) exactly once, in any order",
				strings.Join(refs, ", "), strings.Join(parentKey, ", "))
		}
		if len(r.ForeignKey) > 0 && len(refs) > 0 && len(r.ForeignKey) != len(refs) {
			add(p+".foreign_key", "foreign_key has %d columns but references has %d; they pair by position",
				len(r.ForeignKey), len(refs))
		}
	}

	switch r.DependencyType {
	case "", "1-1", "1-N":
	default:
		add(p+".dependency_type", "dependency_type must be '1-1' or '1-N'")
	}

	for i := range r.Relations {
		errs = append(errs, validateRelation(fmt.Sprintf("%s.relations[%d]", p, i), &r.Relations[i], r.PrimaryKey, depth+1, seen)...)
	}
	return errs
}

func validateTable(field, table string) config.ValidationErrors {
	switch {
	case table == "":
		return config.ValidationErrors{{Field: field, Message: "table name is required"}}
	case !sqlutil.IsSimpleIdentifier(table):
		return config.ValidationErrors{{Field: field, Message: fmt.Sprintf("table %q must contain only letters, digits and underscores", table)}}
	}
	return nil
}

// validateColumns checks a column list: present when required, every name a simple
// identifier, and no column listed twice (MySQL column names are case-insensitive).
func validateColumns(field string, cols Columns, required bool) config.ValidationErrors {
	var errs config.ValidationErrors
	if len(cols) == 0 {
		if required {
			errs = append(errs, config.ValidationError{Field: field, Message: fmt.Sprintf("%s is required", field[strings.LastIndex(field, ".")+1:])})
		}
		return errs
	}
	seen := make(map[string]bool, len(cols))
	for _, c := range cols {
		if !sqlutil.IsSimpleIdentifier(c) {
			errs = append(errs, config.ValidationError{Field: field, Message: fmt.Sprintf("column %q must contain only letters, digits and underscores", c)})
		}
		if seen[strings.ToLower(c)] {
			errs = append(errs, config.ValidationError{Field: field, Message: fmt.Sprintf("column %q is listed twice", c)})
		}
		seen[strings.ToLower(c)] = true
	}
	return errs
}

// isPermutation reports whether a lists exactly the columns of b, each once, in any order.
// Spelling must match exactly, as for primary_key (R31).
func isPermutation(a, b Columns) bool {
	if len(a) != len(b) {
		return false
	}
	count := make(map[string]int, len(b))
	for _, c := range b {
		count[c]++
	}
	for _, c := range a {
		if count[c] == 0 {
			return false
		}
		count[c]--
	}
	return true
}

// fillReferences writes the parent's one key column into every omitted references, so
// later stages and the binding fingerprint (spec §6.2) see the pairing written out. It runs
// only on a validated job.
func fillReferences(parentKey Columns, rels []Relation) {
	for i := range rels {
		r := &rels[i]
		if len(r.References) == 0 {
			r.References = Columns{parentKey[0]}
		}
		fillReferences(r.PrimaryKey, r.Relations)
	}
}

func invalidVerification(field string, v Verification) config.ValidationError {
	return config.ValidationError{Field: field, Message: fmt.Sprintf("verification must be sha256, count or none, not %q", string(v))}
}
