package cmd

import (
	"fmt"
	"io"
	"slices"
	"strings"
	"text/tabwriter"

	"github.com/dbsmedya/goarchive/v3/internal/engine"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
)

// banners are the warnings a run prints once, before it starts (R45): each names a setting
// that weakens the run's checks.
func banners(doc *jobspec.Document, typ jobspec.Type, v jobspec.Verification, skipPolicy, forceTriggers bool) []string {
	var out []string
	if skipPolicy {
		out = append(out, "WARNING: --skip-validate-preflight: preflight's policy checks are skipped (permissions, triggers, "+
			"foreign-key coverage, destination compatibility). The keys are still read from MySQL, and the log-key and "+
			"binding checks still run. A misspelled foreign_key column fails as a statement-caps error instead of "+
			"RELATION_KEY_CHECK.")
	}
	if forceTriggers {
		out = append(out, "WARNING: --force-triggers: DELETE triggers on source tables are accepted; every delete fires them.")
	}
	if v == jobspec.VerifyNone && typ != jobspec.Purge {
		msg := "WARNING: verification: none: copied rows are not compared, and conversion warnings are accepted."
		if typ == jobspec.Archive {
			msg += " The source rows are then deleted."
		}
		out = append(out, msg)
	}
	if doc.Shared.Safety.DisableForeignKeyChecks && typ != jobspec.Purge {
		out = append(out, "WARNING: safety.disable_foreign_key_checks: destination inserts run with FOREIGN_KEY_CHECKS=0 "+
			"and do not validate foreign keys.")
	}
	return out
}

// printReport prints what a run did.
func printReport(w io.Writer, name string, typ jobspec.Type, rep *engine.Report) {
	_, _ = fmt.Fprintf(w, "Job %s (%s): %d roots in %d batches\n", name, typ, rep.Roots, rep.Batches)
	if rep.Recovered > 0 {
		_, _ = fmt.Fprintf(w, "  recovered: %d logged roots finished by replay\n", rep.Recovered)
	}
	if rep.Skipped > 0 {
		_, _ = fmt.Fprintf(w, "  skipped: %d roots this job had already copied (resume)\n", rep.Skipped)
	}
	if len(rep.Copied) > 0 {
		_, _ = fmt.Fprintf(w, "  copied:  %s\n", perTable(rep.Copied))
	}
	if len(rep.Deleted) > 0 {
		_, _ = fmt.Fprintf(w, "  deleted: %s\n", perTable(rep.Deleted))
	}
	if len(rep.Verified) > 0 {
		_, _ = fmt.Fprintf(w, "  verified (%s): %s\n", rep.Verification, perTable(rep.Verified))
	}
	for _, table := range sortedKeys(rep.Conversions) {
		var codes []string
		for _, code := range sortedKeys(rep.Conversions[table]) {
			codes = append(codes, fmt.Sprintf("%d=%d", code, rep.Conversions[table][code]))
		}
		_, _ = fmt.Fprintf(w, "  conversions accepted (verification: none): %s %s\n", table, strings.Join(codes, " "))
	}
	if rep.Stopped {
		_, _ = fmt.Fprintln(w, "  stopped before the end of the where range: run the job again to continue")
	}
	_, _ = fmt.Fprintf(w, "  elapsed: %.3fs\n", rep.Elapsed.Seconds())
}

func perTable(m map[string]int64) string {
	var parts []string
	for _, table := range sortedKeys(m) {
		parts = append(parts, fmt.Sprintf("%s=%d", table, m[table]))
	}
	return strings.Join(parts, " ")
}

func sortedKeys[K interface{ ~string | ~uint16 }, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// printCaps prints the effective statement caps (spec §4.3, M6-5).
func printCaps(w io.Writer, caps []preflight.Cap) {
	_, _ = fmt.Fprintln(w, "Statement caps (key tuples per statement):")
	for _, c := range caps {
		_, _ = fmt.Fprintf(w, "  %-11s  %-9s  %s (%s): %d\n", c.Server, c.Statement, c.Table, strings.Join(c.Columns, ", "), c.Tuples)
	}
}

// printExplanation prints dry-run's report (spec §7.2).
func printExplanation(w io.Writer, name string, typ jobspec.Type, ex *engine.Explanation) {
	_, _ = fmt.Fprintf(w, "Dry-run of %s (%s): the first batch holds %d root keys\n\n", name, typ, ex.Roots)
	printPlan(w, "Root fetch on "+ex.Fetch.Table, ex.Fetch)
	for _, p := range ex.Discovery {
		printPlan(w, "Discovery of "+p.Table, p)
	}
	for _, n := range ex.Notes {
		_, _ = fmt.Fprintf(w, "Note: %s\n", n)
	}
	if len(ex.Notes) > 0 {
		_, _ = fmt.Fprintln(w)
	}
	printCaps(w, ex.Preflight.Caps)
}

func printPlan(w io.Writer, title string, p engine.Plan) {
	_, _ = fmt.Fprintf(w, "%s:\n  %s\n", title, strings.Join(strings.Fields(p.Query), " "))
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "  id\tselect_type\ttable\tpartitions\ttype\tpossible_keys\tkey\tkey_len\tref\trows\tfiltered\tExtra")
	for _, r := range p.Rows {
		_, _ = fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			r.ID, r.SelectType, r.Table, r.Partitions, r.Type, r.PossibleKeys, r.Key, r.KeyLen, r.Ref, r.Rows, r.Filtered, r.Extra)
	}
	_ = tw.Flush()
	_, _ = fmt.Fprintln(w)
}
