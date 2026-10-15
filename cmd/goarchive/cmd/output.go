package cmd

import (
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/dbsmedya/goarchive/v3/internal/engine"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
)

// banners are the warnings a run prints once, before it starts (R45): each names a setting
// that weakens the run's checks.
func banners(doc *jobspec.Document, typ jobspec.Type, v jobspec.Verification, skipPolicy, forceTriggers bool) []string {
	var out []string
	if skipPolicy {
		out = append(out, "WARNING: --"+skipPolicyFlag+": preflight's policy checks are skipped (storage engines, "+
			"permissions, triggers, foreign-key coverage, destination compatibility, packet size). The keys are still read "+
			"from MySQL, and the log-key and binding checks still run. A misspelled foreign_key column fails as a "+
			"statement-caps error instead of RELATION_KEY_CHECK.")
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

// emitBanners writes the R45 banners (spec §3.4a): mandatory on stderr, in stderr's format,
// and recorded in the log, both past the level and file_only. When stderr is the log stream,
// the banner is one entry there; otherwise a plain line on stderr and a log record, which
// Always keeps off the tee.
func emitBanners(log *logger.Logger, stderr io.Writer, bs []string) {
	for _, b := range bs {
		if !log.ConsoleStderr() {
			_, _ = fmt.Fprintln(stderr, b)
		}
		log.Always().Warn(b)
	}
}

// printStart is a run's start line (spec §3.5), with the log file when there is one.
func printStart(w io.Writer, name string, typ jobspec.Type, logFile string) {
	_, _ = fmt.Fprintf(w, "Job %s (%s): starting.", name, typ)
	if logFile != "" {
		_, _ = fmt.Fprintf(w, " Log: %s", logFile)
	}
	_, _ = fmt.Fprintln(w)
}

// status is a run's outcome as the closing block names it.
func status(rep *engine.Report, err error) string {
	switch {
	case err != nil:
		return "failed"
	case rep.Stopped:
		return "stopped before the end of the where range: run the job again to continue"
	}
	return "completed"
}

// printReport prints the closing block (spec §3.5): the status and the duration, the roots,
// then the counts as a table, one row per table in copy order, one column per kind of count.
func printReport(w io.Writer, name string, typ jobspec.Type, order []string, rep *engine.Report, err error) {
	_, _ = fmt.Fprintf(w, "Job %s (%s): %s in %.3fs\n", name, typ, status(rep, err), rep.Elapsed.Seconds())
	_, _ = fmt.Fprintf(w, "  roots: %d in %d %s\n", rep.Roots, rep.Batches, plural(rep.Batches, "batch", "batches"))
	if rep.Recovered > 0 {
		_, _ = fmt.Fprintf(w, "  recovered: %d logged roots finished by replay\n", rep.Recovered)
	}
	if rep.Skipped > 0 {
		_, _ = fmt.Fprintf(w, "  skipped: %d roots this job had already copied (resume)\n", rep.Skipped)
	}
	for _, table := range sortedKeys(rep.Conversions) {
		var codes []string
		for _, code := range sortedKeys(rep.Conversions[table]) {
			codes = append(codes, fmt.Sprintf("%d=%d", code, rep.Conversions[table][code]))
		}
		_, _ = fmt.Fprintf(w, "  conversions accepted (verification: none): %s %s\n", table, strings.Join(codes, " "))
	}

	type column struct {
		header string
		counts map[string]int64
	}
	var cols []column
	if typ != jobspec.Purge {
		cols = append(cols, column{"copied", rep.Copied})
	}
	if typ != jobspec.CopyOnly {
		cols = append(cols, column{"deleted", rep.Deleted})
	}
	if len(rep.Verified) > 0 {
		cols = append(cols, column{"verified (" + string(rep.Verification) + ")", rep.Verified})
	}
	nameWidth := len("table")
	for _, table := range order {
		nameWidth = max(nameWidth, len(table))
	}
	widths := make([]int, len(cols))
	for i, c := range cols {
		widths[i] = len(c.header)
		for _, table := range order {
			widths[i] = max(widths[i], len(strconv.FormatInt(c.counts[table], 10)))
		}
	}
	row := func(first string, cell func(i int) string) {
		var b strings.Builder
		fmt.Fprintf(&b, "  %-*s", nameWidth, first)
		for i := range cols {
			fmt.Fprintf(&b, "  %*s", widths[i], cell(i))
		}
		_, _ = fmt.Fprintln(w, b.String())
	}
	_, _ = fmt.Fprintln(w)
	row("table", func(i int) string { return cols[i].header })
	for _, table := range order {
		row(table, func(i int) string { return strconv.FormatInt(cols[i].counts[table], 10) })
	}
}

// printFailureSummary is the stderr line before a failed run's error (spec §3.4a): the
// duration when the engine returned a report, and the log file when there is one.
func printFailureSummary(w io.Writer, name string, typ jobspec.Type, rep *engine.Report, logFile string) {
	if rep != nil {
		_, _ = fmt.Fprintf(w, "Job %s (%s): failed in %.3fs.", name, typ, rep.Elapsed.Seconds())
	} else {
		_, _ = fmt.Fprintf(w, "Job %s (%s): failed before the first batch.", name, typ)
	}
	if logFile != "" {
		_, _ = fmt.Fprintf(w, " Log: %s", logFile)
	}
	_, _ = fmt.Fprintln(w)
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// logOutcome writes a run's last log entry: how the run ended, with the report's counts when
// the engine returned one (GH #5). The closing block reaches stdout only with --progress, so
// without this a log file, under file_only too, would end at the last batch, and a failed run
// would leave no entry; 2.x logged "Archive complete" for the same reason. A failure is logged
// at error level without the stack trace the logger attaches there. A file log's closing entry
// is FileOnly: the closing block and the failure summary carry the outcome to the console
// (spec §3.3). always writes it past the level filter: under --progress on a JSON console log
// the entry is the closing block.
func logOutcome(log *logger.Logger, typ jobspec.Type, rep *engine.Report, err error, always bool) {
	fields := []any{"job_type", string(typ)}
	if rep != nil {
		fields = append(fields, "roots", rep.Roots, "batches", rep.Batches)
		if rep.Recovered > 0 {
			fields = append(fields, "recovered", rep.Recovered)
		}
		if rep.Skipped > 0 {
			fields = append(fields, "skipped", rep.Skipped)
		}
		for _, counts := range []struct {
			name  string
			table map[string]int64
		}{{"copied", rep.Copied}, {"deleted", rep.Deleted}, {"verified", rep.Verified}} {
			if len(counts.table) > 0 {
				fields = append(fields, counts.name, counts.table)
			}
		}
		if rep.Verification != "" {
			fields = append(fields, "verification", string(rep.Verification))
		}
		if len(rep.Conversions) > 0 {
			fields = append(fields, "conversions", rep.Conversions)
		}
		fields = append(fields, "elapsed_seconds", rep.Elapsed.Seconds())
	}
	out := log.SugaredLogger
	if always {
		out = log.Always()
	}
	if log.File() != "" {
		fields = append(fields, logger.FileOnly)
	}
	switch {
	case err != nil:
		fields = append(fields, "error", err.Error())
		out.Desugar().WithOptions(zap.AddStacktrace(zapcore.FatalLevel)).Sugar().Errorw("Run failed", fields...)
	case rep != nil && rep.Stopped:
		out.Infow("Run stopped before the end of the where range: run the job again to continue", fields...)
	default:
		out.Infow("Run completed", fields...)
	}
}

// endRun logs how a run ended (logOutcome). p is the value recovered from a panic, nil if
// the run returned: a panicking run has no error and no report yet, so the panic is its error.
func endRun(log *logger.Logger, typ jobspec.Type, rep *engine.Report, err error, p any, always bool) {
	if p != nil {
		err = fmt.Errorf("panic: %v", p)
	}
	logOutcome(log, typ, rep, err, always)
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
