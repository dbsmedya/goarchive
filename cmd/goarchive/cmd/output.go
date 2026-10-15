package cmd

import (
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

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

// printExplanation prints dry-run's report (console-output spec §3.8): what the job covers,
// its orders with the first batch's rows, the settings the run would use, the caps, then one
// section per statement with its plan and its notes.
func printExplanation(w io.Writer, name string, typ jobspec.Type, file string, doc *jobspec.Document, spec *jobspec.Spec,
	v jobspec.Verification, ex *engine.Explanation) {
	p := func(format string, a ...any) { _, _ = fmt.Fprintf(w, format, a...) }
	p("Dry-run of %s (%s). No data is changed.\n\n", name, typ)

	proc := spec.EffectiveProcessing(doc.Shared.Processing)
	p("Job\n")
	p("  root table:      %s, key (%s)\n", spec.RootTable, strings.Join(spec.PrimaryKey, ", "))
	p("  where:           %s\n", strings.Join(strings.Split(strings.TrimRight(spec.Where, "\n"), "\n"), "\n"+strings.Repeat(" ", 19)))
	if ex.Count > 0 && proc.BatchSize > 0 {
		batches := (ex.Count + int64(proc.BatchSize) - 1) / int64(proc.BatchSize)
		p("  matching roots:  %d, about %d %s of %d (counted now; the range can change before the run)\n\n",
			ex.Count, batches, plural(int(batches), "batch", "batches"), proc.BatchSize)
	} else {
		p("  matching roots:  %d (counted now; the range can change before the run)\n\n", ex.Count)
	}

	var tables []string
	for _, n := range jobspec.NewTree(spec).CopyOrder() {
		tables = append(tables, n.Table)
	}
	reversed := slices.Clone(tables)
	slices.Reverse(reversed)
	printOrder := func(title string, order []string, counts bool) {
		width := len(title)
		for _, t := range order {
			width = max(width, len("  1. ")+len(t))
		}
		width += 5
		if counts {
			p("%-*s%s\n", width, title, "rows in the first batch")
		} else {
			p("%s\n", title)
		}
		for i, t := range order {
			item := fmt.Sprintf("  %d. %s", i+1, t)
			if counts {
				p("%-*s%d\n", width, item, ex.Rows[t])
			} else {
				p("%s\n", item)
			}
		}
	}
	if typ != jobspec.Purge {
		printOrder("Copy order (parents first)", tables, true)
	}
	if typ != jobspec.CopyOnly {
		printOrder("Delete order (children first)", reversed, typ == jobspec.Purge)
	}
	p("\n")

	p("Settings\n")
	p("  batch_size: %d   batch_delete_size: %d   sleep_seconds: %g   delete_sleep_seconds: %g\n",
		proc.BatchSize, proc.BatchDeleteSize, proc.SleepSeconds, proc.DeleteSleepSeconds)
	if proc.SentinelFile != "" {
		p("  sentinel_file: %s\n", proc.SentinelFile)
	}
	if typ != jobspec.Purge {
		p("  verification: %s\n", v)
		p("  destination foreign key checks: %s\n", onOff(!doc.Shared.Safety.DisableForeignKeyChecks))
	}
	if typ == jobspec.CopyOnly && spec.ResumeEnabled() {
		p("  resume: on (rows this job already copied are skipped by the run; the counts above are candidates)\n")
	}
	switch rc := doc.Shared.Replication; {
	case !rc.Enabled:
		p("  replication lag gate: off\n")
	case typ == jobspec.CopyOnly:
		p("  replication lag gate: configured, not used (copy-only deletes nothing)\n")
	default:
		p("  replication lag gate: on, hold above %ds, check every %ds, cache %ds\n", rc.SecondsBehindSourceWithin, rc.CheckInterval, rc.CacheTTL)
		for _, sv := range rc.Servers {
			p("    %s  channels: %s\n", sv.Addr(), describeChannels(sv.Channels))
		}
	}
	p("\n")

	printCaps(w, ex.Preflight.Caps)
	p("\n")

	p("Plans for the first batch (EXPLAIN FORMAT=TRADITIONAL)\n\n")
	f := ex.Fetch.Display
	p("Root fetch on %s\n", ex.Fetch.Table)
	p("  %s\n", f.Select)
	where := strings.Split(strings.TrimRight(f.Where, "\n"), "\n")
	p("  WHERE (%s", strings.Join(where, "\n"+strings.Repeat(" ", 9)))
	// The SQL closes the where on a new line, so a trailing -- comment cannot swallow the
	// parenthesis; the report does the same only when it must.
	if strings.Contains(where[len(where)-1], "--") {
		p("\n  )\n")
	} else {
		p(")\n")
	}
	p("  %s\n", f.Tail)
	printPlanRows(w, ex.Fetch)
	p("\n")
	if ex.Roots == 0 {
		p("Note: no rows match the where clause, so there are no keys to bind: discovery was not explained.\n\n")
	}
	for _, d := range ex.Discovery {
		if d.Skipped != "" {
			p("Discovery of %s\n  Note: %s, so this table's discovery was not explained.\n\n", d.Table, d.Skipped)
			continue
		}
		chunk := fmt.Sprintf("all %d %s tuples", d.Display.Of, d.Display.Parent)
		if d.Display.Tuples != d.Display.Of {
			chunk = fmt.Sprintf("%d of %d %s tuples", d.Display.Tuples, d.Display.Of, d.Display.Parent)
		}
		p("Discovery of %s (first discovery chunk: %s)\n", d.Table, chunk)
		p("  %s\n", d.Display.Select)
		p("  WHERE %s\n", d.Display.Predicate)
		printPlanRows(w, d)
		p("\n")
	}
	p("Next: goarchive %s -c %s -j %s\n", typ, file, name)
}

// printPlanRows prints a statement's EXPLAIN rows, one line each, then its notes.
func printPlanRows(w io.Writer, pl engine.Plan) {
	for _, r := range pl.Rows {
		_, _ = fmt.Fprintf(w, "  %s\n", describeRow(r))
	}
	for _, n := range pl.Notes {
		_, _ = fmt.Fprintf(w, "  Note: %s\n", n)
	}
}

// describeRow is one EXPLAIN row as one line, by access type (Codex F4): ALL is a full table
// scan, index a full index scan, a row with no table the optimizer's message, never a scan.
func describeRow(r engine.ExplainRow) string {
	var b strings.Builder
	if r.SelectType != "" && r.SelectType != "SIMPLE" && r.SelectType != "PRIMARY" {
		b.WriteString("[" + r.SelectType + "] ")
	}
	switch {
	case r.Table == "" && r.Type == "":
		return b.String() + "optimizer: " + r.Extra
	case r.Type == "ALL":
		keys := r.PossibleKeys
		if keys == "" {
			keys = "none"
		}
		fmt.Fprintf(&b, "%-16s full table scan, possible keys: %s", r.Table, keys)
	case r.Type == "index":
		fmt.Fprintf(&b, "%-16s full index scan on %s", r.Table, r.Key)
	default:
		fmt.Fprintf(&b, "%-16s %s on %s", r.Table, r.Type, r.Key)
	}
	if r.Rows != "" {
		fmt.Fprintf(&b, ", ~%s rows", r.Rows)
	}
	if f := r.Filtered; f != "" {
		if strings.Contains(f, ".") {
			f = strings.TrimSuffix(strings.TrimRight(f, "0"), ".")
		}
		fmt.Fprintf(&b, ", %s%% filtered", f)
	}
	if r.Extra != "" {
		b.WriteString("; " + r.Extra)
	}
	return b.String()
}

// describeChannels renders a server's channel selection (ported from 2.2.x's estimator): no
// entries means every channel, the default (unnamed) channel shows as <default>, and a named
// channel is folded onto one line, so a newline in a name cannot forge a report line.
func describeChannels(channels []string) string {
	if len(channels) == 0 {
		return "all"
	}
	names := make([]string, 0, len(channels))
	for _, c := range channels {
		if c == "" {
			names = append(names, "<default>")
			continue
		}
		names = append(names, strings.NewReplacer("\n", " ", "\r", " ").Replace(c))
	}
	return strings.Join(names, ", ")
}

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}
