// Package archiver: batchPipeline is the shared batch engine behind the
// archive, copy-only, and purge orchestrators. One instance is built per
// Execute run. Modes select which phases run; bookkeeping is identical:
// LogBatchPending (caller) -> [MarkBatchCopied] -> CompleteBatch, with the
// checkpoint advanced only inside CompleteBatch's transaction.
//
// The engine NEVER writes a terminal 'failed' status: any phase error aborts
// the run and leaves the batch's rows in their current non-terminal status
// (pending or copied) so status-aware recovery replays them (issues #1, #18).
package archiver

import (
	"context"
	"fmt"
	"sort"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/dbsmedya/goarchive/internal/types"
	"github.com/dbsmedya/goarchive/internal/verifier"
)

// batchMode selects which phases a batch runs.
type batchMode int

const (
	// batchFull (archive): Discover -> Copy -> Verify -> MarkBatchCopied ->
	// (lag re-check) -> Delete -> CompleteBatch.
	batchFull batchMode = iota
	// batchCopyVerify (copy-only): Discover -> Copy -> Verify ->
	// MarkBatchCopied -> CompleteBatch. No delete phase, no lag check.
	batchCopyVerify
	// batchDeleteOnly (purge main loop; copied-replay for archive/purge):
	// Discover -> (lag) -> Delete -> CompleteBatch. Skips copy/verify — used
	// where the copy already happened (or never happens, for purge).
	batchDeleteOnly
	// batchPromote (copied-replay for copy-only): CompleteBatch only. Skips
	// discovery, copy, verify AND delete — these rows' copy+verify already
	// succeeded and copy-only has no delete phase, so nothing remains but the
	// completion tail (issue #1). Never a mainMode: it is a recovery-only mode
	// reached via recoverChunks.
	batchPromote
)

// batchPipeline is the shared engine. copyPhase/dataVerifier are nil for
// purge; deletePhase is nil for copy-only; lagMonitor is nil unless replica
// monitoring is enabled (archive only today). mainMode identifies the owning
// command (batchFull=archive, batchCopyVerify=copy-only, batchDeleteOnly=
// purge) and drives recovery policy in recover().
type batchPipeline struct {
	jobName       string
	mainMode      batchMode
	graph         *graph.Graph
	logger        *logger.Logger
	stopCh        <-chan struct{}
	processingCfg config.ProcessingConfig
	skipVerify    bool
	countMode     bool // effective verification method == "count" (archive recovery gate)
	// checkpointFloor is the job's checkpoint at startup (empty = none). The
	// copy-only per-chunk recovery advance never writes a checkpoint at or
	// below this floor: CompleteBatch has no monotonic guard, and requeuing a
	// legacy failed row BELOW the current checkpoint (our own recovery
	// guidance) would otherwise regress the checkpoint and re-open every
	// completed root above it to the forward scan.
	checkpointFloor string

	discovery    *RecordDiscovery
	copyPhase    *CopyPhase
	dataVerifier *verifier.Verifier
	deletePhase  *DeletePhase
	resumeMgr    *ResumeManager
	fetcher      *RootIDFetcher
	lagMonitor   lagWaiter
}

// add accumulates o into s so orchestrators can fold chunk stats into their
// run-level result types.
func (s *BatchStats) add(o *BatchStats) {
	s.RootsProcessed += o.RootsProcessed
	s.RecordsCopied += o.RecordsCopied
	s.RecordsDeleted += o.RecordsDeleted
	s.TablesVerified += o.TablesVerified
	s.RecordsVerified += o.RecordsVerified
}

// processBatch runs a whole batch of root PKs through the mode's phases, then
// performs the atomic T3 bookkeeping (CompleteBatch). checkpointPK, when
// non-nil, commits in the same transaction as the completion and is then
// applied to the fetcher — the T3 invariant: the checkpoint can never advance
// past a non-terminal row. Recovery paths that must not advance the main
// checkpoint pass nil. batchPromote selects no phases at all: it runs the
// completion tail alone, which is exactly what a copy-only 'copied' row needs.
//
// On any error the batch's rows keep their current non-terminal status
// (pending or copied) — NEVER failed — so status-aware recovery replays them.
func (p *batchPipeline) processBatch(ctx context.Context, rootIDs []interface{}, mode batchMode, checkpointPK interface{}, checkpoint CheckpointCallback) (*BatchStats, error) {
	stats := &BatchStats{}
	if len(rootIDs) == 0 {
		return stats, nil
	}

	// batchPromote runs the completion tail ONLY. Discovery must not fire: its
	// rows were already copied and verified, and copy-only has no delete phase,
	// so the discovered set has no consumer — issuing the source SELECTs would
	// be pure waste. Both phase gates below enumerate their modes explicitly,
	// so neither admits batchPromote, and nothing derived from `discovered`
	// (recordSet) is reachable in this mode.
	var (
		discovered *types.RecordSet
		recordSet  *RecordSet
	)
	if mode != batchPromote {
		var err error
		discovered, err = p.discovery.Discover(ctx, rootIDs)
		if err != nil {
			return stats, fmt.Errorf("discovery failed: %w", err)
		}
		recordSet = convertRecordSet(discovered)
	}

	if mode == batchFull || mode == batchCopyVerify {
		copyStats, err := p.copyPhase.Copy(ctx, recordSet)
		if err != nil {
			return stats, fmt.Errorf("copy failed: %w", err)
		}
		stats.RecordsCopied = copyStats.RowsCopied

		if !p.skipVerify {
			verifyStats, err := p.dataVerifier.Verify(ctx, discovered)
			if err != nil {
				return stats, fmt.Errorf("verification failed: %w", err)
			}
			if verifyStats != nil {
				stats.TablesVerified += verifyStats.TablesVerified
				stats.RecordsVerified += verifyStats.TotalRows
			}
		}

		// T1.5: durable "copy+verify succeeded" marker.
		if err := p.resumeMgr.MarkBatchCopied(ctx, p.jobName, rootIDs); err != nil {
			return stats, fmt.Errorf("mark batch copied failed: %w", err)
		}
	}

	if mode == batchFull || mode == batchDeleteOnly {
		// Re-check replication lag immediately before the binlog-heavy delete
		// phase (issue #2): the pre-batch check can be stale after a long
		// copy+verify. Fires in both delete-bearing modes.
		if p.lagMonitor != nil {
			if err := p.lagMonitor.WaitForLag(ctx); err != nil {
				return stats, fmt.Errorf("lag monitor error before delete: %w", err)
			}
		}
		deleteStats, err := p.deletePhase.Delete(ctx, recordSet)
		if err != nil {
			return stats, fmt.Errorf("delete failed: %w", err)
		}
		stats.RecordsDeleted = deleteStats.RowsDeleted
	}

	// T3: atomic completion (+ optional checkpoint).
	if err := p.resumeMgr.CompleteBatch(ctx, p.jobName, rootIDs, checkpointPK); err != nil {
		return stats, fmt.Errorf("batch completion bookkeeping failed: %w", err)
	}
	if checkpointPK != nil {
		p.fetcher.UpdateCheckpoint(checkpointPK)
	}

	if checkpoint != nil {
		for _, rootID := range rootIDs {
			if err := checkpoint(rootID, "completed"); err != nil {
				p.logger.Warnw("Checkpoint callback failed", "error", err)
			}
		}
	}

	stats.RootsProcessed = len(rootIDs)
	return stats, nil
}

// aboveCheckpointFloor reports whether rawPK is strictly above the job's
// startup checkpoint. Recovery advances the checkpoint only for chunks whose
// max PK clears the floor — CompleteBatch overwrites the checkpoint
// unconditionally, so writing a lower value (e.g. after the operator requeues
// a legacy failed row below the checkpoint) would regress it and re-open
// every completed root above it to the forward scan.
func (p *batchPipeline) aboveCheckpointFloor(rawPK string, unsigned bool) bool {
	if p.checkpointFloor == "" {
		return true
	}
	return numericPKLess(p.checkpointFloor, rawPK, unsigned)
}

// recoverChunks numerically sorts a status set, chunks it by batch_size, and
// runs each chunk through processBatch in the given mode. advancePerChunk
// selects the checkpoint policy:
//   - false (archive/purge recovery): nil checkpoint — recovered source rows
//     are deleted, so the forward scan cannot re-fetch them.
//   - true (copy-only recovery, both batchCopyVerify replay and batchPromote):
//     the chunk's max PK — copy-only source rows
//     persist, so the checkpoint must move or the forward scan would re-fetch
//     recovered roots (and abort strict-INSERT jobs on duplicates). Chunks
//     ascend and completion+checkpoint commit atomically, and the failed-row
//     gate has already run, so a per-chunk advance can never cross an
//     unresolved lower-PK hole (issue #18).
func (p *batchPipeline) recoverChunks(ctx context.Context, rawPKs []string, mode batchMode, advancePerChunk bool, checkpoint CheckpointCallback, agg *BatchStats) error {
	if len(rawPKs) == 0 {
		return nil
	}
	dataType, unsigned, ok := p.graph.GetRootPKMeta()
	if !ok {
		return fmt.Errorf("root PK metadata not loaded")
	}
	sortPendingPKsNumeric(rawPKs, unsigned)
	// batchPromote gets its own wording: "recovering" understates what is
	// actually happening (no replay, just the completion tail), and operators
	// reading the log should see that no re-copy is being attempted.
	msg := "Recovering non-terminal PKs from prior run"
	if mode == batchPromote {
		msg = "Promoting already-copied PKs to completed (copy+verify already succeeded)"
	}
	p.logger.Infow(msg, "job", p.jobName, "count", len(rawPKs), "mode", mode)

	batchSize := p.processingCfg.BatchSize
	if batchSize <= 0 {
		batchSize = 1
	}
	for start := 0; start < len(rawPKs); start += batchSize {
		// Cooperative graceful stop: each started recovery chunk runs to
		// completion (processBatch is terminal), so stopping at this boundary
		// leaves earlier chunks recovered and the rest in their prior-run
		// status — safe to resume.
		if stopRequested(p.stopCh) {
			p.logger.Warn("Graceful stop requested - stopping recovery at chunk boundary (run again to resume)")
			return nil
		}
		end := start + batchSize
		if end > len(rawPKs) {
			end = len(rawPKs)
		}
		typed := make([]interface{}, 0, end-start)
		for _, raw := range rawPKs[start:end] {
			pk, err := types.ConvertRootPK(raw, dataType, unsigned)
			if err != nil {
				return fmt.Errorf("convert PK %q: %w", raw, err)
			}
			typed = append(typed, pk)
		}
		// Operator pause switch: pause before the next recovery chunk so each
		// started chunk runs to completion first.
		if err := newSentinelGate(p.processingCfg.SentinelFile, p.logger).wait(ctx, p.stopCh); err != nil {
			return err
		}
		if stopRequested(p.stopCh) {
			p.logger.Warn("Graceful stop requested - stopping recovery at chunk boundary (run again to resume)")
			return nil
		}
		if p.lagMonitor != nil {
			if err := p.lagMonitor.WaitForLag(ctx); err != nil {
				return fmt.Errorf("lag monitor error: %w", err)
			}
		}
		var checkpointPK interface{}
		if advancePerChunk && p.aboveCheckpointFloor(rawPKs[end-1], unsigned) {
			checkpointPK = typed[len(typed)-1]
		}
		batchStats, err := p.processBatch(ctx, typed, mode, checkpointPK, checkpoint)
		if err != nil {
			return fmt.Errorf("recovery processBatch failed: %w", err)
		}
		agg.add(batchStats)
	}
	return nil
}

// recover is the status-aware resume entry point shared by all three
// orchestrators. Gate order: legacy failed rows (refuse, all modes) ->
// count-mode (refuse, archive) -> strict-INSERT pending (refuse, archive +
// copy-only) -> copied replay -> pending replay. Returns aggregate stats for
// the caller's result type.
//
// The returned *BatchStats is ALWAYS non-nil, on every error path included, so
// callers may accumulate its counters into their result before checking the
// error (all three orchestrators rely on this to keep partial recovery totals
// when a resume fails part-way through).
func (p *batchPipeline) recover(ctx context.Context, checkpoint CheckpointCallback) (*BatchStats, error) {
	agg := &BatchStats{}

	// Gate 1: rows marked failed (log_status=3) by a pre-1.8 release. This
	// release never writes 'failed' — errors leave rows recoverable — but a
	// legacy failed row below the checkpoint is exactly the silent permanent
	// hole issue #18 describes, so refuse until the operator resolves it.
	failed, err := p.resumeMgr.GetRootPKsByStatus(ctx, p.jobName, LogStatusFailed)
	if err != nil {
		return agg, fmt.Errorf("failed to get failed PKs: %w", err)
	}
	if len(failed) > 0 {
		preview := failed
		if len(preview) > 10 {
			preview = preview[:10]
		}
		return agg, fmt.Errorf(
			"job %q has %d root PKs marked 'failed' (log_status=3) by an earlier GoArchive release.\n\n"+
				"This release no longer marks rows failed (errors leave rows in a recoverable status), but these "+
				"legacy rows would otherwise be skipped forever.\n\n"+
				"To recover, first list the full set (error_message explains each failure):\n"+
				"       SELECT root_pk_id, error_message FROM %s WHERE log_status=3;\n"+
				"Then choose per PK - substitute the PKs you decided on for each option, and run each\n"+
				"statement only against its own subset (the IN list is what keeps this per PK):\n"+
				"  1. Re-queue it for processing:\n"+
				"       UPDATE %s SET log_status=0 WHERE log_status=3 AND root_pk_id IN ('<pk>', ...);\n"+
				"  2. Skip it permanently: exclude the PK in the job's 'where' clause (e.g. AND id NOT IN (...))\n"+
				"     or resolve the source row manually, then clear the marker:\n"+
				"       UPDATE %s SET log_status=2 WHERE log_status=3 AND root_pk_id IN ('<pk>', ...);\n"+
				"     NOTE: the status edit alone does NOT skip the row - the forward scan re-fetches any\n"+
				"     source row above the checkpoint regardless of log status.\n"+
				"Then re-run.\n\n"+
				"Failed PKs (first 10): %v",
			p.jobName, len(failed), p.resumeMgr.LogTableName(), p.resumeMgr.LogTableName(),
			p.resumeMgr.LogTableName(), preview)
	}

	copied, err := p.resumeMgr.GetRootPKsByStatus(ctx, p.jobName, LogStatusCopied)
	if err != nil {
		return agg, fmt.Errorf("failed to get copied PKs: %w", err)
	}
	pending, err := p.resumeMgr.GetPendingPKs(ctx, p.jobName)
	if err != nil {
		return agg, fmt.Errorf("failed to get pending PKs: %w", err)
	}
	if len(copied) == 0 && len(pending) == 0 {
		return agg, nil
	}

	// Gate 2 (archive): count-mode cannot safely re-derive ANY non-terminal
	// rows — pre-existing destination rows cannot be verified equal to source.
	if p.mainMode == batchFull && p.countMode {
		total := len(copied) + len(pending)
		preview := append(append([]string{}, copied...), pending...)
		if len(preview) > 10 {
			preview = preview[:10]
		}
		return agg, fmt.Errorf(
			"job %q has %d non-terminal root PKs (copied/pending) from a prior interrupted run, and is configured with verification.method: count.\n\n"+
				"Resuming a count-mode job is unsafe - pre-existing destination rows cannot be verified equal to source.\n\n"+
				"To recover, choose one:\n"+
				"  1. Switch this job to verification.method: sha256 in config and re-run (recommended).\n"+
				"  2. Manually inspect destination rows for these PKs, delete any that don't match source, then clear the entries:\n"+
				"       UPDATE %s SET log_status=2 WHERE log_status IN (0,1);\n"+
				"     and re-run.\n\n"+
				"PKs (first 10): %v",
			p.jobName, total, p.resumeMgr.LogTableName(), preview)
	}

	// Gate 3 (archive + copy-only): strict INSERT cannot re-copy 'pending'
	// rows — their destination copy may already be committed, so a strict
	// re-INSERT aborts on duplicate and the job would self-block on every
	// resume. 'copied' rows are safe (no re-copy), so refuse only on pending.
	if p.copyPhase != nil && p.copyPhase.StrictInsert() && len(pending) > 0 {
		preview := pending
		if len(preview) > 10 {
			preview = preview[:10]
		}
		recovery := "  1. Delete the destination rows already written for these pending PKs, then re-run " +
			"(the strict re-copy then inserts cleanly).\n" +
			"  2. If you have confirmed the destination rows match source, mark them copied so they " +
			"resume as delete-only:\n" +
			fmt.Sprintf("       UPDATE %s SET log_status=1 WHERE log_status=0;\n", p.resumeMgr.LogTableName()) +
			"     and re-run."
		if p.mainMode == batchCopyVerify {
			recovery = "  1. Delete the destination rows already written for these pending PKs, then re-run.\n" +
				"  2. If using --skip-verify, drop it (and use verification.method: sha256) so replay uses " +
				"idempotent INSERT IGNORE, then re-run."
		}
		return agg, fmt.Errorf(
			"job %q has %d 'pending' root PKs from a prior interrupted run and uses strict INSERT "+
				"(forced by verification.method: count, --skip-verify, or a destination secondary unique index), "+
				"so they cannot be safely re-copied (their destination rows may already be committed, and a "+
				"strict INSERT aborts on duplicate).\n\n"+
				"To recover, choose one:\n%s\n\n"+
				"Pending PKs (first 10): %v",
			p.jobName, len(pending), recovery, preview)
	}

	// Copy-only: one merged, globally ascending schedule across BOTH statuses.
	// Copy-only recovery advances the checkpoint per chunk, so copied and
	// pending must NOT run as two independent phases: with copied={10},
	// pending={9}, promote-first would advance the checkpoint to 10 while 9 is
	// still pending, then regress it to 9 — violating the never-past-
	// non-terminal invariant and re-opening completed roots to the forward
	// scan. The merged schedule keeps the checkpoint monotonic: when any
	// chunk's checkpoint commits, every lower PK is already terminal.
	if p.mainMode == batchCopyVerify {
		_, unsigned, ok := p.graph.GetRootPKMeta()
		if !ok {
			return agg, fmt.Errorf("root PK metadata not loaded")
		}
		for _, run := range mergeRecoverySchedule(copied, pending, unsigned) {
			if run.copied {
				if err := p.recoverChunks(ctx, run.pks, batchPromote, true, checkpoint, agg); err != nil {
					return agg, fmt.Errorf("copied promotion failed: %w", err)
				}
			} else {
				if err := p.recoverChunks(ctx, run.pks, batchCopyVerify, true, checkpoint, agg); err != nil {
					return agg, fmt.Errorf("pending recovery failed: %w", err)
				}
			}
		}
		return agg, nil
	}

	// Archive/purge: copied first (delete-only), then pending (full pipeline),
	// nil checkpoint throughout — recovered source rows are deleted, so the
	// forward scan cannot re-fetch them and ordering across statuses is free.
	if err := p.recoverChunks(ctx, copied, batchDeleteOnly, false, checkpoint, agg); err != nil {
		return agg, fmt.Errorf("copied recovery failed: %w", err)
	}
	if err := p.recoverChunks(ctx, pending, p.mainMode, false, checkpoint, agg); err != nil {
		return agg, fmt.Errorf("pending recovery failed: %w", err)
	}
	return agg, nil
}

// recoveryRun is a maximal run of same-status PKs within the merged,
// numerically ascending copy-only recovery schedule.
type recoveryRun struct {
	copied bool
	pks    []string
}

// mergeRecoverySchedule merges the copied and pending PK sets into one
// globally ascending schedule, segmented into maximal same-status runs.
// Processing runs in order (promote for copied, full pipeline for pending)
// keeps the per-chunk checkpoint advance monotonic across statuses.
func mergeRecoverySchedule(copied, pending []string, unsigned bool) []recoveryRun {
	type entry struct {
		pk     string
		copied bool
	}
	entries := make([]entry, 0, len(copied)+len(pending))
	for _, pk := range copied {
		entries = append(entries, entry{pk: pk, copied: true})
	}
	for _, pk := range pending {
		entries = append(entries, entry{pk: pk, copied: false})
	}
	sort.Slice(entries, func(i, j int) bool {
		return numericPKLess(entries[i].pk, entries[j].pk, unsigned)
	})
	var runs []recoveryRun
	for _, e := range entries {
		if len(runs) == 0 || runs[len(runs)-1].copied != e.copied {
			runs = append(runs, recoveryRun{copied: e.copied})
		}
		runs[len(runs)-1].pks = append(runs[len(runs)-1].pks, e.pk)
	}
	return runs
}
