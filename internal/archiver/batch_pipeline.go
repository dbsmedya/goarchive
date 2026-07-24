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
// checkpoint pass nil.
//
// On any error the batch's rows keep their current non-terminal status
// (pending or copied) — NEVER failed — so status-aware recovery replays them.
func (p *batchPipeline) processBatch(ctx context.Context, rootIDs []interface{}, mode batchMode, checkpointPK interface{}, checkpoint CheckpointCallback) (*BatchStats, error) {
	stats := &BatchStats{}
	if len(rootIDs) == 0 {
		return stats, nil
	}

	discovered, err := p.discovery.Discover(ctx, rootIDs)
	if err != nil {
		return stats, fmt.Errorf("discovery failed: %w", err)
	}
	recordSet := convertRecordSet(discovered)

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
//   - true (copy-only recovery): the chunk's max PK — copy-only source rows
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
	p.logger.Infow("Recovering non-terminal PKs from prior run",
		"job", p.jobName, "count", len(rawPKs), "mode", mode)

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
