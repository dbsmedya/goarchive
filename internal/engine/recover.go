package engine

import (
	"context"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
)

// recover finishes the work an interrupted run logged (spec §6.4), before the loop starts.
// Attach has already refused a binding that differs while such work is pending, so the
// replay runs under the config that wrote the log rows.
//
//   - archive: copied roots were copied and verified; their trees are re-discovered from
//     the source by key, checked against the destination, deleted child first, and the
//     roots marked completed;
//   - purge: deleting roots are replayed the same way, with no destination to check;
//   - copy-only: each batch was all or nothing, so there is nothing to recover.
//
// A root whose own row is gone from the source was deleted before the crash, after its
// children (deletes run child first), so only the roots still present are deleted again;
// their children are re-discovered from every logged root.
func (r *run) recover(ctx context.Context) error {
	var st tracking.Status
	switch r.typ {
	case jobspec.Archive:
		st = tracking.StatusCopied
	case jobspec.Purge:
		st = tracking.StatusDeleting
	default:
		return nil
	}
	// The sentinel pause and a first signal take effect at a page boundary, as they do at a
	// batch boundary: the rest stays logged for the next run (M5-F3, M6-F1). Each page reads
	// the log on its own connection, after the gate, so a pause holds no idle session that
	// the server's wait_timeout could close under it.
	sentinel := newSentinelGate(r.cfg.Processing.SentinelFile, r.log)
	var after key.Key
	for {
		if err := sentinel.wait(ctx, r.cfg.Stop); err != nil {
			return err
		}
		if stopRequested(r.cfg.Stop) {
			r.report.Stopped = true
			return nil
		}
		roots, err := r.loggedPage(ctx, st, after)
		if err != nil {
			return err
		}
		if len(roots) == 0 {
			return nil
		}
		after = roots[len(roots)-1]
		r.log.Infof("Recovery: replaying %d %s roots of %s", len(roots), st, r.root.Table)
		if err := r.replay(ctx, st, roots); err != nil {
			return err
		}
		r.report.Recovered += int64(len(roots))
		if len(roots) < r.cfg.Processing.BatchSize {
			return nil
		}
	}
}

// loggedPage reads the next page of logged roots in status st after the key after, on a
// connection of its own that it returns before the page is replayed.
func (r *run) loggedPage(ctx context.Context, st tracking.Status, after key.Key) ([]key.Key, error) {
	conn, err := r.cfg.Destination.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	return r.job.Roots(ctx, conn, st, after, r.cfg.Processing.BatchSize)
}

func (r *run) replay(ctx context.Context, st tracking.Status, roots []key.Key) error {
	src, err := r.cfg.Source.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()
	present, err := r.present(ctx, src, roots)
	if err != nil {
		return err
	}
	gone := without(roots, present)
	if len(gone) > 0 {
		// A crash after a root's delete committed leaves it logged; so does a root deleted
		// from outside, which delete-only replay cannot tell apart (spec §6.4, GH #7).
		r.log.Warnf("Recovery: %d logged roots of %s were already gone from the source; their trees are re-checked and the roots marked completed", len(gone), r.root.Table)
	}
	t, err := r.discover(ctx, src, roots)
	if err != nil {
		return err
	}
	t.keys[r.root.Table] = present
	if r.typ == jobspec.Archive {
		// Recovery deletes only what the destination holds (GH #7): every table is read
		// before the first delete, and a page with a missing row is refused whole.
		if err := r.checkHeld(ctx, t, gone); err != nil {
			return err
		}
	}
	if err := r.deleteTree(ctx, src, t); err != nil {
		return err
	}
	return r.complete(ctx, st, roots)
}

// present returns the logged roots still in the source, in the order logged.
func (r *run) present(ctx context.Context, src queryer, roots []key.Key) ([]key.Key, error) {
	return r.members(ctx, src, r.root, roots, opPresent, false)
}

// members returns the keys of d's table, among keys, that q holds, in the order given. A
// returned key that was not requested, or repeats one, fails with *KeySetError (spec §7.1a).
func (r *run) members(ctx context.Context, q queryer, d key.Def, keys []key.Key, o op, destination bool) ([]key.Key, error) {
	requested := key.NewSet()
	for _, k := range keys {
		requested.Add(k)
	}
	found := key.NewSet()
	var extra int64
	for _, chunk := range key.Chunks(keys, r.byKeyChunk(d.Table)) {
		got, err := r.queryKeys(ctx, q, d, statement{
			op: o, destination: destination, table: d.Table, query: byKeySQL(d, len(chunk)), args: key.Args(chunk),
		})
		if err != nil {
			return nil, err
		}
		for _, k := range got {
			if !requested.Has(k) || !found.Add(k) {
				extra++
			}
		}
	}
	if extra > 0 {
		return nil, &KeySetError{Table: d.Table, Statement: string(o), Requested: int64(len(requested.Keys())),
			Matched: int64(len(found.Keys())), Extra: extra, Destination: destination}
	}
	var out []key.Key
	for _, k := range keys {
		if found.Has(k) {
			out = append(out, k)
		}
	}
	return out, nil
}

// checkHeld is archive recovery's evidence check (GH #7). Before any delete of a page it
// reads, table by table, which of the re-discovered keys the destination holds, and refuses
// the page with *UncopiedError when one is missing: a row written into a copied root's tree
// after its copy, or one the destination lost. gone are the page's logged roots already
// deleted from the source; they are not deleted again, and one the destination lacks as well
// exists on neither server, so it is warned about, not refused. The destination connection
// is released before the page's deletes.
func (r *run) checkHeld(ctx context.Context, t *tree, gone []key.Key) error {
	dst, err := r.cfg.Destination.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = dst.Close() }()
	var missing []MissingRows
	for _, n := range r.tree.CopyOrder() {
		keys := t.keys[n.Table]
		held, err := r.members(ctx, dst, r.keys[n.Table], keys, opHeld, true)
		if err != nil {
			return err
		}
		if lack := without(keys, held); len(lack) > 0 {
			missing = append(missing, MissingRows{Table: n.Table, Root: n.Parent == nil, Count: int64(len(lack)), Keys: formatKeys(lack)})
		}
	}
	if len(gone) > 0 {
		held, err := r.members(ctx, dst, r.root, gone, opHeld, true)
		if err != nil {
			return err
		}
		if lost := without(gone, held); len(lost) > 0 {
			r.log.Warnf("Recovery: %d logged roots of %s are gone from both the source and the destination: their rows exist on neither server (%s)",
				len(lost), r.root.Table, formatKeys(lost))
		}
	}
	if len(missing) > 0 {
		return &UncopiedError{Missing: missing}
	}
	return nil
}

// without returns the keys of all that are not in some, in the order of all.
func without(all, some []key.Key) []key.Key {
	drop := key.NewSet()
	for _, k := range some {
		drop.Add(k)
	}
	var out []key.Key
	for _, k := range all {
		if !drop.Has(k) {
			out = append(out, k)
		}
	}
	return out
}

// dropLogged removes the roots this copy-only job already completed (spec §6.6, resume:
// true). The log's key columns have the root key's exact type and collation and were
// written from the source's own bytes, so a logged root's bytes equal the batch root's: Go
// compares them byte for byte, the one comparison spec §4.2 rule 2 sanctions. Under the
// cold-data contract (spec §3) a copied key never changes in the source; if one did, its
// root is not skipped and the copy fails loudly on the duplicate (§4.2 rule 2).
func (r *run) dropLogged(ctx context.Context, roots []key.Key) ([]key.Key, error) {
	conn, err := r.cfg.Destination.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	logged, err := r.job.Logged(ctx, conn, tracking.StatusCompleted, roots)
	if err != nil {
		return nil, err
	}
	if len(logged) == 0 {
		return roots, nil
	}
	keep := without(roots, logged)
	skipped := len(roots) - len(keep)
	r.report.Skipped += int64(skipped)
	if skipped > 0 {
		r.log.Infof("Resume: skipped %d roots of %s this job already copied", skipped, r.root.Table)
	}
	return keep, nil
}
