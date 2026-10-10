package engine

import (
	"context"
	"fmt"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
)

// tree is one batch's discovered key set, by table: the one set that copy, verification
// and delete use (spec §7.3).
type tree struct {
	keys map[string][]key.Key
}

// discover walks the job's tables parent first from roots. A child's rows are found by its
// foreign_key columns, with each parent tuple projected into references order (spec §7.3,
// normative); child keys are de-duplicated by their exact bytes (spec §4.2 rule 2). A child
// row with a NULL in a foreign_key column matches no tuple, so it is never discovered.
func (r *run) discover(ctx context.Context, src queryer, roots []key.Key) (*tree, error) {
	t := &tree{keys: map[string][]key.Key{r.root.Table: roots}}
	for _, n := range r.tree.CopyOrder() {
		if n.Parent == nil {
			continue
		}
		keys, err := r.discoverChild(ctx, src, n, t.keys[n.Parent.Table])
		if err != nil {
			return nil, err
		}
		t.keys[n.Table] = keys
	}
	return t, nil
}

func (r *run) discoverChild(ctx context.Context, src queryer, n *jobspec.Node, parents []key.Key) ([]key.Key, error) {
	parent, child := r.keys[n.Parent.Table], r.keys[n.Table]
	idx, err := n.Projection(parent.Names())
	if err != nil {
		return nil, fmt.Errorf("engine: discovery of %s: %w", n.Table, err)
	}
	bind := jobspec.ProjectColumns(parent.Columns, idx)
	found := key.NewSet()
	for _, chunk := range key.Chunks(parents, r.discoveryChunk(n.Table)) {
		projected := make([]key.Key, len(chunk))
		for i, k := range chunk {
			projected[i] = jobspec.Project(k, idx)
		}
		keys, err := r.queryKeys(ctx, src, child, statement{
			op: opDiscover, table: n.Table, query: discoverySQL(child, n, bind, len(chunk)), args: key.Args(projected),
		})
		if err != nil {
			return nil, err
		}
		for _, k := range keys {
			found.Add(k)
		}
	}
	return found.Keys(), nil
}
