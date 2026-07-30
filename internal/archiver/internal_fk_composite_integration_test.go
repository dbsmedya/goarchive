//go:build integration

package archiver

import (
	"testing"

	"github.com/dbsmedya/goarchive/internal/graph"
)

// TestInternalFKCompositeRejected proves a multi-column foreign key between two graph
// tables is rejected with a message that says why. 1.8 rejected it too, but by accident:
// information_schema returned one row per constraint column and at least one of them
// failed the single-column reconciliation.
//
// Two fixture constraints, both load-bearing:
//
//   - The table is `order_lines`, NOT `lines`. LINES is a MySQL reserved word and
//     `CREATE TABLE lines (…)` fails with Error 1064 at setup. Verified against the 8.4
//     test server; see INDEX.md's trap register.
//   - The child is added with chrAddChild, NOT a bare AddEdgeWithMeta. AddEdgeWithMeta
//     records the edge but does not create the node, so AllNodes() — and therefore
//     r.tables and the Within request — would omit order_lines and the check would find
//     nothing to reconcile.
//
// MySQL requires the parent side of a composite FK to be a key, while COMPOSITE_PK_CHECK
// (position 3) rejects composite PRIMARY keys in the graph. Hence a single-column PRIMARY
// KEY plus a composite UNIQUE key on the parent.
func TestInternalFKCompositeRejected(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	const parentDDL = "CREATE TABLE orders (" +
		"id bigint NOT NULL, tenant_id int NOT NULL, " +
		"PRIMARY KEY (id), UNIQUE KEY uq_id_tenant (id, tenant_id)) ENGINE=InnoDB"
	const childDDL = "CREATE TABLE order_lines (" +
		"id bigint NOT NULL, order_id bigint NOT NULL, tenant_id int NOT NULL, " +
		"PRIMARY KEY (id), KEY idx_ot (order_id, tenant_id), " +
		"CONSTRAINT fk_ol_o FOREIGN KEY (order_id, tenant_id) REFERENCES orders (id, tenant_id)) ENGINE=InnoDB"

	f.ExecSource(t, ctx, parentDDL)
	f.ExecSource(t, ctx, childDDL)
	f.ExecDest(t, ctx, parentDDL)
	f.ExecDest(t, ctx, childDDL)

	g := graph.NewGraph("orders", "id")
	chrAddChild(g, "orders", "id", "order_lines", "order_id", "id")

	err := chrRun(t, ctx, f.Checker(t, g), chrCommands[0], false) // archive
	chrAssertCheck(t, err, "INTERNAL_FK_COVERAGE", nil)
}
