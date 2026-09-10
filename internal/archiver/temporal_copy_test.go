package archiver

import (
	"context"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/types"
	"regexp"
	"strings"
	"testing"
)

func copyIdentityCase(t *testing.T, kind string) {
	t.Helper()
	src, sm := diagnosticMock(t)
	dst, dm := diagnosticMock(t)
	g := graph.NewGraph("vp_keys", "d")
	cp, err := NewCopyPhase(src, dst, g, config.SafetyConfig{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	cp.SetColumnMetadata(map[string]types.ColumnMetadata{"vp_keys": {Names: []string{"d", "payload"}, Temporal: map[string]types.TemporalKind{"d": types.TemporalDate}}})
	dm.ExpectBegin()
	tx, err := dst.Begin()
	if err != nil {
		t.Fatal(err)
	}
	rows := sqlmock.NewRows([]string{"d", "payload"})
	cause := errors.New("iteration sentinel")
	switch kind {
	case "wrong":
		rows.AddRow("2020-03-03", "x")
	case "duplicate":
		rows.AddRow("2020-03-02", "x").AddRow("2020-03-02", "x")
	case "iteration":
		rows.AddRow("2020-03-02", "x").RowError(0, cause)
	}
	sm.ExpectQuery(regexp.QuoteMeta("SELECT CAST(`d` AS CHAR) AS `d`, `payload` FROM `vp_keys` WHERE `d` IN (?)")).WithArgs("2020-03-02").WillReturnRows(rows)
	_, err = cp.copyChunk(context.Background(), tx, "vp_keys", []interface{}{"2020-03-02"}, &copyOperation{})
	if err == nil || !strings.Contains(err.Error(), "TEMPORAL_READ_CONTRACT") {
		t.Fatalf("copy accepted missing/wrong requested identity: %v", err)
	}
	if kind == "iteration" && !errors.Is(err, cause) {
		t.Fatal(err)
	}
	dm.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}
func TestCopyTemporalIdentityZeroRows(t *testing.T)       { copyIdentityCase(t, "zero") }
func TestCopyTemporalIdentityWrongRow(t *testing.T)       { copyIdentityCase(t, "wrong") }
func TestCopyTemporalIdentityDuplicateRow(t *testing.T)   { copyIdentityCase(t, "duplicate") }
func TestCopyTemporalIdentityIterationError(t *testing.T) { copyIdentityCase(t, "iteration") }
