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
	"time"
)

func deleteFixture(t *testing.T, temporal bool) (*DeletePhase, sqlmock.Sqlmock, *RecordSet) {
	t.Helper()
	db, m := diagnosticMock(t)
	pk := "id"
	kind := map[string]types.TemporalKind{}
	key := interface{}(int64(8))
	if temporal {
		pk = "d"
		kind[pk] = types.TemporalDate
		key = "2020-03-02"
	}
	g, err := graph.BuildFromJob(&config.JobConfig{RootTable: "vp_other", PrimaryKey: "id", Relations: []config.Relation{{Table: "vp_keys", PrimaryKey: pk, ForeignKey: "root_id"}}})
	if err != nil {
		t.Fatal(err)
	}
	dp, err := NewDeletePhase(db, g, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	dp.SetColumnMetadata(map[string]types.ColumnMetadata{"vp_other": {Names: []string{"id"}, Temporal: map[string]types.TemporalKind{}}, "vp_keys": {Names: []string{pk, "root_id"}, Temporal: kind}})
	return dp, m, &RecordSet{Records: map[string][]interface{}{"vp_other": {int64(7)}, "vp_keys": {key}}}
}
func deleteProbeRefusal(t *testing.T, count int64, queryErr error, two bool) {
	t.Helper()
	dp, m, rs := deleteFixture(t, true)
	q := "SELECT COUNT(*) FROM `vp_keys` WHERE `d` IN (?)"
	if two {
		rs.Records["vp_keys"] = append(rs.Records["vp_keys"], "2020-03-03")
		q = "SELECT COUNT(*) FROM `vp_keys` WHERE `d` IN (?,?)"
	}
	expect := m.ExpectQuery(regexp.QuoteMeta(q))
	if queryErr != nil {
		expect.WillReturnError(queryErr)
	} else {
		expect.WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(count))
	}
	_, err := dp.Delete(context.Background(), rs)
	if err == nil || !strings.Contains(err.Error(), "TEMPORAL_READ_CONTRACT") {
		t.Fatalf("pre-delete probe accepted zero for one requested identity: %v", err)
	}
	if queryErr != nil && !errors.Is(err, queryErr) {
		t.Fatal(err)
	}
}
func TestDeleteTemporalProbeZeroCount(t *testing.T)  { deleteProbeRefusal(t, 0, nil, false) }
func TestDeleteTemporalProbeWrongCount(t *testing.T) { deleteProbeRefusal(t, 1, nil, true) }
func TestDeleteTemporalProbeQueryError(t *testing.T) {
	deleteProbeRefusal(t, 0, errors.New("probe sentinel"), false)
}
func TestDeleteTemporalProbeBeforeAnyDelete(t *testing.T) {
	dp, m, rs := deleteFixture(t, true)
	dp.graph.AddNode("vp_keys2", &graph.Node{Name: "vp_keys2"})
	dp.graph.SetPK("vp_keys2", "d")
	dp.graph.AddEdgeWithMeta("vp_other", "vp_keys2", "root_id", "id", "1-N")
	dp.columnMetadata["vp_keys2"] = dp.columnMetadata["vp_keys"]
	rs.Records["vp_keys2"] = []interface{}{"2020-03-03"}
	order, err := dp.graph.DeleteOrder()
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, table := range order {
		if table == "vp_other" {
			continue
		}
		count := 1
		if seen == 1 {
			count = 0
		}
		m.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM `" + table + "` WHERE `d` IN (?)")).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(count))
		seen++
	}
	_, err = dp.Delete(context.Background(), rs)
	if err == nil || !strings.Contains(err.Error(), "TEMPORAL_READ_CONTRACT") {
		t.Fatalf("pre-delete probe accepted zero: %v", err)
	}
}
func TestDeleteTemporalProbeValid(t *testing.T) {
	dp, m, rs := deleteFixture(t, true)
	m.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM `vp_keys` WHERE `d` IN (?)")).WithArgs("2020-03-02").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	m.ExpectExec(regexp.QuoteMeta("DELETE FROM `vp_keys` WHERE `d` IN (?)")).WithArgs("2020-03-02").WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectExec(regexp.QuoteMeta("DELETE FROM `vp_other` WHERE `id` IN (?)")).WithArgs(int64(7)).WillReturnResult(sqlmock.NewResult(0, 1))
	stats, err := dp.Delete(context.Background(), rs)
	if err != nil || stats.RowsDeleted != 2 {
		t.Fatal(stats, err)
	}
}
func TestDeleteTemporalProbeReleasesConnection(t *testing.T) {
	dp, m, rs := deleteFixture(t, true)
	dp.db.SetMaxOpenConns(1)
	m.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	if _, err := dp.Delete(context.Background(), rs); err == nil {
		t.Fatal("missing refusal")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := dp.db.Conn(ctx)
	if err != nil {
		t.Fatalf("source connection leaked: %v", err)
	}
	_ = conn.Close()
}
func TestDeleteNonTemporalNoProbe(t *testing.T) {
	dp, m, rs := deleteFixture(t, false)
	m.ExpectExec(regexp.QuoteMeta("DELETE FROM `vp_keys` WHERE `id` IN (?)")).WithArgs(int64(8)).WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectExec(regexp.QuoteMeta("DELETE FROM `vp_other` WHERE `id` IN (?)")).WithArgs(int64(7)).WillReturnResult(sqlmock.NewResult(0, 1))
	stats, err := dp.Delete(context.Background(), rs)
	if err != nil || stats.RowsDeleted != 2 {
		t.Fatal(stats, err)
	}
}
