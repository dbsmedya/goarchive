package archiver

import (
	"context"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/types"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

func discoveryFixture(t *testing.T, temporal bool) (*RecordDiscovery, sqlmock.Sqlmock) {
	t.Helper()
	db, m := diagnosticMock(t)
	pk := "id"
	kind := map[string]types.TemporalKind{}
	if temporal {
		pk = "d"
		kind[pk] = types.TemporalDate
	}
	g, err := graph.BuildFromJob(&config.JobConfig{RootTable: "vp_roots", PrimaryKey: "id", Relations: []config.Relation{{Table: "vp_keys", PrimaryKey: pk, ForeignKey: "root_id"}}})
	if err != nil {
		t.Fatal(err)
	}
	d, err := NewRecordDiscovery(g, db, 100)
	if err != nil {
		t.Fatal(err)
	}
	d.SetColumnMetadata(map[string]types.ColumnMetadata{"vp_roots": {Names: []string{"id"}, Temporal: map[string]types.TemporalKind{}}, "vp_keys": {Names: []string{pk, "root_id"}, Temporal: kind}})
	return d, m
}
func TestDiscoveryTemporalRawIdentity(t *testing.T) {
	for _, key := range []string{"2020-02-29", "2020-02-31"} {
		t.Run(key, func(t *testing.T) {
			d, m := discoveryFixture(t, true)
			m.ExpectQuery(regexp.QuoteMeta("SELECT CAST(`d` AS CHAR) AS `d` FROM `vp_keys` WHERE `root_id` IN (?)")).WithArgs(int64(1)).WillReturnRows(sqlmock.NewRows([]string{"d"}).AddRow([]byte(key)))
			got, err := d.fetchChildIDs(context.Background(), "vp_roots", "vp_keys", []interface{}{int64(1)})
			if key == "2020-02-31" {
				if err == nil || !strings.Contains(err.Error(), "TEMPORAL_READ_CONTRACT") {
					t.Fatalf("expected TEMPORAL_READ_CONTRACT, got %v", err)
				}
			} else if err != nil || !reflect.DeepEqual(got, []interface{}{"2020-02-29"}) {
				t.Fatalf("raw key representation=%#v err=%v", got, err)
			}
		})
	}
}
func TestDiscoveryIntegerIdentityControl(t *testing.T) {
	d, m := discoveryFixture(t, false)
	m.ExpectQuery(regexp.QuoteMeta("SELECT `id` FROM `vp_keys` WHERE `root_id` IN (?)")).WithArgs(int64(1)).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(7)))
	got, err := d.fetchChildIDs(context.Background(), "vp_roots", "vp_keys", []interface{}{int64(1)})
	if err != nil || !reflect.DeepEqual(got, []interface{}{int64(7)}) {
		t.Fatalf("integer discovery changed: want child 7 and native SELECT: %v %v", got, err)
	}
}
