package verifier

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/types"
	"regexp"
	"strings"
	"testing"
)

func temporalVerifier(t *testing.T, key bool) (*Verifier, sqlmock.Sqlmock, sqlmock.Sqlmock) {
	t.Helper()
	src, sm, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	dst, dm, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = src.Close(); _ = dst.Close() })
	pk := "id"
	names := []string{"id", "d"}
	if key {
		pk = "d"
		names = []string{"d", "payload"}
	}
	v, err := NewVerifier(src, dst, graph.NewGraph("vp_keys", pk), MethodSHA256, nil)
	if err != nil {
		t.Fatal(err)
	}
	v.SetColumnMetadata(map[string]types.ColumnMetadata{"vp_keys": {Names: names, Temporal: map[string]types.TemporalKind{"d": types.TemporalDate}}})
	return v, sm, dm
}
func programTemporalRead(m sqlmock.Sqlmock, method VerificationMethod, present bool) {
	query := "SELECT CAST(`d` AS CHAR) AS `d` FROM `vp_keys` WHERE `d` IN (?)"
	rows := sqlmock.NewRows([]string{"d"})
	if method == MethodSHA256 {
		query = "SELECT CAST(`d` AS CHAR) AS `d`, `payload` FROM `vp_keys` WHERE `d` IN (?) ORDER BY `d`"
		rows = sqlmock.NewRows([]string{"d", "payload"})
		if present {
			rows.AddRow("2020-03-02", nil)
		}
	} else if present {
		rows.AddRow("2020-03-02")
	}
	m.ExpectQuery(regexp.QuoteMeta(query)).WithArgs("2020-03-02").WillReturnRows(rows)
}
func verifyIdentitySide(t *testing.T, side string, bothEmpty bool) {
	t.Helper()
	for _, method := range []VerificationMethod{MethodCount, MethodSHA256} {
		t.Run(string(method), func(t *testing.T) {
			v, sm, dm := temporalVerifier(t, true)
			programTemporalRead(sm, method, side != "source" && !bothEmpty)
			programTemporalRead(dm, method, side != "destination" && !bothEmpty)
			var err error
			if method == MethodCount {
				_, err = v.verifyByCount(context.Background(), "vp_keys", []interface{}{"2020-03-02"})
			} else {
				_, err = v.verifyBySHA256(context.Background(), "vp_keys", []interface{}{"2020-03-02"})
			}
			if err == nil || !strings.Contains(err.Error(), "TEMPORAL_READ_CONTRACT") {
				t.Fatalf("%s temporal identity check missing: %v", side, err)
			}
			if err := sm.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
			if side == "destination" && !bothEmpty {
				if err := dm.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
func TestVerifyTemporalSourceIdentity(t *testing.T)      { verifyIdentitySide(t, "source", false) }
func TestVerifyTemporalDestinationIdentity(t *testing.T) { verifyIdentitySide(t, "destination", false) }
func TestVerifyTemporalBothSidesEmpty(t *testing.T)      { verifyIdentitySide(t, "source", true) }
func verifyRawProjection(t *testing.T) {
	t.Helper()
	v, sm, dm := temporalVerifier(t, false)
	q := "SELECT `id`, CAST(`d` AS CHAR) AS `d` FROM `vp_keys` WHERE `id` IN (?) ORDER BY `id`"
	sm.ExpectQuery(regexp.QuoteMeta(q)).WithArgs(int64(1)).WillReturnRows(sqlmock.NewRows([]string{"id", "d"}).AddRow(1, []byte("2020-03-02")))
	dm.ExpectQuery(regexp.QuoteMeta(q)).WithArgs(int64(1)).WillReturnRows(sqlmock.NewRows([]string{"id", "d"}).AddRow(1, "2020-02-31"))
	result, err := v.verifyBySHA256(context.Background(), "vp_keys", []interface{}{int64(1)})
	if err != nil || result.Match {
		t.Errorf("raw temporal projection did not distinguish collision: result=%+v err=%v", result, err)
	}
	if err := sm.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
	if err := dm.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
func TestVerifyTemporalRawHashCollision(t *testing.T)      { verifyRawProjection(t) }
func TestVerifyTemporalSourceProjection(t *testing.T)      { verifyRawProjection(t) }
func TestVerifyTemporalDestinationProjection(t *testing.T) { verifyRawProjection(t) }
func TestVerifyTemporalValidKeys(t *testing.T) {
	for _, method := range []VerificationMethod{MethodCount, MethodSHA256} {
		t.Run(string(method), func(t *testing.T) {
			v, sm, dm := temporalVerifier(t, true)
			programTemporalRead(sm, method, true)
			programTemporalRead(dm, method, true)
			var r *VerifyResult
			var err error
			if method == MethodCount {
				r, err = v.verifyByCount(context.Background(), "vp_keys", []interface{}{"2020-03-02"})
			} else {
				r, err = v.verifyBySHA256(context.Background(), "vp_keys", []interface{}{"2020-03-02"})
			}
			if err != nil || !r.Match {
				t.Fatalf("valid temporal identity refused: %+v %v", r, err)
			}
			for _, m := range []sqlmock.Sqlmock{sm, dm} {
				if err := m.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
func TestVerifyNonTemporalControl(t *testing.T) {
	v, sm, dm := temporalVerifier(t, false)
	v.SetColumnMetadata(map[string]types.ColumnMetadata{"vp_keys": {Names: []string{"id", "n"}, Temporal: map[string]types.TemporalKind{}}})
	for _, m := range []sqlmock.Sqlmock{sm, dm} {
		m.ExpectQuery(regexp.QuoteMeta("SELECT `id`, `n` FROM `vp_keys` WHERE `id` IN (?) ORDER BY `id`")).WithArgs(int64(1)).WillReturnRows(sqlmock.NewRows([]string{"id", "n"}).AddRow(1, 7))
	}
	r, err := v.verifyBySHA256(context.Background(), "vp_keys", []interface{}{int64(1)})
	if err != nil || !r.Match {
		t.Fatal(r, err)
	}
	for _, m := range []sqlmock.Sqlmock{sm, dm} {
		if err := m.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	}
}
func TestVerifyTemporalReadFailures(t *testing.T) {
	for _, method := range []VerificationMethod{MethodCount, MethodSHA256} {
		t.Run(string(method), func(t *testing.T) {
			v, sm, _ := temporalVerifier(t, true)
			sm.ExpectQuery("SELECT").WillReturnError(sql.ErrConnDone)
			var err error
			if method == MethodCount {
				_, err = v.verifyByCount(context.Background(), "vp_keys", []interface{}{"2020-03-02"})
			} else {
				_, err = v.verifyBySHA256(context.Background(), "vp_keys", []interface{}{"2020-03-02"})
			}
			if err == nil || !strings.Contains(fmt.Sprint(err), "TEMPORAL_READ_CONTRACT") {
				t.Fatal(err)
			}
		})
	}
}
