package verifier

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/goarchive/internal/types"
)

func temporalErrorsInHashChain(err error) []*types.TemporalReadContractError {
	var found []*types.TemporalReadContractError
	for current := err; current != nil; current = errors.Unwrap(current) {
		if temporalErr, ok := current.(*types.TemporalReadContractError); ok {
			found = append(found, temporalErr)
		}
	}
	return found
}

func hashReadQuery(metadata types.ColumnMetadata) string {
	if metadata.Kind("d") != types.TemporalNone {
		return "SELECT `id`, CAST(`d` AS CHAR) AS `d` FROM `vp_keys` WHERE `id` IN (?) ORDER BY `id`"
	}
	return "SELECT `id`, `n` FROM `vp_keys` WHERE `id` IN (?) ORDER BY `id`"
}

func TestHashNonTemporalReadErrors(t *testing.T) {
	metadataCases := []struct {
		name     string
		metadata types.ColumnMetadata
	}{
		{
			name:     "plain",
			metadata: types.ColumnMetadata{Names: []string{"id", "n"}, Temporal: map[string]types.TemporalKind{}},
		},
		{
			name:     "payload",
			metadata: types.ColumnMetadata{Names: []string{"id", "d"}, Temporal: map[string]types.TemporalKind{"d": types.TemporalDate}},
		},
	}
	for _, side := range []string{"source", "destination"} {
		t.Run(side, func(t *testing.T) {
			for _, metadataCase := range metadataCases {
				t.Run(metadataCase.name, func(t *testing.T) {
					for _, stage := range []string{"query", "scan", "iteration"} {
						t.Run(stage, func(t *testing.T) {
							v, sourceMock, destinationMock := temporalVerifier(t, false)
							v.SetColumnMetadata(map[string]types.ColumnMetadata{"vp_keys": metadataCase.metadata})
							db, mock := v.source, sourceMock
							if side == "destination" {
								db, mock = v.destination, destinationMock
							}
							cause := errors.New(stage + " sentinel")
							expectation := mock.ExpectQuery(regexp.QuoteMeta(hashReadQuery(metadataCase.metadata))).WithArgs(int64(1))
							switch stage {
							case "query":
								expectation.WillReturnError(cause)
							case "scan":
								expectation.WillReturnRows(sqlmock.NewRows([]string{"only"}).AddRow(1))
							case "iteration":
								expectation.WillReturnRows(sqlmock.NewRows(metadataCase.metadata.Names).AddRow(1, 7).RowError(0, cause))
							}

							hash, count, err := v.computeTableHash(context.Background(), db, "vp_keys", []interface{}{int64(1)})
							if hash != "" || count != 0 || err == nil {
								t.Fatalf("read failure result: hash=%q count=%d err=%v", hash, count, err)
							}
							if len(temporalErrorsInHashChain(err)) != 0 || strings.Contains(err.Error(), "TEMPORAL_READ_CONTRACT") {
								t.Fatalf("HASH_IO_LABEL: ordinary read failure has temporal identifier: %v", err)
							}
							stageContext := map[string]string{
								"query":     "query failed",
								"scan":      "failed to scan row",
								"iteration": "error iterating rows",
							}[stage]
							if !strings.Contains(err.Error(), "vp_keys") || !strings.Contains(err.Error(), stageContext) {
								t.Fatalf("hash table/stage context missing: %v", err)
							}
							if stage == "scan" {
								if !strings.Contains(err.Error(), "sql: expected 1 destination arguments in Scan, not 2") {
									t.Fatalf("scan cause missing: %v", err)
								}
							} else if !errors.Is(err, cause) {
								t.Fatalf("hash %s cause missing: %v", stage, err)
							}
							for name, handleMock := range map[string]sqlmock.Sqlmock{"source": sourceMock, "destination": destinationMock} {
								if mockErr := handleMock.ExpectationsWereMet(); mockErr != nil {
									t.Fatalf("%s expectations: %v", name, mockErr)
								}
							}
						})
					}
				})
			}
		})
	}
}

func TestHashTemporalIdentityErrorContext(t *testing.T) {
	for _, side := range []string{"source", "destination"} {
		t.Run(side, func(t *testing.T) {
			v, sourceMock, destinationMock := temporalVerifier(t, true)
			db, mock := v.source, sourceMock
			if side == "destination" {
				db, mock = v.destination, destinationMock
			}
			query := "SELECT CAST(`d` AS CHAR) AS `d`, `payload` FROM `vp_keys` WHERE `d` IN (?) ORDER BY `d`"
			mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs("2020-03-02").WillReturnRows(sqlmock.NewRows([]string{"d", "payload"}))

			hash, count, err := v.computeTableHash(context.Background(), db, "vp_keys", []interface{}{"2020-03-02"})
			if hash != "" || count != 0 || err == nil {
				t.Fatalf("identity failure result: hash=%q count=%d err=%v", hash, count, err)
			}
			temporalErrors := temporalErrorsInHashChain(err)
			if len(temporalErrors) != 1 || temporalErrors[0].Table != "vp_keys" || temporalErrors[0].Operation != "verify" || temporalErrors[0].Detail != "requested=1 fetched=0" || temporalErrors[0].Cause != nil {
				t.Fatalf("HASH_IDENTITY_REWRAPPED: identity failure has redundant temporal wrapper: %v", err)
			}
			for name, handleMock := range map[string]sqlmock.Sqlmock{"source": sourceMock, "destination": destinationMock} {
				if mockErr := handleMock.ExpectationsWereMet(); mockErr != nil {
					t.Fatalf("%s expectations: %v", name, mockErr)
				}
			}
		})
	}
}

func TestHashTemporalPayloadErrorContext(t *testing.T) {
	for _, side := range []string{"source", "destination"} {
		t.Run(side, func(t *testing.T) {
			v, sourceMock, destinationMock := temporalVerifier(t, false)
			db, mock := v.source, sourceMock
			if side == "destination" {
				db, mock = v.destination, destinationMock
			}
			query := "SELECT `id`, CAST(`d` AS CHAR) AS `d` FROM `vp_keys` WHERE `id` IN (?) ORDER BY `id`"
			mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(int64(1)).WillReturnRows(
				sqlmock.NewRows([]string{"id", "d"}).AddRow(1, time.Date(2020, time.March, 2, 0, 0, 0, 0, time.UTC)),
			)

			hash, count, err := v.computeTableHash(context.Background(), db, "vp_keys", []interface{}{int64(1)})
			if hash != "" || count != 0 || err == nil {
				t.Fatalf("temporal payload refusal missing: hash=%q count=%d err=%v", hash, count, err)
			}
			temporalErrors := temporalErrorsInHashChain(err)
			if len(temporalErrors) < 2 || temporalErrors[0].Table != "vp_keys" || temporalErrors[0].Operation != "verify" {
				t.Fatalf("hash temporal context missing: %v", err)
			}
			foundRawRead := false
			for _, temporalErr := range temporalErrors {
				if temporalErr.Operation == "raw-read" && strings.Contains(temporalErr.Detail, "received time.Time") {
					foundRawRead = true
				}
			}
			if !foundRawRead {
				t.Fatalf("raw-read representation refusal missing: %v", err)
			}
			for name, handleMock := range map[string]sqlmock.Sqlmock{"source": sourceMock, "destination": destinationMock} {
				if mockErr := handleMock.ExpectationsWereMet(); mockErr != nil {
					t.Fatalf("%s expectations: %v", name, mockErr)
				}
			}
		})
	}
}
