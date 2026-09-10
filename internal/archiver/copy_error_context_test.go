package archiver

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/types"
)

func temporalErrorsInChain(err error) []*types.TemporalReadContractError {
	var found []*types.TemporalReadContractError
	for current := err; current != nil; current = errors.Unwrap(current) {
		if temporalErr, ok := current.(*types.TemporalReadContractError); ok {
			found = append(found, temporalErr)
		}
	}
	return found
}

func newReadErrorCopy(t *testing.T, metadata types.ColumnMetadata) (*CopyPhase, sqlmock.Sqlmock, sqlmock.Sqlmock) {
	t.Helper()
	source, sourceMock := diagnosticMock(t)
	destination, destinationMock := diagnosticMock(t)
	cp, err := NewCopyPhase(source, destination, graph.NewGraph("vp_rows", "id"), config.SafetyConfig{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	cp.SetColumnMetadata(map[string]types.ColumnMetadata{"vp_rows": metadata})
	testCopyPolicy(cp)
	return cp, sourceMock, destinationMock
}

func expectReadErrorCopyTransaction(destinationMock sqlmock.Sqlmock) {
	expectDiagnosticSession(destinationMock)
	destinationMock.ExpectBegin()
	destinationMock.ExpectExec(regexp.QuoteMeta("SET FOREIGN_KEY_CHECKS = 1")).WillReturnResult(sqlmock.NewResult(0, 0))
	destinationMock.ExpectRollback()
}

func TestCopyNonTemporalReadErrors(t *testing.T) {
	metadataCases := []struct {
		name     string
		metadata types.ColumnMetadata
		query    string
	}{
		{
			name:     "plain",
			metadata: types.ColumnMetadata{Names: []string{"id", "n"}, Temporal: map[string]types.TemporalKind{}},
			query:    "SELECT `id`, `n` FROM `vp_rows` WHERE `id` IN (?)",
		},
		{
			name:     "payload",
			metadata: types.ColumnMetadata{Names: []string{"id", "d"}, Temporal: map[string]types.TemporalKind{"d": types.TemporalDate}},
			query:    "SELECT `id`, CAST(`d` AS CHAR) AS `d` FROM `vp_rows` WHERE `id` IN (?)",
		},
	}
	for _, metadataCase := range metadataCases {
		t.Run(metadataCase.name, func(t *testing.T) {
			for _, stage := range []string{"query", "scan", "iteration"} {
				t.Run(stage, func(t *testing.T) {
					cp, sourceMock, destinationMock := newReadErrorCopy(t, metadataCase.metadata)
					expectReadErrorCopyTransaction(destinationMock)
					cause := errors.New(stage + " sentinel")
					expectation := sourceMock.ExpectQuery(regexp.QuoteMeta(metadataCase.query)).WithArgs(int64(1))
					switch stage {
					case "query":
						expectation.WillReturnError(cause)
					case "scan":
						expectation.WillReturnRows(sqlmock.NewRows([]string{"only"}).AddRow(1))
					case "iteration":
						rows := sqlmock.NewRows(metadataCase.metadata.Names).AddRow(1, 7).RowError(0, cause)
						expectation.WillReturnRows(rows)
					}

					stats, err := cp.Copy(context.Background(), &RecordSet{
						RootPKs: []interface{}{int64(1)},
						Records: map[string][]interface{}{"vp_rows": {int64(1)}},
					})
					if stats != nil || err == nil {
						t.Fatalf("read failure result: stats=%+v err=%v", stats, err)
					}
					if len(temporalErrorsInChain(err)) != 0 || strings.Contains(err.Error(), "TEMPORAL_READ_CONTRACT") {
						t.Fatalf("COPY_IO_LABEL: ordinary read failure has temporal identifier: %v", err)
					}
					stageContext := map[string]string{
						"query":     "source query failed",
						"scan":      "row scan failed",
						"iteration": "row iteration failed",
					}[stage]
					if !strings.Contains(err.Error(), "vp_rows") || !strings.Contains(err.Error(), stageContext) {
						t.Fatalf("copy table/stage context missing: %v", err)
					}
					if stage == "scan" {
						if !strings.Contains(err.Error(), "sql: expected 1 destination arguments in Scan, not 2") {
							t.Fatalf("scan cause missing: %v", err)
						}
					} else if !errors.Is(err, cause) {
						t.Fatalf("copy %s cause missing: %v", stage, err)
					}
					if err := sourceMock.ExpectationsWereMet(); err != nil {
						t.Fatal(err)
					}
					if err := destinationMock.ExpectationsWereMet(); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}

func TestCopyTemporalPayloadErrorContext(t *testing.T) {
	metadata := types.ColumnMetadata{Names: []string{"id", "d"}, Temporal: map[string]types.TemporalKind{"d": types.TemporalDate}}
	cp, sourceMock, destinationMock := newReadErrorCopy(t, metadata)
	expectReadErrorCopyTransaction(destinationMock)
	query := "SELECT `id`, CAST(`d` AS CHAR) AS `d` FROM `vp_rows` WHERE `id` IN (?)"
	sourceMock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(int64(1)).WillReturnRows(
		sqlmock.NewRows([]string{"id", "d"}).AddRow(1, time.Date(2020, time.March, 2, 0, 0, 0, 0, time.UTC)),
	)

	stats, err := cp.Copy(context.Background(), &RecordSet{
		RootPKs: []interface{}{int64(1)},
		Records: map[string][]interface{}{"vp_rows": {int64(1)}},
	})
	if stats != nil || err == nil {
		t.Fatalf("temporal payload refusal missing: stats=%+v err=%v", stats, err)
	}
	temporalErrors := temporalErrorsInChain(err)
	if len(temporalErrors) < 2 || temporalErrors[0].Table != "vp_rows" || temporalErrors[0].Operation != "copy" {
		t.Fatalf("copy temporal context missing: %v", err)
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
	for name, mock := range map[string]sqlmock.Sqlmock{"source": sourceMock, "destination": destinationMock} {
		if mockErr := mock.ExpectationsWereMet(); mockErr != nil {
			t.Fatalf("%s expectations: %v", name, mockErr)
		}
	}
}
