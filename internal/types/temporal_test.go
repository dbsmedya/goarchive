package types

import (
	"errors"
	"strings"
	"testing"
)

func TestTemporalProjection(t *testing.T) {
	m := ColumnMetadata{Names: []string{"id", "EventDate", "dt", "ts", "tm", "yr", "secret"}, Temporal: map[string]TemporalKind{"EventDate": TemporalDate, "dt": TemporalDateTime, "ts": TemporalTimestamp}}
	got, err := m.Projection()
	want := "`id`, CAST(`EventDate` AS CHAR) AS `EventDate`, CAST(`dt` AS CHAR) AS `dt`, CAST(`ts` AS CHAR) AS `ts`, `tm`, `yr`, `secret`"
	if err != nil || got != want {
		t.Fatalf("projection=%q err=%v, want %q", got, err, want)
	}
	if m.Kind("eventdate") != TemporalDate {
		t.Fatal("column name fold lost")
	}
	if _, err := (ColumnMetadata{Names: []string{"id"}}).Projection(); err == nil {
		t.Fatal("missing metadata accepted")
	}
	if got, err := (ColumnMetadata{Names: []string{"id"}, Temporal: map[string]TemporalKind{}}).Projection(); err != nil || got != "`id`" {
		t.Fatalf("known non-temporal column refused: %q %v", got, err)
	}
}

func TestTemporalReadContractError(t *testing.T) {
	cause := errors.New("sentinel")
	e := &TemporalReadContractError{Table: "t", Operation: "copy", Detail: "failed", Cause: cause}
	if !errors.Is(e, cause) || !strings.Contains(e.Error(), "TEMPORAL_READ_CONTRACT: table=t operation=copy:") {
		t.Fatal(e)
	}
}
