package types

import (
	"errors"
	"strings"
	"testing"
	"time"
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

func TestValidateTemporalKey(t *testing.T) {
	for _, tc := range []struct {
		kind  TemporalKind
		value string
		valid bool
	}{
		{TemporalDate, "2020-02-29", true}, {TemporalDate, "2021-02-29", false}, {TemporalDate, "2020-02-31", false}, {TemporalDate, "2020-00-15", false}, {TemporalDate, "2020-01-00", false}, {TemporalDate, "0000-00-00", false}, {TemporalDate, "0000-03-02", false}, {TemporalDate, "2020-03-02 12:00:00", false},
		{TemporalDateTime, "2020-03-02 12:00:00", true}, {TemporalDateTime, "2020-03-02 12:00:00.123456", true}, {TemporalTimestamp, "2020-03-02 12:00:00.123456", true}, {TemporalDateTime, "2020-03-02 24:00:00", false}, {TemporalDateTime, "2020-03-02 12:00:60", false}, {TemporalTimestamp, "2020-03-02 12:00:00.1234567", false}, {TemporalDate, "2020-02-29 ", false}, {TemporalNone, "2020-02-29", false}, {TemporalDateTime, "2020-02-29", false}, {TemporalDateTime, "0000-03-02 12:00:00", false}, {TemporalDateTime, "2021-02-29 12:00:00", false}, {TemporalDateTime, "2020-03-02 12:00:00Z", false}, {TemporalDateTime, "2020-03-02 12:00:00 ", false},
	} {
		t.Run(tc.value, func(t *testing.T) {
			if err := ValidateTemporalKey(tc.kind, tc.value); (err == nil) != tc.valid {
				t.Fatalf("key=%q valid=%v err=%v", tc.value, tc.valid, err)
			}
		})
	}
}
func TestTemporalIdentitySet(t *testing.T) {
	for _, observed := range [][]interface{}{nil, {"2020-03-03"}, {"2020-03-02", "2020-03-02"}} {
		s, err := NewTemporalIdentitySet(TemporalDate, "t", "copy", []interface{}{"2020-03-02"})
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range observed {
			if e := s.Observe(v); e != nil {
				err = e
				break
			}
		}
		if err == nil {
			err = s.Finish()
		}
		if err == nil || !strings.Contains(err.Error(), "TEMPORAL_READ_CONTRACT") {
			t.Fatalf("missing identity refusal for %v: %v", observed, err)
		}
	}
}
func TestTemporalTextContainers(t *testing.T) {
	b := []byte("2020-02-29")
	got, err := TemporalText(b)
	b[0] = '9'
	if err != nil || got != "2020-02-29" {
		t.Fatal(got, err)
	}
	got, err = TemporalText("2020-02-29")
	if err != nil || got != "2020-02-29" {
		t.Fatal(got, err)
	}
	for _, v := range []interface{}{nil, time.Time{}, 1} {
		if _, err := TemporalText(v); err == nil {
			t.Fatalf("accepted %T", v)
		}
	}
}
func TestTemporalIdentityDeduplicatesRequests(t *testing.T) {
	s, err := NewTemporalIdentitySet(TemporalDate, "t", "copy", []interface{}{"2020-03-02", "2020-03-02"})
	if err != nil {
		t.Fatal(err)
	}
	keys := s.Keys()
	if len(keys) != 1 {
		t.Fatal(keys)
	}
	keys[0] = "changed"
	if err := s.Observe("2020-03-02"); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(); err != nil {
		t.Fatal(err)
	}
}
func TestTemporalReadContractError(t *testing.T) {
	cause := errors.New("sentinel")
	e := &TemporalReadContractError{Table: "t", Operation: "copy", Detail: "failed", Cause: cause}
	if !errors.Is(e, cause) || !strings.Contains(e.Error(), "TEMPORAL_READ_CONTRACT: table=t operation=copy:") {
		t.Fatal(e)
	}
}
