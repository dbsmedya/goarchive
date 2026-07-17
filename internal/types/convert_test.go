package types

import (
	"testing"
)

func TestConvertRootPK(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		dataType string
		unsigned bool
		want     interface{}
		wantErr  bool
	}{
		{"int signed", "42", "int", false, int64(42), false},
		{"bigint signed negative", "-9999", "bigint", false, int64(-9999), false},
		{"bigint unsigned max", "18446744073709551615", "bigint", true, uint64(18446744073709551615), false},
		{"smallint", "100", "smallint", false, int64(100), false},
		{"varchar rejected", "abc", "varchar", false, nil, true},
		{"empty rejected", "", "int", false, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ConvertRootPK(tt.raw, tt.dataType, tt.unsigned)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err: want %v, got %v", tt.wantErr, err)
			}
			if !tt.wantErr && got != tt.want {
				t.Fatalf("value: want %v (%T), got %v (%T)", tt.want, tt.want, got, got)
			}
		})
	}
}
