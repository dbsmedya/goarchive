package types

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRecordSet_Initialization(t *testing.T) {
	t.Run("Empty RecordSet", func(t *testing.T) {
		rs := &RecordSet{}
		assert.Nil(t, rs.RootPKs)
		assert.Nil(t, rs.Records)
		assert.Equal(t, DiscoveryStats{}, rs.Stats)
	})

	t.Run("RecordSet with data", func(t *testing.T) {
		rs := &RecordSet{
			RootPKs: []interface{}{int64(1), int64(2), int64(3)},
			Records: map[string][]interface{}{
				"customers": {int64(1), int64(2), int64(3)},
				"orders":    {int64(101), int64(102), int64(103)},
			},
			Stats: DiscoveryStats{
				TablesScanned: 2,
				RecordsFound:  6,
				BFSLevels:     2,
				Duration:      100 * time.Millisecond,
			},
		}

		assert.Len(t, rs.RootPKs, 3)
		assert.Equal(t, int64(1), rs.RootPKs[0])
		assert.Len(t, rs.Records, 2)
		assert.Len(t, rs.Records["customers"], 3)
		assert.Len(t, rs.Records["orders"], 3)
		assert.Equal(t, 2, rs.Stats.TablesScanned)
		assert.Equal(t, int64(6), rs.Stats.RecordsFound)
		assert.Equal(t, 2, rs.Stats.BFSLevels)
		assert.Equal(t, 100*time.Millisecond, rs.Stats.Duration)
	})
}

func TestRecordSet_FieldAccess(t *testing.T) {
	rs := &RecordSet{
		RootPKs: []interface{}{int64(10), int64(20)},
		Records: map[string][]interface{}{
			"table1": {int64(1)},
			"table2": {int64(2), int64(3)},
		},
		Stats: DiscoveryStats{
			TablesScanned: 2,
			RecordsFound:  3,
		},
	}

	t.Run("Access RootPKs", func(t *testing.T) {
		assert.NotNil(t, rs.RootPKs)
		assert.Len(t, rs.RootPKs, 2)
		assert.Equal(t, int64(10), rs.RootPKs[0])
		assert.Equal(t, int64(20), rs.RootPKs[1])
	})

	t.Run("Access Records map", func(t *testing.T) {
		assert.NotNil(t, rs.Records)
		assert.Len(t, rs.Records, 2)
		assert.Contains(t, rs.Records, "table1")
		assert.Contains(t, rs.Records, "table2")
	})

	t.Run("Access individual table records", func(t *testing.T) {
		table1Records := rs.Records["table1"]
		assert.Len(t, table1Records, 1)
		assert.Equal(t, int64(1), table1Records[0])

		table2Records := rs.Records["table2"]
		assert.Len(t, table2Records, 2)
		assert.Equal(t, int64(2), table2Records[0])
		assert.Equal(t, int64(3), table2Records[1])
	})

	t.Run("Access Stats", func(t *testing.T) {
		assert.Equal(t, 2, rs.Stats.TablesScanned)
		assert.Equal(t, int64(3), rs.Stats.RecordsFound)
	})
}

func TestDiscoveryStats_ZeroValues(t *testing.T) {
	stats := DiscoveryStats{}
	assert.Equal(t, 0, stats.TablesScanned)
	assert.Equal(t, int64(0), stats.RecordsFound)
	assert.Equal(t, 0, stats.BFSLevels)
	assert.Equal(t, time.Duration(0), stats.Duration)
}

func TestDiscoveryStats_WithValues(t *testing.T) {
	duration := 500 * time.Millisecond
	stats := DiscoveryStats{
		TablesScanned: 5,
		RecordsFound:  1234,
		BFSLevels:     3,
		Duration:      duration,
	}

	assert.Equal(t, 5, stats.TablesScanned)
	assert.Equal(t, int64(1234), stats.RecordsFound)
	assert.Equal(t, 3, stats.BFSLevels)
	assert.Equal(t, duration, stats.Duration)
}

func TestRecordSet_EmptyRecordsMap(t *testing.T) {
	rs := &RecordSet{
		RootPKs: []interface{}{int64(1)},
		Records: map[string][]interface{}{},
		Stats: DiscoveryStats{
			TablesScanned: 0,
			RecordsFound:  0,
		},
	}

	assert.NotNil(t, rs.Records)
	assert.Len(t, rs.Records, 0)
	assert.Len(t, rs.RootPKs, 1)
}

func TestRecordSet_NilRecords(t *testing.T) {
	rs := &RecordSet{
		RootPKs: []interface{}{},
		Records: nil,
	}

	assert.Nil(t, rs.Records)
	assert.NotNil(t, rs.RootPKs)
	assert.Len(t, rs.RootPKs, 0)
}

func TestRecordSet_MultipleTableTypes(t *testing.T) {
	rs := &RecordSet{
		RootPKs: []interface{}{int64(1), int64(2)},
		Records: map[string][]interface{}{
			"customers":      {int64(1), int64(2)},
			"orders":         {int64(101), int64(102), int64(103)},
			"order_items":    {int64(1001), int64(1002)},
			"order_payments": {int64(2001)},
		},
		Stats: DiscoveryStats{
			TablesScanned: 4,
			RecordsFound:  8,
			BFSLevels:     3,
			Duration:      250 * time.Millisecond,
		},
	}

	assert.Len(t, rs.Records, 4)
	assert.Len(t, rs.Records["customers"], 2)
	assert.Len(t, rs.Records["orders"], 3)
	assert.Len(t, rs.Records["order_items"], 2)
	assert.Len(t, rs.Records["order_payments"], 1)
	assert.Equal(t, int64(8), rs.Stats.RecordsFound)
}
