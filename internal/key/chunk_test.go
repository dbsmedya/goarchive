package key

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func TestMaxTuples(t *testing.T) {
	const mib = 1 << 20
	ints := func(n int) []Column {
		cols := make([]Column, n)
		for i := range cols {
			cols[i] = Column{Name: fmt.Sprintf("c%d", i), Family: FamilySigned}
		}
		return cols
	}
	utf8mb4 := func(n int) Column {
		return Column{Name: "s", Family: FamilyString, Length: n, Charset: "utf8mb4"}
	}
	cases := []struct {
		name   string
		cols   []Column
		budget uint64
		want   int
	}{
		{"2 ints", ints(2), 8 * mib, 8000}, // probe P2: 10,000 two-column tuples were still a range
		{"1 int", ints(1), 8 * mib, 16000},
		{"3 ints", ints(3), 8 * mib, 5333},
		{"2 ints at 1 MiB", ints(2), 1 * mib, 1000}, // probe P2 at 1 MiB: 1,000 range, 5,000 full scan
		{"unlimited", ints(2), 0, 32767},            // only the 65,535-placeholder limit
		{"1 int at 64 MiB", ints(1), 64 * mib, 65535},
		{"tiny budget", ints(4), 1 << 10, 1}, // a tiny budget still allows one tuple
		// Wide parts: the width bound is tighter than the 2,000-parts-per-MiB density. Measured
		// limits at 8 MiB on 8.0.46 / 8.4.11: 6,415 / 6,496, 5,386 / 5,503, 2,428 / 2,438.
		{"utf8mb4 varchar(255)", []Column{utf8mb4(255)}, 8 * mib, 3297},
		{"int, utf8mb4 varchar(255)", append(ints(1), utf8mb4(255)), 8 * mib, 2741},
		{"utf8mb4 varchar(768)", []Column{utf8mb4(768)}, 8 * mib, 1261},
		{"latin1 varchar(255)", []Column{{Name: "s", Family: FamilyString, Length: 255, Charset: "latin1"}}, 8 * mib, 8272},
		{"binary(16)", []Column{{Name: "u", Family: FamilyBinary, Length: 16}}, 8 * mib, 15650},
		{"unknown charset is charged 4 bytes", []Column{{Name: "s", Family: FamilyString, Length: 255, Charset: "gb18030"}}, 8 * mib, 3297},
		// 2,000 × budget must not overflow: a huge budget leaves the placeholder bound.
		{"budget 2^63", ints(1), 1 << 63, 65535},
		{"budget 2^64-1", ints(1), ^uint64(0), 65535},
	}
	for _, tc := range cases {
		if got := MaxTuples(tc.cols, tc.budget); got != tc.want {
			t.Errorf("%s: MaxTuples(%d) = %d, want %d", tc.name, tc.budget, got, tc.want)
		}
	}
}

func TestChunks(t *testing.T) {
	keys := make([]Key, 7)
	for i := range keys {
		keys[i] = Key{int64(i)}
	}
	got := Chunks(keys, 3)
	if len(got) != 3 || len(got[0]) != 3 || len(got[2]) != 1 || got[2][0][0].(int64) != 6 {
		t.Fatalf("Chunks = %v", got)
	}
	if Chunks(nil, 3) != nil {
		t.Fatal("Chunks(nil) is not nil")
	}
	// Appending to a chunk must not overwrite the next one.
	first := append(got[0], Key{int64(99)})
	if got[1][0][0].(int64) != 3 || len(first) != 4 {
		t.Fatal("chunks share capacity")
	}
}

// TestChunksDoNotAliasTheSource covers the last chunk, which used to keep the input's spare
// capacity: Set.Keys() returns the set's own slice, so appending to that chunk wrote into the
// set's backing array, and the set's next Add overwrote the appended key in the chunk.
func TestChunksDoNotAliasTheSource(t *testing.T) {
	s := NewSet()
	for i := 0; i < 5; i++ {
		s.Add(Key{int64(i)})
	}
	chunks := Chunks(s.Keys(), 3)
	last := append(chunks[len(chunks)-1], Key{int64(99)})
	s.Add(Key{int64(5)})
	if got := last[len(last)-1][0].(int64); got != 99 {
		t.Fatalf("appended key became %d: the last chunk aliases the set", got)
	}
}

// A pool may run SHOW WARNINGS on another connection than the statement's, where warning
// 3170 is never seen, so CheckRangeWarning refuses one before it queries (M5-3).
func TestCheckRangeWarningRefusesAPool(t *testing.T) {
	db, err := sql.Open("mysql", "u:p@tcp(127.0.0.1:1)/x")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	err = CheckRangeWarning(context.Background(), db)
	if err == nil || !strings.Contains(err.Error(), "connection pool") {
		t.Fatalf("CheckRangeWarning(*sql.DB) = %v, want a refusal naming the connection pool", err)
	}
}

// driverExecuteBytes is the COM_STMT_EXECUTE payload go-sql-driver sends for args
// (writeExecutePacket): 1 command, 4 statement id, 1 flags and 4 iteration bytes, the null
// bitmap, 1 new-params flag, 2 bytes of type per parameter, then each value: 8 bytes for
// an int64 or uint64, a length-encoded length plus the bytes for a string or []byte.
func driverExecuteBytes(args []any) int {
	n := len(args)
	size := 1 + 4 + 1 + 4 + (n+7)/8 + 1 + 2*n
	for _, a := range args {
		switch v := a.(type) {
		case int64, uint64:
			size += 8
		case string:
			size += lengthEncoded(len(v)) + len(v)
		case []byte:
			size += lengthEncoded(len(v)) + len(v)
		}
	}
	return size
}

func lengthEncoded(l int) int {
	switch {
	case l < 251:
		return 1
	case l < 1<<16:
		return 3
	case l < 1<<24:
		return 4
	}
	return 9
}

// widestKeys holds each key family with its widest value, as Decode holds it (M7c,
// Fable M7c-1 review I1, whose probe measured them).
var widestKeys = []struct {
	name string
	col  Column
	val  any
}{
	{"int", Column{Name: "k", Family: FamilySigned}, int64(-1)},
	{"bigint unsigned", Column{Name: "k", Family: FamilyUnsigned}, uint64(1)},
	{"year", Column{Name: "k", Family: FamilyYear}, int64(2024)},
	{"decimal(10,2)", Column{Name: "k", Family: FamilyDecimal, Precision: 10, Scale: 2}, "-" + strings.Repeat("9", 8) + ".99"},
	{"decimal(38,0)", Column{Name: "k", Family: FamilyDecimal, Precision: 38}, "-" + strings.Repeat("9", 38)},
	{"decimal(65,30)", Column{Name: "k", Family: FamilyDecimal, Precision: 65, Scale: 30}, "-" + strings.Repeat("9", 35) + "." + strings.Repeat("9", 30)},
	{"date", Column{Name: "k", Family: FamilyDate}, "2024-01-01"},
	{"datetime(0)", Column{Name: "k", Family: FamilyDateTime}, "2024-01-01 12:34:56"},
	{"datetime(6)", Column{Name: "k", Family: FamilyDateTime, Fsp: 6}, "2024-01-01 12:34:56.123456"},
	{"timestamp(6)", Column{Name: "k", Family: FamilyTimestamp, Fsp: 6}, "2024-01-01 12:34:56.123456"},
	{"time(6)", Column{Name: "k", Family: FamilyTime, Fsp: 6}, "-838:59:59.000000"},
	{"varchar(255) utf8mb4", Column{Name: "k", Family: FamilyString, Length: 255, Charset: "utf8mb4"}, []byte(strings.Repeat("x", 1020))},
	// A latin1 key comes back converted to the session's utf8mb4 (character_set_results), so
	// '€', one byte stored, is three on the wire (Codex M7c-1 review B1).
	{"char(10) latin1", Column{Name: "k", Family: FamilyString, Length: 10, Charset: "latin1"}, []byte(strings.Repeat("€", 10))},
	{"varchar(255) latin1", Column{Name: "k", Family: FamilyString, Length: 255, Charset: "latin1"}, []byte(strings.Repeat("€", 250) + "12345")},
	{"binary(16)", Column{Name: "k", Family: FamilyBinary, Length: 16}, []byte(strings.Repeat("x", 16))},
}

// TestStatementBytesBoundsTheWire (M7c F3, Fable M7c-1 review I1): StatementBytes and
// InsertBytes bound both packets of a statement: its prepared text and the execute packet
// the driver really sends. The execute packet carries the value Decode holds (8 bytes for
// an integer, SQL text for DECIMAL and the temporal families, the bytes for strings), not
// the index key image MaxTuples charges. Checked for each family's widest value, at the
// caps of three range budgets, alone and as the second part of a composite key, for an IN
// statement with 0 and 2 integer status parameters and for the tracking log's INSERT, which
// binds one status per row.
func TestStatementBytesBoundsTheWire(t *testing.T) {
	for _, w := range widestKeys {
		lead := Column{Name: "a", Family: FamilySigned}
		for _, shape := range []struct {
			cols []Column
			vals []any
		}{{[]Column{w.col}, []any{w.val}}, {[]Column{lead, w.col}, []any{int64(-1), w.val}}} {
			names := make([]string, len(shape.cols))
			for i, c := range shape.cols {
				names[i] = c.Name
			}
			for _, budget := range []uint64{8 << 20, 64 << 20, 0} {
				tuples := MaxTuples(shape.cols, budget)
				for _, ints := range []int{0, 2} {
					args := make([]any, 0, tuples*len(shape.cols)+ints)
					for range tuples {
						args = append(args, shape.vals...)
					}
					for range ints {
						args = append(args, int64(4))
					}
					text, exec := len(In(names, shape.cols, tuples)), driverExecuteBytes(args)
					if got := StatementBytes(names, shape.cols, tuples, ints, 0); got < uint64(max(text, exec)) {
						t.Errorf("%s, %d parts, budget %d, %d ints: StatementBytes = %d, below text %d / execute %d",
							w.name, len(shape.cols), budget, ints, got, text, exec)
					}
				}

				rows := min(tuples, 65535/(len(shape.cols)+1))
				phs := make([]string, len(shape.cols))
				for i, c := range shape.cols {
					phs[i] = Placeholder(c)
				}
				row := "(" + strings.Join(phs, ", ") + ", ?)"
				args := make([]any, 0, rows*(len(shape.cols)+1))
				list := make([]string, rows)
				for i := range list {
					list[i] = row
					args = append(args, shape.vals...)
					args = append(args, int64(1))
				}
				text, exec := len(" VALUES "+strings.Join(list, ", ")), driverExecuteBytes(args)
				if got := InsertBytes(shape.cols, rows, 1, 0); got < uint64(max(text, exec)) {
					t.Errorf("%s, %d parts, budget %d: InsertBytes(%d rows) = %d, below text %d / execute %d",
						w.name, len(shape.cols), budget, rows, got, text, exec)
				}
			}
		}
	}
}
