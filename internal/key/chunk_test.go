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
