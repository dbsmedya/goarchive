package key

import "testing"

func TestMaxTuples(t *testing.T) {
	const mib = 1 << 20
	cases := []struct {
		cols   int
		budget uint64
		want   int
	}{
		{2, 8 * mib, 8000},  // probe P2: 10,000 two-column tuples were still a range
		{1, 8 * mib, 16000}, // single-column key
		{3, 8 * mib, 5333},
		{2, 1 * mib, 1000}, // probe P2 at 1 MiB: 1,000 range, 5,000 full scan
		{2, 0, 32767},      // unlimited budget: only the 65,535-placeholder limit
		{1, 64 * mib, 65535},
		{4, 1 << 10, 1}, // a tiny budget still allows one tuple
	}
	for _, tc := range cases {
		if got := MaxTuples(tc.cols, tc.budget); got != tc.want {
			t.Errorf("MaxTuples(%d, %d) = %d, want %d", tc.cols, tc.budget, got, tc.want)
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
