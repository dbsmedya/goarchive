package engine

import (
	"fmt"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/key"
)

func int64Keys(n int) []key.Key {
	keys := make([]key.Key, n)
	for i := range keys {
		keys[i] = key.Key{int64(i)}
	}
	return keys
}

// TestChunkCursorCoversEveryKeyOnce (spec §7, D16): the delete takes a table's keys one chunk
// at a time by an offset, asking for the size at each take, so every key is handed out exactly
// once and in order, whether the size divides the table or not and whatever sizes are asked
// for in turn (a size that changes between two DELETEs applies to the next one, C4a).
func TestChunkCursorCoversEveryKeyOnce(t *testing.T) {
	for _, tc := range []struct {
		name  string
		n     int
		sizes []int // the size asked at each take; the last repeats
	}{
		{"a size that divides", 12, []int{3}},
		{"a size that does not divide", 10, []int{3}},
		{"a size of one", 5, []int{1}},
		{"a size equal to the table", 7, []int{7}},
		{"a size above the table", 7, []int{100}},
		{"one key", 1, []int{2}},
		{"a size lowered then restored", 13, []int{5, 2, 2, 5}},
		{"sizes that change at every take", 20, []int{1, 7, 2, 3, 4, 1, 9}},
		{"a size below one is one", 4, []int{0, -3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keys := int64Keys(tc.n)
			c := newChunkCursor(keys)
			var got []key.Key
			for take := 0; !c.done(); take++ {
				size := tc.sizes[min(take, len(tc.sizes)-1)]
				chunk := c.take(size)
				if len(chunk) == 0 {
					t.Fatalf("take %d returned no keys with %d left: the delete would loop forever", take+1, tc.n-len(got))
				}
				if want := max(size, 1); len(chunk) > want {
					t.Fatalf("take %d returned %d keys for a size of %d", take+1, len(chunk), size)
				}
				if take > 4*tc.n {
					t.Fatalf("%d takes for %d keys", take, tc.n)
				}
				got = append(got, chunk...)
			}
			if fmt.Sprint(got) != fmt.Sprint(keys) {
				t.Fatalf("handed out %v, want every key once in order: %v", got, keys)
			}
			if rest := c.take(3); len(rest) != 0 {
				t.Fatalf("a finished cursor handed out %v", rest)
			}
		})
	}
	if c := newChunkCursor(nil); !c.done() {
		t.Fatal("a table with no keys is not done")
	}
}

// TestChunkCursorChunksAreIndependent: a chunk has no spare capacity, so a statement that
// appends to it cannot write into the keys behind it (the guarantee key.Chunks gives).
func TestChunkCursorChunksAreIndependent(t *testing.T) {
	keys := int64Keys(6)
	c := newChunkCursor(keys)
	first := c.take(2)
	_ = append(first, key.Key{int64(99)})
	if fmt.Sprint(keys[2]) != fmt.Sprint(key.Key{int64(2)}) {
		t.Fatalf("appending to a chunk overwrote the next key: %v", keys)
	}
}
