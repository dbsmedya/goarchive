package archiver

import "testing"

func BenchmarkAppendUnique(b *testing.B) {
	// Simulates one parent->child edge merge: 5k existing PKs, 1k incoming
	// with 50% overlap — the shape Discover produces on wide child tables.
	existing := make([]interface{}, 0, 5000)
	for i := 0; i < 5000; i++ {
		existing = append(existing, int64(i))
	}
	incoming := make([]interface{}, 0, 1000)
	for i := 4500; i < 5500; i++ {
		incoming = append(incoming, int64(i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seen := make(map[interface{}]struct{}, len(existing))
		for _, v := range existing {
			seen[v] = struct{}{}
		}
		base := existing[:5000:5000]
		_ = appendUnique(base, incoming, seen)
	}
}
