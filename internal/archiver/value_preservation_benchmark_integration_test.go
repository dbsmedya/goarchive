//go:build integration

package archiver

import (
	"context"
	"fmt"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
	"github.com/dbsmedya/goarchive/internal/graph"
	"strings"
	"testing"
)

func vpBenchmarkCopier(b *testing.B, cfg *config.Config, mgr *database.Manager, method string) *CopyPhase {
	b.Helper()
	g := graph.NewGraph("vp_bench", "id")
	cp, err := NewCopyPhase(mgr.Source, mgr.Destination, g, config.SafetyConfig{}, nil)
	if err != nil {
		b.Fatal(err)
	}
	meta, err := sourceColumnMetadata(context.Background(), mgr.Source, cfg.Source.Database, g.AllNodes())
	if err != nil {
		b.Fatal(err)
	}
	cp.SetColumnMetadata(meta)
	cp.SetStrictInsert(method == "count")
	cp.SetDiagnosticPolicy(config.VerificationConfig{Method: method}, false)
	return cp
}
func BenchmarkValuePreservationCopy(b *testing.B) {
	for _, method := range []string{"count", "sha256"} {
		for _, size := range []int{100, 5000} {
			for _, duplicate := range []bool{false, true} {
				if duplicate && method != "sha256" {
					continue
				}
				b.Run(fmt.Sprintf("%s/rows_%d/duplicate_%v", method, size, duplicate), func(b *testing.B) {
					b.StopTimer()
					cfg, mgr := vpFixture(b)
					columns := []string{"id BIGINT PRIMARY KEY"}
					for i := 0; i < 12; i++ {
						columns = append(columns, fmt.Sprintf("v%d BIGINT", i))
					}
					vpTable(b, mgr, "vp_bench", "("+strings.Join(columns, ",")+") ENGINE=InnoDB")
					rows := make([]string, size)
					args := make([]interface{}, 0, size*13)
					keys := make([]interface{}, size)
					for i := 0; i < size; i++ {
						rows[i] = "(" + strings.TrimSuffix(strings.Repeat("?,", 13), ",") + ")"
						keys[i] = int64(i + 1)
						for j := 0; j < 13; j++ {
							args = append(args, int64(i+1))
						}
					}
					insert := "INSERT INTO vp_bench VALUES " + strings.Join(rows, ",")
					vpExec(b, mgr.Source, insert, args...)
					cp := vpBenchmarkCopier(b, cfg, mgr, method)
					records := &RecordSet{Records: map[string][]interface{}{"vp_bench": keys}}
					var cap int
					if err := mgr.Destination.QueryRow("SELECT @@SESSION.max_error_count").Scan(&cap); err != nil {
						b.Fatal(err)
					}
					b.Logf("method=%s rows=%d duplicate=%v max_error_count=%d", method, size, duplicate, cap)
					for i := 0; i < b.N; i++ {
						vpExec(b, mgr.Destination, "TRUNCATE TABLE vp_bench")
						if duplicate {
							vpExec(b, mgr.Destination, insert, args...)
						}
						b.StartTimer()
						_, err := cp.Copy(context.Background(), records)
						b.StopTimer()
						if err != nil {
							b.Fatal(err)
						}
						var count int
						if err := mgr.Destination.QueryRow("SELECT COUNT(*) FROM vp_bench").Scan(&count); err != nil {
							b.Fatal(err)
						}
						if count != size {
							b.Fatalf("destination count=%d want=%d", count, size)
						}
					}
					b.ReportMetric(float64(size), "rows/op")
					b.ReportMetric(float64(size*b.N)/b.Elapsed().Seconds(), "rows/s")
				})
			}
		}
	}
}
