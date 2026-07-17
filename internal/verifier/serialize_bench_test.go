package verifier

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/goarchive/internal/logger"
)

func benchColumnsAndValues() ([]string, []interface{}) {
	columns := []string{"payment_id", "customer_id", "staff_id", "rental_id", "amount", "payment_date", "last_update", "note"}
	values := []interface{}{
		int64(16049), int64(599), int64(2), int64(15966),
		float64(2.99), []byte("2005-08-23 21:56:42"), []byte("2006-02-15 22:12:32"), "some free-text note",
	}
	return columns, values
}

func BenchmarkSerializeRow(b *testing.B) {
	db, _, _ := sqlmock.New()
	defer func() { _ = db.Close() }()
	g := createTestGraph()
	v, _ := NewVerifier(db, db, g, MethodSHA256, logger.NewDefault())
	columns, values := benchColumnsAndValues()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = v.serializeRow(columns, values)
	}
}
