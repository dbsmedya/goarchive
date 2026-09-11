package archiver

import (
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	archiveconfig "github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
)

func TestIntegrationDSNParity(t *testing.T) {
	fixture := DatabaseConfig{Name: "source", Host: "db.example", Port: 3307, User: "user", Password: "p@ss:word", Database: "archive"}
	for _, tc := range []struct { name, db string; timeout time.Duration }{{"server", "", 5 * time.Second}, {"database", "archive", 30 * time.Second}} {
		t.Run(tc.name, func(t *testing.T) {
			gotDSN, err := integrationDSN(fixture, tc.db, tc.timeout); if err != nil { t.Fatal(err) }
			got, err := mysql.ParseDSN(gotDSN); if err != nil { t.Fatal(err) }
			want, err := mysql.ParseDSN(database.BuildDSN(&archiveconfig.DatabaseConfig{Host: fixture.Host, Port: fixture.Port, User: fixture.User, Password: fixture.Password, Database: tc.db, TLS: "disable"})); if err != nil { t.Fatal(err) }
			if got.User != want.User || got.Passwd != want.Passwd || got.Net != want.Net || got.Addr != want.Addr || got.DBName != tc.db || got.ParseTime != want.ParseTime || got.Loc != want.Loc || got.TLSConfig != want.TLSConfig || got.MultiStatements != want.MultiStatements || got.Params["sql_mode"] != want.Params["sql_mode"] || got.Params["sql_notes"] != want.Params["sql_notes"] { t.Fatalf("integration DSN diverged: got %#v want %#v", got, want) }
			if got.Timeout != tc.timeout { t.Fatalf("timeout = %s, want %s", got.Timeout, tc.timeout) }
		})
	}
}
