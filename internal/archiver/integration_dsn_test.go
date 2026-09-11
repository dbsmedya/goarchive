package archiver

import (
	"reflect"
	"testing"
	"time"

	archiveconfig "github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
	"github.com/go-sql-driver/mysql"
)

// integrationDSN keeps test-estate sessions on the same driver contract as the
// production manager while retaining the shorter harness timeouts.
func integrationDSN(dbCfg DatabaseConfig, databaseName string, timeout time.Duration) (string, error) {
	production := &archiveconfig.DatabaseConfig{Host: dbCfg.Host, Port: dbCfg.Port, User: dbCfg.User, Password: dbCfg.Password, Database: databaseName, TLS: "disable"}
	driverCfg, err := mysql.ParseDSN(database.BuildDSN(production))
	if err != nil {
		return "", err
	}
	driverCfg.DBName, driverCfg.Timeout = databaseName, timeout
	return driverCfg.FormatDSN(), nil
}

func TestIntegrationDSNParity(t *testing.T) {
	fixture := DatabaseConfig{Name: "source", Host: "db.example", Port: 3307, User: "user", Password: "p@ss:word", Database: "archive"}
	for _, tc := range []struct {
		name, db string
		timeout  time.Duration
	}{{"server", "", 5 * time.Second}, {"database", "archive", 30 * time.Second}} {
		t.Run(tc.name, func(t *testing.T) {
			gotDSN, err := integrationDSN(fixture, tc.db, tc.timeout)
			if err != nil {
				t.Fatal(err)
			}
			got, err := mysql.ParseDSN(gotDSN)
			if err != nil {
				t.Fatal(err)
			}
			want, err := mysql.ParseDSN(database.BuildDSN(&archiveconfig.DatabaseConfig{Host: fixture.Host, Port: fixture.Port, User: fixture.User, Password: fixture.Password, Database: tc.db, TLS: "disable"}))
			if err != nil {
				t.Fatal(err)
			}
			want.Timeout, want.DBName = tc.timeout, tc.db
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("integration DSN diverged: got %#v want %#v", got, want)
			}
		})
	}
}
