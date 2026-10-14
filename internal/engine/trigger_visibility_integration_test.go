//go:build integration

package engine

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/database"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
)

// TestMySQLTriggersTheAccountCannotSee (beta audit A2): MySQL lists a trigger only to an
// account holding TRIGGER on its table. A source AFTER DELETE or destination AFTER INSERT
// trigger that deletes an unrelated row used to pass the trigger checks as absent for an
// account without TRIGGER, and the archive reported success. Each case runs the job as an
// account with exactly the grant recipe's privileges, with and without TRIGGER, through
// engine.Run: without it the run is refused by the side's visibility check before any row
// moves, and the state is unconfirmed, since this account cannot read mysql.* (as an
// operator's recipe account cannot); with it the existing trigger check names the trigger.
func TestMySQLTriggersTheAccountCannotSee(t *testing.T) {
	for _, side := range []string{"source", "destination"} {
		for _, canSee := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s trigger, TRIGGER held %t", side, canSee), func(t *testing.T) {
				e := newEnv(t, "trigvis")
				e.both(t, "CREATE TABLE payload (id INT PRIMARY KEY, body VARCHAR(40)) ENGINE=InnoDB",
					"CREATE TABLE victims (id INT PRIMARY KEY, body VARCHAR(40)) ENGINE=InnoDB")
				execAll(t, e.src, "INSERT INTO payload VALUES (1, 'archive me')")
				admin, schema, prefix := e.src, e.srcSchema, "TEST_SOURCE"
				grants := "SELECT, DELETE" // the source recipe
				trigger := "CREATE TRIGGER erase_victim AFTER DELETE ON payload FOR EACH ROW DELETE FROM victims WHERE id = OLD.id"
				check, hidden := "DELETE_TRIGGER_CHECK", "DELETE_TRIGGER_VISIBILITY_CHECK"
				if side == "destination" {
					admin, schema, prefix = e.dst, e.dstSchema, "TEST_DEST"
					grants = "SELECT, INSERT, CREATE, UPDATE" // the destination recipe, tracking tables included
					trigger = "CREATE TRIGGER erase_victim AFTER INSERT ON payload FOR EACH ROW DELETE FROM victims WHERE id = NEW.id"
					check, hidden = "DEST_INSERT_TRIGGER_CHECK", "DEST_INSERT_TRIGGER_VISIBILITY_CHECK"
				}
				execAll(t, admin, "INSERT INTO victims VALUES (1, 'must survive')", trigger)

				user := fmt.Sprintf("ga_trigvis_%d", os.Getpid())
				execAll(t, admin, "DROP USER IF EXISTS '"+user+"'@'%'", "CREATE USER '"+user+"'@'%' IDENTIFIED BY 'witness_only'",
					"GRANT "+grants+" ON `"+schema+"`.* TO '"+user+"'@'%'", "GRANT PROCESS ON *.* TO '"+user+"'@'%'")
				t.Cleanup(func() { _, _ = admin.Exec("DROP USER IF EXISTS '" + user + "'@'%'") })
				want, wantTables := hidden, []string{"payload(unconfirmed)"}
				if canSee {
					execAll(t, admin, "GRANT TRIGGER ON `"+schema+"`.* TO '"+user+"'@'%'")
					want, wantTables = check, []string{"payload(erase_victim)"}
				}
				port, _ := strconv.Atoi(os.Getenv(prefix + "_PORT"))
				limited, err := sql.Open("mysql", database.BuildDSN(&config.DatabaseConfig{Host: os.Getenv(prefix + "_HOST"),
					Port: port, User: user, Password: "witness_only", Database: schema, TLS: "disable"}))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = limited.Close() })

				cfg := e.config(t, job(t, jobspec.Archive, "\n    root_table: payload\n    primary_key: id\n    where: '1=1'\n"))
				if side == "source" {
					cfg.Source = limited
				} else {
					cfg.Destination = limited
				}
				_, err = e.run(t, cfg)
				victims, payload := count(t, admin, "SELECT COUNT(*) FROM victims"), count(t, e.src, "SELECT COUNT(*) FROM payload")
				t.Logf("run error: %v; victims left %d; source payload left %d", err, victims, payload)
				var pe *preflight.Error
				if !errors.As(err, &pe) || pe.Check != want || !slices.Equal(pe.Tables, wantTables) {
					t.Errorf("run error = %v, want preflight's %s naming %v", err, want, wantTables)
				}
				if victims != 1 || payload != 1 {
					t.Errorf("rows changed before the refusal: victims left %d (want 1), source payload left %d (want 1)", victims, payload)
				}
			})
		}
	}
}
