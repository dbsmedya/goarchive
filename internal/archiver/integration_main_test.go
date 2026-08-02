//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// TestMain proves the real-DB environment is usable before a single test runs,
// and fails the whole test binary if it is not.
//
// Building with `-tags=integration` IS the opt-in: a binary that reaches here
// has explicitly asked for real-DB tests. Skipping them when the environment is
// missing makes `go test` print
//
//	ok  github.com/dbsmedya/goarchive/internal/archiver  0.520s
//
// and exit 0 for a suite that executed nothing. That is the one answer that can
// never be right, and it is invisible to a gate that counts `--- FAIL` — the
// count is 0 either way. Worse, without `-v` the `--- SKIP` lines are not
// printed at all, so nothing on screen distinguishes a green run from an absent
// one. Failing here costs one clear, actionable message instead.
//
// A data-dependent `t.Skip` inside a test body remains legitimate; this guards
// only the environment.
func TestMain(m *testing.M) {
	if err := requireIntegrationEnv(); err != nil {
		fmt.Fprint(os.Stderr, err.Error())
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// requireIntegrationEnv returns nil only when credentials are present AND every
// configured server actually answers. Both are checked here rather than at the
// first query so the failure names the cause once, up front, instead of
// surfacing as an unrelated-looking error inside whichever test happened to run
// first.
func requireIntegrationEnv() error {
	cfg, err := LoadIntegrationConfig()
	if err != nil {
		return integrationEnvError("credentials are not set", firstLine(err.Error()))
	}

	if len(cfg.Databases) == 0 {
		return integrationEnvError("no databases configured",
			"the integration config lists zero databases")
	}

	for _, dbCfg := range cfg.Databases {
		if dbCfg.Password == "" {
			return integrationEnvError("credentials are not set",
				fmt.Sprintf("database %q (%s:%d) has an empty password",
					dbCfg.Name, dbCfg.Host, dbCfg.Port))
		}
	}

	for _, dbCfg := range cfg.Databases {
		if err := pingIntegrationDB(dbCfg); err != nil {
			return integrationEnvError("a test database is unreachable",
				fmt.Sprintf("%s at %s:%d — %v", dbCfg.Name, dbCfg.Host, dbCfg.Port, err))
		}
	}

	return nil
}

func pingIntegrationDB(dbCfg DatabaseConfig) error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=5s",
		dbCfg.User, dbCfg.Password, dbCfg.Host, dbCfg.Port)

	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return conn.PingContext(ctx)
}

// integrationEnvError renders a message an operator can act on without reading
// any source. It names what is wrong, then exactly what to type.
func integrationEnvError(headline, detail string) error {
	const rule = "──────────────────────────────────────────────────────────────────────────────"

	return fmt.Errorf(`
%s
 INTEGRATION TESTS DID NOT RUN — %s
%s

  %s

  These tests talk to real MySQL servers, so the test environment has to be
  loaded before you call go test:

      set -a; source tests/.env; set +a

  If tests/.env does not exist yet:

      cp tests/dot.env tests/.env        # then edit it

  If the databases are not running:

      make test-up

  Or let the runner do all of it — it sources tests/.env for you:

      bash tests/scripts/run-tests.sh --setup --integration-only

  Reference: tests/README.md § Prerequisites
%s
`, rule, headline, rule, detail, rule)
}

// firstLine keeps the cause on one line. LoadIntegrationConfig's error already
// carries its own multi-line remediation block, which would otherwise be printed
// alongside — and disagree with — the guidance above.
func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return strings.TrimSpace(s[:i])
	}
	return strings.TrimSpace(s)
}
