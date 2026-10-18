package database

import (
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/config"
)

// TestOpenPoolGlobals (M7c-F6): a test's emulated server global layers under goarchive's pins
// the way a real one does. sql_mode becomes the base of the auto-zero expression; the
// globals a pin or a check would undo, or the driver owns, are refused by name; any other
// global is passed through. With no globals the DSN is exactly BuildDSN's.
func TestOpenPoolGlobals(t *testing.T) {
	cfg := &config.DatabaseConfig{Host: "localhost", Port: 3306, User: "test", Database: "test", TLS: "disable"}

	t.Run("no globals", func(t *testing.T) {
		mc, err := dsnConfig(cfg, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := mc.FormatDSN(), BuildDSN(cfg); got != want {
			t.Errorf("dsnConfig(cfg, nil) = %q, want BuildDSN's %q", got, want)
		}
		if got, want := mc.Params["sql_mode"], "CONCAT_WS(',', NULLIF(@@SESSION.sql_mode, ''), 'NO_AUTO_VALUE_ON_ZERO')"; got != want {
			t.Errorf("sql_mode = %q, want %q", got, want)
		}
	})

	composed := []struct {
		name   string
		global Global
		want   string
	}{
		{"empty sql_mode", Global{"sql_mode", "''"}, "CONCAT_WS(',', NULLIF('', ''), 'NO_AUTO_VALUE_ON_ZERO')"},
		{"a quoted sql_mode", Global{"sql_mode", "'ALLOW_INVALID_DATES'"}, "CONCAT_WS(',', NULLIF('ALLOW_INVALID_DATES', ''), 'NO_AUTO_VALUE_ON_ZERO')"},
		{"a name in another case", Global{"SQL_MODE", "'ANSI'"}, "CONCAT_WS(',', NULLIF('ANSI', ''), 'NO_AUTO_VALUE_ON_ZERO')"},
	}
	for _, tc := range composed {
		t.Run(tc.name, func(t *testing.T) {
			mc, err := dsnConfig(cfg, []Global{tc.global})
			if err != nil {
				t.Fatal(err)
			}
			if got := mc.Params["sql_mode"]; got != tc.want {
				t.Errorf("sql_mode = %q, want %q", got, tc.want)
			}
			if _, ok := mc.Params[tc.global.Name]; ok && tc.global.Name != "sql_mode" {
				t.Errorf("Params carries %q as well as sql_mode: %#v", tc.global.Name, mc.Params)
			}
		})
	}

	refused := []Global{
		{"sql_mode", "ALLOW_INVALID_DATES"},
		{"sql_mode", `'a\'b'`},
		{"time_zone", "'+01:00'"},
		{"autocommit", "0"},
		{"sql_notes", "0"},
		{"sql_select_limit", "20"},
		{"wait_timeout", "10"},
		{"charset", "latin1"},
		{"collation", "latin1_swedish_ci"},
		{"max_error_count", "0"},
		{"x; y", "1"},
	}
	for _, g := range refused {
		t.Run("refused "+g.Name+"="+g.Value, func(t *testing.T) {
			mc, err := dsnConfig(cfg, []Global{g})
			if err == nil {
				t.Fatalf("dsnConfig accepted %+v: %#v", g, mc.Params)
			}
			if !strings.Contains(err.Error(), g.Name) {
				t.Errorf("error %q does not name %q", err, g.Name)
			}
			if mc != nil {
				t.Errorf("dsnConfig returned a config with its error: %#v", mc.Params)
			}
		})
	}

	passed := []Global{
		{"max_error_count", "1"},
		{"range_optimizer_max_mem_size", "1048576"},
	}
	for _, g := range passed {
		t.Run("passed "+g.Name+"="+g.Value, func(t *testing.T) {
			mc, err := dsnConfig(cfg, []Global{g})
			if err != nil {
				t.Fatal(err)
			}
			if got := mc.Params[g.Name]; got != g.Value {
				t.Errorf("Params[%q] = %q, want %q", g.Name, got, g.Value)
			}
			if got, want := mc.Params["sql_mode"], autoZeroMode("@@SESSION.sql_mode"); got != want {
				t.Errorf("sql_mode = %q, want the pin %q", got, want)
			}
		})
	}
}
