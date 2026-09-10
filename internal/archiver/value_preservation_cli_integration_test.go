//go:build integration

package archiver

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/internal/config"
	"gopkg.in/yaml.v3"
)

// This is an E2E witness: its subject is the separately built CLI, despite the
// integration build tag. The matrix supplies an isolated server and fresh binary.
func TestValuePreservationCLI_Integration(t *testing.T) {
	binary, profile := os.Getenv("VP_CLI"), os.Getenv("VP_MATRIX_PROFILE")
	if profile == "" {
		t.Skip("matrix-only E2E: fresh VP_CLI and default/empty/raw profile required")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("CLI fixture: VP_CLI must name an absolute freshly built binary")
	}
	if profile != "default" && profile != "empty" && profile != "raw" {
		t.Fatalf("CLI fixture: unsupported matrix profile %q", profile)
	}
	info, err := os.Stat(binary)
	if err != nil || info.IsDir() || info.Mode()&0111 == 0 {
		t.Fatalf("CLI fixture: binary unavailable or not executable: %v", err)
	}
	for _, prefix := range []string{"TEST_SOURCE", "TEST_DEST"} {
		port := os.Getenv(prefix + "_PORT")
		if os.Getenv(prefix+"_HOST") != "127.0.0.1" || (port != "13460" && port != "13464" && port != "13467") {
			t.Fatalf("CLI fixture: %s must target an owned disposable matrix server", prefix)
		}
	}

	for _, command := range []string{"archive", "dry-run"} {
		for _, skip := range []bool{false, true} {
			t.Run(fmt.Sprintf("payload/%s/skip_%v", command, skip), func(t *testing.T) {
				cfg, mgr := vpFixture(t)
				vpCLIAssertProfile(t, mgr.Destination, profile)
				const table = "vp_cli_dates"
				vpTable(t, mgr, table, "(id BIGINT PRIMARY KEY,d DATE,dt DATETIME(6)) ENGINE=InnoDB")
				vpSeed(t, cfg.Source, "ALLOW_INVALID_DATES",
					"INSERT INTO vp_cli_dates VALUES(1,'2020-02-31','2020-02-31 12:00:00.123456')")
				name := fmt.Sprintf("vp_cli_%s_%v", strings.ReplaceAll(command, "-", "_"), skip)
				cfg.Jobs = map[string]config.JobConfig{name: {RootTable: table, PrimaryKey: "id", Where: "id=1"}}
				cfg.Logging.Format = "text"
				logTable := bootstrapJobTracking(t, mgr.Destination, cfg.Destination.Database, name, table, JobTypeArchive)
				query := "SELECT CONCAT(id,'/',CAST(d AS CHAR),'/',CAST(dt AS CHAR)) FROM vp_cli_dates ORDER BY id"
				original := []string{"1/2020-02-31/2020-02-31 12:00:00.123456"}
				if got := vpCLIRows(t, mgr.Source, query); !reflect.DeepEqual(got, original) {
					t.Fatalf("CLI fixture: source temporal text=%v, want %v", got, original)
				}
				if got := vpCLIRows(t, mgr.Destination, query); len(got) != 0 {
					t.Fatalf("CLI fixture: destination is not empty: %v", got)
				}
				var flags []string
				if skip {
					flags = append(flags, "--skip-verify")
				}
				output, exit := vpCLIRun(t, binary, cfg, command, name, flags...)
				wantSuccess := profile == "raw" || (skip && (profile == "empty" || command == "dry-run"))
				if (exit == 0) != wantSuccess {
					t.Errorf("CLI conversion contract: unexpected exit=%d, want success=%v", exit, wantSuccess)
				}
				if !wantSuccess && !(profile == "default" && command == "archive") && !strings.Contains(output, "INSERT_DIAGNOSTIC_REJECTED") {
					t.Error("CLI conversion contract: unexpected diagnostic refusal identifier")
				}
				if !wantSuccess && profile == "default" && command == "archive" && !strings.Contains(output, "1292") {
					t.Error("CLI conversion contract: expected strict plain-INSERT SQL error 1292")
				}
				if skip {
					vpCLIAssertOverrideNotice(t, output, command)
				}
				if skip && profile != "raw" && wantSuccess {
					message := "Completed with accepted conversion warnings"
					if command == "dry-run" {
						message = "Sample rolled back with accepted conversion warnings"
					}
					if !strings.Contains(output, message) {
						t.Errorf("CLI conversion contract: missing conversion result notice %q", message)
					}
				}
				source, dest := vpCLIRows(t, mgr.Source, query), vpCLIRows(t, mgr.Destination, query)
				statuses := readLogStatuses(t, mgr.Destination, logTable)
				t.Logf("raw source=%v destination=%v markers=%v", source, dest, statuses)
				archived := command == "archive" && wantSuccess
				if archived {
					want := original
					if profile == "empty" {
						want = []string{"1/0000-00-00/0000-00-00 00:00:00.000000"}
					}
					if len(source) != 0 || !reflect.DeepEqual(dest, want) || statuses["1"] != LogStatusCompleted {
						t.Errorf("CLI conversion contract: unexpected raw value or persistence; want source empty, destination=%v, completed root", want)
					}
				} else {
					if !reflect.DeepEqual(source, original) || len(dest) != 0 {
						t.Error("CLI conversion contract: unexpected persistence after failure or dry-run")
					}
					vpCLIAssertNoCopyMarker(t, statuses, "CLI conversion contract")
				}
			})
		}
	}

	for _, command := range []string{"archive", "copy-only", "purge"} {
		for _, skip := range []bool{false, true} {
			for _, skipPreflight := range []bool{false, true} {
				t.Run(fmt.Sprintf("collision/%s/skip_%v/preflight_skip_%v", command, skip, skipPreflight), func(t *testing.T) {
					cfg, mgr, job := vpCollision(t)
					vpCLIAssertProfile(t, mgr.Destination, profile)
					name := fmt.Sprintf("vp_cli_key_%s_%v_%v", strings.ReplaceAll(command, "-", "_"), skip, skipPreflight)
					cfg.Jobs = map[string]config.JobConfig{name: *job}
					cfg.Logging.Format = "text"
					logTable := bootstrapJobTracking(t, mgr.Destination, cfg.Destination.Database, name, job.RootTable, command)
					queries := []struct {
						query string
						want  []string
					}{
						{"SELECT CAST(id AS CHAR) FROM vp_roots ORDER BY id", []string{"1", "2"}},
						{"SELECT CONCAT(CAST(d AS CHAR),'/',root_id,'/',x) FROM vp_keys ORDER BY d", []string{"2020-02-31/1/1", "2020-03-02/2/2"}},
						{"SELECT CONCAT(id,'/',CAST(parent_d AS CHAR)) FROM vp_leaves ORDER BY id", []string{"11/2020-02-31", "22/2020-03-02"}},
					}
					for _, q := range queries {
						if got := vpCLIRows(t, mgr.Source, q.query); !reflect.DeepEqual(got, q.want) {
							t.Fatalf("CLI fixture: collision source=%v, want %v", got, q.want)
						}
						if got := vpCLIRows(t, mgr.Destination, q.query); len(got) != 0 {
							t.Fatalf("CLI fixture: collision destination not empty: %v", got)
						}
					}
					var flags []string
					if skip {
						flags = append(flags, "--skip-verify")
					}
					if skipPreflight {
						flags = append(flags, "--skip-validate-preflight")
					}
					output, exit := vpCLIRun(t, binary, cfg, command, name, flags...)
					if exit == 0 || !strings.Contains(output, "TEMPORAL_READ_CONTRACT") {
						t.Errorf("CLI temporal identity guard bypassed by override: exit=%d; expected TEMPORAL_READ_CONTRACT", exit)
					}
					for _, q := range queries {
						source, dest := vpCLIRows(t, mgr.Source, q.query), vpCLIRows(t, mgr.Destination, q.query)
						t.Logf("collision raw source=%v destination=%v", source, dest)
						if !reflect.DeepEqual(source, q.want) || len(dest) != 0 {
							t.Error("CLI temporal identity guard bypassed by override: source graph changed or destination written")
						}
					}
					statuses := readLogStatuses(t, mgr.Destination, logTable)
					t.Logf("collision markers=%v", statuses)
					vpCLIAssertNoCopyMarker(t, statuses, "CLI temporal identity guard bypassed by override")
				})
			}
		}
	}
}

func vpCLIRun(t *testing.T, binary string, cfg *config.Config, command, job string, flags ...string) (string, int) {
	t.Helper()
	data, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatalf("CLI fixture YAML: %v", err)
	}
	path := filepath.Join(t.TempDir(), "job.yaml")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("CLI fixture config: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	args := append([]string{command, "--config", path, "--job", job}, flags...)
	cmd := exec.CommandContext(ctx, binary, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err = cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("CLI timed out, not a contract refusal: %v", ctx.Err())
	}
	exit := 0
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("CLI launch failed, not a contract refusal: %v", err)
		}
		exit = exitErr.ExitCode()
	}
	t.Logf("command=%s flags=%v exit=%d\nstdout:\n%s\nstderr:\n%s", command, flags, exit, stdout.String(), stderr.String())
	return stdout.String() + "\n" + stderr.String(), exit
}

func vpCLIRows(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		t.Fatalf("CLI raw evidence query: %v", err)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("CLI raw evidence scan: %v", err)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("CLI raw evidence iteration: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("CLI raw evidence close: %v", err)
	}
	return result
}

func vpCLIAssertProfile(t *testing.T, db *sql.DB, profile string) {
	t.Helper()
	var mode string
	if err := db.QueryRowContext(context.Background(), "SELECT @@GLOBAL.sql_mode").Scan(&mode); err != nil {
		t.Fatalf("CLI fixture mode: %v", err)
	}
	t.Logf("matrix profile=%s global_sql_mode=%s", profile, mode)
	switch profile {
	case "empty":
		if mode != "" {
			t.Fatalf("CLI fixture: empty profile has mode %q", mode)
		}
	case "raw":
		if mode != "ALLOW_INVALID_DATES" {
			t.Fatalf("CLI fixture: raw profile has mode %q", mode)
		}
	case "default":
		if !strings.Contains(mode, "STRICT_TRANS_TABLES") || strings.Contains(mode, "ALLOW_INVALID_DATES") {
			t.Fatalf("CLI fixture: default profile must reject invalid dates; mode=%q", mode)
		}
	}
}

func vpCLIAssertNoCopyMarker(t *testing.T, statuses map[string]LogStatus, failure string) {
	t.Helper()
	for pk, status := range statuses {
		if status == LogStatusCopied || status == LogStatusCompleted {
			t.Errorf("%s: root %s advanced to status %v", failure, pk, status)
		}
	}
}

func vpCLIAssertOverrideNotice(t *testing.T, output, command string) {
	t.Helper()
	clauses := []string{
		"SAFETY WARNING: skip_verification is enabled",
		"Copied values will not be compared. Recognized conversion/truncation warnings are accepted and reported.",
		"Archive may permanently DELETE originals after MySQL changes their copied values.",
		"SQL errors, unknown/incomplete diagnostics and temporal identity failures still stop the run.",
	}
	if command == "dry-run" {
		clauses = []string{"NOTICE: skip_verification is enabled for dry-run; recognized conversion warnings are accepted in the rolled-back sample. This sample does not prove destination equality."}
	}
	for _, clause := range clauses {
		if !strings.Contains(output, clause) {
			t.Errorf("CLI conversion contract: missing exact override notice %q", clause)
		}
	}
}
