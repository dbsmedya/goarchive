package cmd

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// execute runs the CLI with args, every flag reset to its default first, and returns what it
// printed and its error.
func execute(t *testing.T, args ...string) (string, error) {
	t.Helper()
	resetFlags(rootCmd)
	var out bytes.Buffer
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetArgs(args)
	defer func() {
		resetFlags(rootCmd)
		rootCmd.SetOut(nil)
		rootCmd.SetErr(nil)
		rootCmd.SetArgs(nil)
	}()
	_, err := rootCmd.ExecuteC()
	return out.String(), err
}

func resetFlags(c *cobra.Command) {
	reset := func(f *pflag.Flag) {
		_ = f.Value.Set(f.DefValue)
		f.Changed = false
	}
	c.PersistentFlags().VisitAll(reset)
	c.Flags().VisitAll(reset)
	for _, sub := range c.Commands() {
		resetFlags(sub)
	}
}

// writeConfig writes body to a config file in a test directory and returns its path.
func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "goarchive.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// unreachable is a 3.0 configuration whose servers refuse every connection.
const unreachable = `
source:
  host: 127.0.0.1
  port: 1
  user: u
  database: src
destination:
  host: 127.0.0.1
  port: 1
  user: u
  database: dst
jobs:
  purge_old:
    job_type: purge
    root_table: t
    primary_key: id
    where: "1=1"
  copy_old:
    job_type: copy-only
    root_table: t
    primary_key: id
    where: "1=1"
`

// TestCommandsCheckTheJobBeforeConnecting (spec §5, R4, M6-2): every command loads the file
// and picks the job before it opens a connection, so a wrong subcommand, a missing job, a
// 2.x file or a bad flag is refused at once, and never as a connection error.
func TestCommandsCheckTheJobBeforeConnecting(t *testing.T) {
	cfg := writeConfig(t, unreachable)
	old := writeConfig(t, "\nsource:\n  host: h\n  user: u\n  database: s\ndestination:\n  host: h\n  user: u\n  database: d\njobs:\n  j:\n    root_table: t\n    primary_key: id\n    where: \"1=1\"\n")
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"archive", "-c", cfg, "--job", "purge_old"}, `run it with "goarchive purge", not "goarchive archive"`},
		{[]string{"copy-only", "-c", cfg, "--job", "purge_old"}, `not "goarchive copy-only"`},
		{[]string{"purge", "-c", cfg, "--job", "copy_old"}, `run it with "goarchive copy-only", not "goarchive purge"`},
		{[]string{"purge", "-c", cfg, "--job", "missing"}, `job "missing" not found`},
		{[]string{"dry-run", "-c", cfg, "--job", "missing"}, `job "missing" not found`},
		{[]string{"validate", "-c", cfg, "--job", "missing"}, `job "missing" not found`},
		{[]string{"plan", "-c", cfg, "--job", "missing"}, `job "missing" not found`},
		{[]string{"purge", "-c", cfg, "--job", "Purge_Old"}, `did you mean "purge_old"?`},
		{[]string{"purge", "-c", cfg, "--job", "purge_old", "--verification=count"}, "unknown flag: --verification"},
		{[]string{"copy-only", "-c", cfg, "--job", "copy_old", "--verification=md5"}, `invalid --verification "md5": use sha256, count or none`},
		{[]string{"archive", "-c", cfg, "--job", "purge_old", "--skip-verify"}, "unknown flag: --skip-verify"},
		{[]string{"archive", "-c", cfg, "--job", "purge_old", "--force"}, "unknown flag: --force"},
		{[]string{"copy-only", "-c", cfg, "--job", "copy_old", "--force"}, "unknown flag: --force"},
		{[]string{"archive", "-c", old, "--job", "j"}, "docs/README_UPGRADING_3_0.md"},
		// Review Focus 4: a --progress interval below 1s is refused before any connection.
		{[]string{"purge", "-c", cfg, "--job", "purge_old", "--progress=500ms"}, `--progress interval "500ms" is too small`},
	} {
		_, err := execute(t, tc.args...)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%v = %v, want %q", tc.args, err, tc.want)
			continue
		}
		if strings.Contains(err.Error(), "connect") {
			t.Errorf("%v reached a connection: %v", tc.args, err)
		}
	}
}

// TestRunCommandsConnect: once the job is fit, archive, purge, copy-only, dry-run and
// validate go on to connect (they are wired, R45 lifted).
func TestRunCommandsConnect(t *testing.T) {
	cfg := writeConfig(t, unreachable)
	for _, args := range [][]string{
		{"purge", "-c", cfg, "--job", "purge_old"},
		{"copy-only", "-c", cfg, "--job", "copy_old", "--verification=count"},
		{"dry-run", "-c", cfg, "--job", "copy_old"},
		{"validate", "-c", cfg, "--job", "purge_old"},
	} {
		if _, err := execute(t, args...); err == nil || !strings.Contains(err.Error(), "failed to connect to source database") {
			t.Errorf("%v = %v, want a connection failure", args, err)
		}
	}
}

// TestFlagMatrix (P4): which command carries which flag. --force-triggers is on the commands
// whose preflight checks DELETE triggers: archive and purge, and validate and dry-run, which
// run their preflight. copy-only's profile has no such check (spec §8.1).
func TestFlagMatrix(t *testing.T) {
	all := []string{"job", "verification", "skip-validate-preflight", "force-triggers", "progress", "force", "skip-verify"}
	for name, want := range map[string][]string{
		"archive":   {"job", "verification", "skip-validate-preflight", "force-triggers", "progress"},
		"purge":     {"job", "skip-validate-preflight", "force-triggers", "progress"},
		"copy-only": {"job", "verification", "skip-validate-preflight", "progress"},
		"dry-run":   {"job", "verification", "skip-validate-preflight", "force-triggers"},
		"validate":  {"job", "verification", "force-triggers"},
		"plan":      {"job"},
		"list-jobs": nil,
	} {
		c, _, err := rootCmd.Find([]string{name})
		if err != nil || c.Name() != name {
			t.Fatalf("no %s command: %v", name, err)
		}
		for _, f := range all {
			if has := c.Flags().Lookup(f) != nil; has != slices.Contains(want, f) {
				t.Errorf("%s --%s registered = %v, want %v", name, f, has, !has)
			}
		}
	}
}
