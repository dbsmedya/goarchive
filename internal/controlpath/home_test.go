package controlpath

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// unknownUID is a uid that no user database is expected to hold.
const unknownUID = 2147482000

// absHome is an absolute home directory for the platform under test.
func absHome() string {
	if runtime.GOOS == "windows" {
		return `C:\Users\gc-test`
	}
	return "/home/gc-test"
}

// TestHomeIgnoresEnvironment is spec §7 *Default directory* (the in-process half) and §9
// item 1: the default directory is <home>/.goarchive/run with <home> as the user database
// gives it for the effective user, and a different $HOME, $USER or %USERPROFILE% does not
// change it. Where the effective uid has no entry (L5: a container run as a uid the image
// does not know, with USER and HOME set), the default is refused with the remedy named and
// nothing is created under $HOME.
func TestHomeIgnoresEnvironment(t *testing.T) {
	euid := os.Geteuid()
	home, err := HomeOf(euid)

	envHome := filepath.Join(t.TempDir(), "env-home")
	for _, k := range []string{"HOME", "USERPROFILE", "LOCALAPPDATA"} {
		t.Setenv(k, envHome)
	}
	for _, k := range []string{"USER", "LOGNAME", "USERNAME"} {
		t.Setenv(k, "gc-env-user")
	}

	home2, err2 := HomeOf(euid)
	if home != home2 || (err == nil) != (err2 == nil) {
		t.Fatalf("the environment changed the home: %q, %v before; %q, %v after", home, err, home2, err2)
	}
	if home2 == envHome {
		t.Fatalf("the home is the environment's: %q", home2)
	}

	dbHome, found, have := databaseHome(t, euid)
	if have {
		if found && filepath.IsAbs(dbHome) {
			if err != nil || home != dbHome {
				t.Fatalf("HomeOf(%d) = %q, %v; the user database records %q", euid, home, err, dbHome)
			}
		} else if err == nil {
			t.Fatalf("HomeOf(%d) = %q, but the user database has no usable home (found %v, %q)", euid, home, found, dbHome)
		}
	}

	path, perr := Resolve(true, "", "job", euid)
	if err != nil {
		var nd *NoDefaultError
		if !errors.As(perr, &nd) {
			t.Fatalf("Resolve without a home: %v, want a *NoDefaultError", perr)
		}
		if !strings.Contains(perr.Error(), "control.socket_dir") {
			t.Fatalf("the refusal must name the remedy: %v", perr)
		}
		if runtime.GOOS != "windows" && !strings.Contains(perr.Error(), strconv.Itoa(euid)) {
			t.Fatalf("the refusal must name the uid %d: %v", euid, perr)
		}
		if _, serr := os.Lstat(envHome); !os.IsNotExist(serr) {
			t.Fatalf("something exists under the environment's home: %v", serr)
		}
		return
	}
	want := filepath.Join(home, ".goarchive", "run", "job.sock")
	if perr != nil || path != want {
		t.Fatalf("Resolve(true, \"\", job) = %q, %v; want %q", path, perr, want)
	}
	if _, serr := os.Lstat(envHome); !os.IsNotExist(serr) {
		t.Fatalf("resolving created something under the environment's home: %v", serr)
	}
}

// TestHomeOfUnknownUID: a uid with no entry has no default, whatever the environment says.
func TestHomeOfUnknownUID(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows takes the profile of the process token, not a uid")
	}
	t.Setenv("HOME", t.TempDir())
	t.Setenv("USER", "gc-env-user")
	home, err := HomeOf(unknownUID)
	if err == nil {
		t.Skipf("uid %d has an entry here (home %q)", unknownUID, home)
	}
	var nd *NoDefaultError
	if !errors.As(err, &nd) || nd.UID != unknownUID {
		t.Fatalf("HomeOf(%d) = %v, want a *NoDefaultError for that uid", unknownUID, err)
	}
	for _, part := range []string{strconv.Itoa(unknownUID), "control.socket_dir"} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("the message must contain %q: %v", part, err)
		}
	}
}

// TestJudgeHome: an empty or relative home has no default (spec §4.1, "no absolute home").
func TestJudgeHome(t *testing.T) {
	good, err := judgeHome(1000, absHome())
	if err != nil || good != absHome() {
		t.Fatalf("judgeHome(abs) = %q, %v", good, err)
	}
	for _, c := range []struct{ name, home, reason string }{
		{"empty", "", "no home directory"},
		{"relative", "home/gc", "relative"},
		{"dot", ".", "relative"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := judgeHome(1000, c.home)
			var nd *NoDefaultError
			if !errors.As(err, &nd) || nd.UID != 1000 {
				t.Fatalf("judgeHome(%q) = %v, want a *NoDefaultError for uid 1000", c.home, err)
			}
			for _, part := range []string{"1000", c.reason, "control.socket_dir"} {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("the message must contain %q: %v", part, err)
				}
			}
		})
	}
}

// TestResolve covers the resolver's rules around the lookup (spec §4.1, plan §2 r5 minor 1):
// it is side-effect free, makes no lookup without need, and refuses what cannot be a path.
func TestResolve(t *testing.T) {
	dir := shortDir(t)

	t.Run("disabled makes no lookup and no path", func(t *testing.T) {
		// A uid with no entry would fail a lookup, and a bad name would fail the name rule.
		for _, job := range []string{"job", "a/b"} {
			path, err := Resolve(false, "", job, unknownUID)
			if path != "" || err != nil {
				t.Fatalf("Resolve(false, job %q) = %q, %v", job, path, err)
			}
		}
	})
	t.Run("an explicit directory makes no lookup", func(t *testing.T) {
		path, err := Resolve(true, dir, "job", unknownUID)
		if err != nil || path != filepath.Join(dir, "job.sock") {
			t.Fatalf("Resolve = %q, %v", path, err)
		}
	})
	t.Run("creates nothing", func(t *testing.T) {
		missing := filepath.Join(dir, "not", "yet")
		if _, err := Resolve(true, missing, "job", unknownUID); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(filepath.Join(dir, "not")); !os.IsNotExist(err) {
			t.Fatalf("Resolve created a directory: %v", err)
		}
	})
	t.Run("a relative directory is refused", func(t *testing.T) {
		_, err := Resolve(true, filepath.Join("run", "control"), "job", unknownUID)
		if err == nil || !strings.Contains(err.Error(), "absolute") {
			t.Fatalf("Resolve(relative) = %v, want a refusal naming the absolute rule", err)
		}
	})
	t.Run("an unsafe job name is refused", func(t *testing.T) {
		for _, job := range []string{"", "..", "a/b", ".hidden", "a b"} {
			_, err := Resolve(true, dir, job, unknownUID)
			if err == nil || !strings.Contains(err.Error(), strconv.Quote(job)) {
				t.Errorf("Resolve(job %q) = %v, want a refusal naming the job", job, err)
			}
		}
	})
	t.Run("the path is cleaned", func(t *testing.T) {
		sep := string(filepath.Separator)
		path, err := Resolve(true, dir+sep+sep, "job", unknownUID)
		if err != nil || path != dir+sep+"job.sock" {
			t.Fatalf("Resolve = %q, %v", path, err)
		}
	})
}
