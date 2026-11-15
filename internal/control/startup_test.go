package control

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

// Start (spec §4.2): the directory, the lock, the decision about a socket that is already there,
// and the bind. A path is only a name: every decision about "this run's" object is made by identity.

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

// TestClassifyStartAttempt (spec §7 row 20a): no error is live; ECONNREFUSED, and only it, is
// stale; EAGAIN, EACCES, a passed deadline, any error that reports itself as a timeout, and an
// error of no known kind are each undecided. Nothing but ECONNREFUSED is ever stale.
func TestClassifyStartAttempt(t *testing.T) {
	op := func(err error) error {
		return &net.OpError{Op: "dial", Net: "unix", Err: &os.SyscallError{Syscall: "connect", Err: err}}
	}
	for _, tc := range []struct {
		name string
		err  error
		want startVerdict
	}{
		{"no error", nil, startLive},
		{"refused", op(syscall.ECONNREFUSED), startStale},
		{"refused, bare", syscall.ECONNREFUSED, startStale},
		{"refused, wrapped", fmt.Errorf("start: %w", op(syscall.ECONNREFUSED)), startStale},
		{"try again", op(syscall.EAGAIN), startUndecided},
		{"permission", op(syscall.EACCES), startUndecided},
		{"reset", op(syscall.ECONNRESET), startUndecided},
		{"no such file", op(syscall.ENOENT), startUndecided},
		{"deadline", &net.OpError{Op: "dial", Net: "unix", Err: os.ErrDeadlineExceeded}, startUndecided},
		{"a timeout of no kind", timeoutError{}, startUndecided},
		{"a timeout wrapping nothing else", fmt.Errorf("dial: %w", timeoutError{}), startUndecided},
		{"of no known kind", errors.New("boom"), startUndecided},
	} {
		if got := classifyStartAttempt(tc.err); got != tc.want {
			t.Errorf("%s: classifyStartAttempt = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestRemoveChecksIdentityAgain (Codex r5): a stale socket replaced by another object after
// inspection and before removal is not removed; the same object is; one already gone is nothing to
// do.
func TestRemoveChecksIdentityAgain(t *testing.T) {
	dir := shortDir(t)
	path := staleSocket(t, dir, "s.sock")
	inspected := captured(t, path)

	// The object that was inspected is moved aside and kept alive, and another takes its name.
	if err := os.Rename(path, path+".inspected"); err != nil {
		t.Fatal(err)
	}
	staleSocket(t, dir, "s.sock")
	removed, err := removeIfSame(path, inspected)
	if err != nil || removed {
		t.Fatalf("removeIfSame on a replaced object = %v, %v, want it left", removed, err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Errorf("the replacement is gone: %v", err)
	}

	current := captured(t, path)
	if removed, err := removeIfSame(path, current); err != nil || !removed {
		t.Fatalf("removeIfSame on the same object = %v, %v, want it removed", removed, err)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Errorf("the object is still there: %v", err)
	}
	if removed, err := removeIfSame(path, current); err != nil || removed {
		t.Errorf("removeIfSame on a name already gone = %v, %v, want nothing done", removed, err)
	}

	// A regular file is never removed on the strength of another object's identity.
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if removed, err := removeIfSame(path, inspected); err != nil || removed {
		t.Errorf("removeIfSame on a regular file = %v, %v, want it left", removed, err)
	}
}

// TestStaleSocketReplaced (spec §7 row 23): after the holder is gone, a leftover socket file is
// replaced.
func TestStaleSocketReplaced(t *testing.T) {
	f := &fixture{t: t}
	o := baseOptions(t, f, archiveApplies)
	staleSocket(t, f.dir, "job.sock")
	srv, err := Start(o)
	if err != nil {
		t.Fatalf("Start() over a stale socket = %v", err)
	}
	defer srv.Close()
	if got := ask(t, f.path(), "help\n"); !strings.HasPrefix(got, "OK\nprotocol 1") {
		t.Errorf("the replaced socket answers %q", got)
	}
}

// TestNotASocket (spec §7 row 24): a regular file, a directory and a symbolic link at the socket
// path are each refused and left in place, byte for byte; the run's lock is released.
func TestNotASocket(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("creating a symbolic link needs a privilege on Windows")
	}
	for _, tc := range []struct {
		name string
		make func(t *testing.T, path string)
		keep func(t *testing.T, path string)
	}{
		{"a regular file", func(t *testing.T, p string) {
			if err := os.WriteFile(p, []byte("precious"), 0o600); err != nil {
				t.Fatal(err)
			}
		}, func(t *testing.T, p string) {
			if b, err := os.ReadFile(p); err != nil || string(b) != "precious" {
				t.Errorf("the file is %q, %v, want it untouched", b, err)
			}
		}},
		{"a directory", func(t *testing.T, p string) {
			if err := os.Mkdir(p, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(p, "inside"), []byte("in"), 0o600); err != nil {
				t.Fatal(err)
			}
		}, func(t *testing.T, p string) {
			if b, err := os.ReadFile(filepath.Join(p, "inside")); err != nil || string(b) != "in" {
				t.Errorf("the directory's content is %q, %v, want it untouched", b, err)
			}
		}},
		{"a symbolic link", func(t *testing.T, p string) {
			target := filepath.Join(filepath.Dir(p), "target")
			if err := os.WriteFile(target, []byte("t"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, p); err != nil {
				t.Fatal(err)
			}
		}, func(t *testing.T, p string) {
			if fi, err := os.Lstat(p); err != nil || fi.Mode()&os.ModeSymlink == 0 {
				t.Errorf("the link is %v, %v, want it left", fi, err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fixture{t: t}
			o := baseOptions(t, f, archiveApplies)
			tc.make(t, f.path())
			_, err := Start(o)
			var inUse *InUseError
			if err == nil || errors.As(err, &inUse) || !strings.Contains(err.Error(), f.path()) {
				t.Fatalf("Start() = %v, want a refusal that names the path and is not an in-use error", err)
			}
			tc.keep(t, f.path())
			lock, lerr := os.OpenFile(f.lockPath(), os.O_RDWR, 0)
			if lerr != nil {
				t.Fatal(lerr)
			}
			defer lock.Close()
			if err := lockFile(lock); err != nil {
				t.Errorf("the lock is held after a refused start: %v", err)
			}
		})
	}
}

// TestLiveLock (spec §7 row 18): a second server on the same path is refused while the first
// lives, with an error that is an InUseError (the command exits 4); the first goes on answering.
func TestLiveLock(t *testing.T) {
	a := startFixture(t, archiveApplies)
	f := &fixture{t: t, dir: a.dir}
	o := baseOptions(t, f, archiveApplies)
	o.Dir = a.dir
	_, err := Start(o)
	var inUse *InUseError
	if !errors.As(err, &inUse) {
		t.Fatalf("Start() = %v, want an InUseError", err)
	}
	if !inUse.Lock || !strings.Contains(err.Error(), a.lockPath()) || !strings.Contains(err.Error(), "already uses") {
		t.Errorf("error = %q, want it to name the lock file and say that a run of this job already uses this directory", err)
	}
	if got := a.ask("help"); !strings.HasPrefix(got, "OK\nprotocol 1") {
		t.Errorf("the first server after the refusal: %q", got)
	}
}

// TestLockNameRemoved (spec §7 row 19): the first server's lock file is unlinked while it runs. A
// second start obtains a lock on a new file, connects to the socket, finds it answering, refuses
// (an InUseError) and leaves the socket untouched; the first server still answers.
func TestLockNameRemoved(t *testing.T) {
	a := startFixture(t, archiveApplies)
	if err := os.Remove(a.lockPath()); err != nil {
		t.Fatal(err)
	}
	before := captured(t, a.path())
	f := &fixture{t: t, dir: a.dir}
	o := baseOptions(t, f, archiveApplies)
	o.Dir = a.dir
	_, err := Start(o)
	var inUse *InUseError
	if !errors.As(err, &inUse) {
		t.Fatalf("Start() = %v, want an InUseError", err)
	}
	if inUse.Lock || !strings.Contains(err.Error(), a.path()) || !strings.Contains(err.Error(), "lock file was removed or replaced") {
		t.Errorf("error = %q, want it to name the socket and say its lock file was removed or replaced", err)
	}
	if !sameFile(before, captured(t, a.path())) {
		t.Error("the live socket was touched")
	}
	if got := a.ask("help"); !strings.HasPrefix(got, "OK\nprotocol 1") {
		t.Errorf("the first server after the refusal: %q", got)
	}
}

// TestLiveListenerThatNeverAnswers (spec §7 row 20): a socket whose listener accepts and never
// answers counts as live: a made connection is enough.
func TestLiveListenerThatNeverAnswers(t *testing.T) {
	f := &fixture{t: t}
	o := baseOptions(t, f, archiveApplies)
	l, err := net.Listen("unix", f.path())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			defer c.Close()
		}
	}()
	_, err = Start(o)
	var inUse *InUseError
	if !errors.As(err, &inUse) || inUse.Lock {
		t.Fatalf("Start() = %v, want an InUseError about the socket", err)
	}
}

// TestStartLeavesAnUndecidedSocket: nothing but ECONNREFUSED removes a socket. A socket the run
// may not connect to is undecided: the start refuses (exit 1, not an InUseError) and leaves it.
func TestStartLeavesAnUndecidedSocket(t *testing.T) {
	if os.Geteuid() == 0 || runtime.GOOS == "windows" {
		t.Skip("a socket with no permission bits refuses a connect only to a user other than root")
	}
	f := &fixture{t: t}
	o := baseOptions(t, f, archiveApplies)
	path := staleSocket(t, f.dir, "job.sock")
	if err := os.Chmod(path, 0); err != nil {
		t.Fatal(err)
	}
	before := captured(t, path)
	_, err := Start(o)
	var inUse *InUseError
	if err == nil || errors.As(err, &inUse) || !strings.Contains(err.Error(), path) {
		t.Fatalf("Start() = %v, want a refusal that names the socket and is not an InUseError", err)
	}
	if !sameFile(before, captured(t, path)) {
		t.Error("an undecided socket was removed or replaced")
	}
}

// TestDefaultDirectoryCreation (spec §7 row 27): under a home the test owns, `.goarchive` and `run`
// are created 0700 when absent; an existing `run` that breaks rule 2 is refused and left as it is.
func TestDefaultDirectoryCreation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("directory modes are not enforced on Windows")
	}
	home := shortDir(t)
	f := &fixture{t: t}
	o := baseOptions(t, f, archiveApplies)
	o.Dir, o.DefaultDir = filepath.Join(home, ".goarchive", "run"), true
	srv, err := Start(o)
	if err != nil {
		t.Fatalf("Start() = %v", err)
	}
	for _, p := range []string{filepath.Join(home, ".goarchive"), o.Dir} {
		if fi, err := os.Lstat(p); err != nil || !fi.IsDir() || fi.Mode().Perm() != 0o700 {
			t.Errorf("%s = %v, %v, want a directory of mode 0700", p, fi, err)
		}
	}
	_ = srv.Close()

	// An existing run directory that is open to others is refused and left as it is.
	other := shortDir(t)
	if err := os.MkdirAll(filepath.Join(other, ".goarchive", "run"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(other, ".goarchive", "run"), 0o755); err != nil {
		t.Fatal(err)
	}
	o.Dir = filepath.Join(other, ".goarchive", "run")
	if _, err := Start(o); err == nil || !strings.Contains(err.Error(), o.Dir) {
		t.Errorf("Start() over a 0755 run directory = %v, want a refusal naming it", err)
	}
	if fi, err := os.Lstat(o.Dir); err != nil || fi.Mode().Perm() != 0o755 {
		t.Errorf("the run directory is %v, %v, want it left at 0755", fi, err)
	}
}

// TestDefaultDirectoryRefusals: a home that does not exist, and a home of "/" (the image's
// `nobody`), are refused with the remedy before anything is created.
func TestDefaultDirectoryRefusals(t *testing.T) {
	f := &fixture{t: t}
	o := baseOptions(t, f, archiveApplies)
	o.DefaultDir = true

	missing := filepath.Join(shortDir(t), "no-such-home")
	o.Dir = filepath.Join(missing, ".goarchive", "run")
	if _, err := Start(o); err == nil || !strings.Contains(err.Error(), strconv.Quote(missing)) {
		t.Errorf("Start() under a missing home = %v, want it named (quoted: %s)", err, strconv.Quote(missing))
	}
	if _, err := os.Lstat(missing); !os.IsNotExist(err) {
		t.Errorf("a missing home was created: %v", err)
	}

	root := string(filepath.Separator)
	if v := filepath.VolumeName(os.TempDir()); v != "" {
		root = v + root
	}
	o.Dir = filepath.Join(root, ".goarchive", "run")
	_, err := Start(o)
	if err == nil || !strings.Contains(err.Error(), "control.socket_dir") {
		t.Errorf("Start() under a home of %q = %v, want the control.socket_dir remedy", root, err)
	}
	if _, serr := os.Lstat(filepath.Join(root, ".goarchive")); serr == nil && err != nil {
		t.Errorf("%s was created under the root", filepath.Join(root, ".goarchive"))
	}
}

// TestDirectoryRules (spec §7 row 28): a configured socket_dir has its last element created 0700
// when absent and the run fails when its parent is absent; an existing 0755 directory, a symbolic
// link and a regular file at its name are each refused and left as they are; the owner rule is the
// judging function's.
func TestDirectoryRules(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("directory modes and owners are not enforced on Windows")
	}
	base := shortDir(t)
	start := func(dir string, mod ...func(*Options)) (*Server, error) {
		f := &fixture{t: t}
		o := baseOptions(t, f, archiveApplies)
		o.Dir = dir
		for _, m := range mod {
			m(&o)
		}
		return Start(o)
	}

	t.Run("last element created", func(t *testing.T) {
		dir := filepath.Join(base, "new")
		srv, err := start(dir)
		if err != nil {
			t.Fatalf("Start() = %v", err)
		}
		defer srv.Close()
		if fi, err := os.Lstat(dir); err != nil || fi.Mode().Perm() != 0o700 {
			t.Errorf("%s = %v, %v, want 0700", dir, fi, err)
		}
	})

	t.Run("parent absent", func(t *testing.T) {
		dir := filepath.Join(base, "absent-parent", "run")
		if _, err := start(dir); err == nil || !strings.Contains(err.Error(), filepath.Join(base, "absent-parent")) {
			t.Errorf("Start() = %v, want a refusal naming the missing parent", err)
		}
		if _, err := os.Lstat(filepath.Join(base, "absent-parent")); !os.IsNotExist(err) {
			t.Errorf("a missing parent was created: %v", err)
		}
	})

	t.Run("open directory", func(t *testing.T) {
		dir := filepath.Join(base, "open")
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := start(dir); err == nil || !strings.Contains(err.Error(), dir) {
			t.Errorf("Start() = %v, want a refusal naming the directory", err)
		}
		if fi, _ := os.Lstat(dir); fi.Mode().Perm() != 0o755 {
			t.Errorf("the directory is %v, want it left at 0755", fi.Mode())
		}
	})

	t.Run("symbolic link", func(t *testing.T) {
		real := filepath.Join(base, "real")
		if err := os.Mkdir(real, 0o700); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(base, "link")
		if err := os.Symlink(real, link); err != nil {
			t.Fatal(err)
		}
		if _, err := start(link); err == nil || !strings.Contains(err.Error(), link) {
			t.Errorf("Start() = %v, want a refusal naming the link", err)
		}
		if fi, err := os.Lstat(link); err != nil || fi.Mode()&os.ModeSymlink == 0 {
			t.Errorf("the link is %v, %v, want it left", fi, err)
		}
	})

	t.Run("regular file", func(t *testing.T) {
		file := filepath.Join(base, "file")
		if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := start(file); err == nil || !strings.Contains(err.Error(), file) {
			t.Errorf("Start() = %v, want a refusal naming the file", err)
		}
		if b, _ := os.ReadFile(file); string(b) != "x" {
			t.Errorf("the file is %q, want it left", b)
		}
	})

	t.Run("owner", func(t *testing.T) {
		dir := filepath.Join(base, "owned")
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		_, err := start(dir, func(o *Options) { o.Euid = os.Geteuid() + 1 })
		if err == nil || !strings.Contains(err.Error(), "owned by") {
			t.Errorf("Start() under another effective uid = %v, want the owner rule", err)
		}
	})
}

// TestStartIsRepeatable: a server that ended leaves its directory ready for the next: the lock file
// stays, the socket is gone, and a new start binds at once.
func TestStartIsRepeatable(t *testing.T) {
	f := startFixture(t, archiveApplies)
	if err := f.srv.Close(); err != nil {
		t.Fatal(err)
	}
	g := &fixture{t: t}
	o := baseOptions(t, g, archiveApplies)
	o.Dir = f.dir
	srv, err := Start(o)
	if err != nil {
		t.Fatalf("a second Start() = %v", err)
	}
	defer srv.Close()
	if got := ask(t, srv.Path(), "help\n"); !strings.HasPrefix(got, "OK\nprotocol 1") {
		t.Errorf("the second server answers %q", got)
	}
	if srv.Path() != f.path() {
		t.Errorf("Path() = %q, want %q", srv.Path(), f.path())
	}
}
