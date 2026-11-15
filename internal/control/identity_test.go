package control

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// Identities (spec §4.2, review R1 finding 1): the run records the identity of its directory, its
// lock file and its socket when it creates or opens them, and decides later by comparing what is
// at the path with what it recorded. The identity must be captured when the object is inspected
// or opened, not when two of them are first compared: on Windows os.Lstat saves only the path and
// loads the volume and file index lazily, on the first os.SameFile call, so a replacement made
// between two Lstat calls could be read from the replacement both times. These tests never compare
// before the replacement, which would prime the old identity and hide that.

func captured(t *testing.T, path string) identity {
	t.Helper()
	id, err := identityAt(path)
	if err != nil {
		t.Fatalf("identityAt(%s): %v", path, err)
	}
	return id
}

// makeObject creates the kind of object a run records at path.
type makeObject func(t *testing.T, path string)

func makeDir(t *testing.T, path string) {
	t.Helper()
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
}

func makeFile(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
}

func makeSocket(t *testing.T, path string) {
	t.Helper()
	staleSocket(t, filepath.Dir(path), filepath.Base(path))
}

var objects = []struct {
	name string
	make makeObject
}{
	{"directory", makeDir},
	{"ordinary file", makeFile},
	{"socket", makeSocket},
}

// TestIdentityDetectsReplacement: capture, move the object away (it stays alive, so no file index
// can be reused), put a new one at the path, capture, and only then compare: they differ. The
// identity belongs to the object, not to the name: the old object, now at another name, is still
// equal to what was captured.
func TestIdentityDetectsReplacement(t *testing.T) {
	for _, o := range objects {
		t.Run(o.name, func(t *testing.T) {
			dir := shortDir(t)
			path := filepath.Join(dir, "obj")
			aside := filepath.Join(dir, "obj.old")
			o.make(t, path)
			before := captured(t, path) // no comparison yet: nothing primes the old identity

			if err := os.Rename(path, aside); err != nil {
				t.Fatal(err)
			}
			o.make(t, path)
			after := captured(t, path)

			if sameFile(before, after) {
				t.Fatal("a new object at the old name has the old object's identity")
			}
			if !sameFile(before, captured(t, aside)) {
				t.Fatal("the old object, moved aside, is not the object that was captured")
			}
			if !sameFile(after, captured(t, path)) {
				t.Fatal("the new object is not the object now at the name")
			}
		})
	}
}

// TestIdentityUnchanged: what has not been replaced compares equal, for a directory, a file and a
// socket, captured twice.
func TestIdentityUnchanged(t *testing.T) {
	for _, o := range objects {
		t.Run(o.name, func(t *testing.T) {
			path := filepath.Join(shortDir(t), "obj")
			o.make(t, path)
			if a, b := captured(t, path), captured(t, path); !sameFile(a, b) {
				t.Fatal("an unchanged object is not the same as itself")
			}
		})
	}
}

// TestIdentityIsCapturedNotLoaded: comparing two captured identities does no I/O. The captured
// object is deleted first; the comparison still gives the answer it gave while it existed.
func TestIdentityIsCapturedNotLoaded(t *testing.T) {
	dir := shortDir(t)
	one, two := filepath.Join(dir, "one"), filepath.Join(dir, "two")
	makeFile(t, one)
	makeFile(t, two)
	a, b := captured(t, one), captured(t, two)
	if err := os.Remove(one); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(two); err != nil {
		t.Fatal(err)
	}
	if !sameFile(a, a) {
		t.Error("a captured identity is not the same as itself once its object is gone")
	}
	if sameFile(a, b) {
		t.Error("two captured identities are the same once their objects are gone")
	}
}

// TestIdentityOfAHeldFile: the run's lock file is open and held, so its identity is taken from its
// handle: it equals what the path gives while the name is unchanged. Where a held file can be
// moved (not on Windows, whose open files cannot be renamed), a replacement at the name differs
// from the held one, and the held file at its new name still equals it.
func TestIdentityOfAHeldFile(t *testing.T) {
	dir := shortDir(t)
	path := filepath.Join(dir, "job.lock")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := lockFile(f); err != nil {
		t.Fatal(err)
	}
	held, err := identityOfFile(f)
	if err != nil {
		t.Fatalf("identityOfFile: %v", err)
	}
	if !sameFile(held, captured(t, path)) {
		t.Fatal("a held lock file's handle and its unchanged name give different identities")
	}
	if runtime.GOOS == "windows" {
		return
	}
	aside := filepath.Join(dir, "job.lock.old")
	if err := os.Rename(path, aside); err != nil {
		t.Fatal(err)
	}
	makeFile(t, path)
	if sameFile(held, captured(t, path)) {
		t.Fatal("a file made at the lock's old name has the held lock's identity")
	}
	if !sameFile(held, captured(t, aside)) {
		t.Fatal("the held lock file, moved aside, is not the held identity")
	}
}

// TestIdentityDoesNotFollowALink: a symbolic link has an identity of its own, not its target's: the
// run asks about what is at the path, and a link there is not the directory or the socket behind it.
func TestIdentityDoesNotFollowALink(t *testing.T) {
	dir := shortDir(t)
	target := filepath.Join(dir, "target")
	makeDir(t, target)
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("cannot make a symbolic link here: %v", err)
	}
	if sameFile(captured(t, link), captured(t, target)) {
		t.Fatal("the identity of a symbolic link is its target's: the capture followed the link")
	}
}

// TestIdentityErrors: a path that is not there fails when the identity is captured, saying so, and
// the zero identity is the same as nothing, itself included.
func TestIdentityErrors(t *testing.T) {
	_, err := identityAt(filepath.Join(shortDir(t), "missing"))
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("identityAt(missing) = %v, want it to wrap fs.ErrNotExist", err)
	}
	if sameFile(identity{}, identity{}) {
		t.Fatal("two uncaptured identities are the same")
	}
	if sameFile(identity{}, captured(t, shortDir(t))) {
		t.Fatal("an uncaptured identity is the same as a captured one")
	}
}
