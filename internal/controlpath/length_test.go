package controlpath

import (
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// shortDir makes a directory whose path is short enough for sockets and has a non-ASCII byte
// pair in its name (the length rule counts bytes). t.TempDir is too long on macOS.
func shortDir(t *testing.T) string {
	t.Helper()
	base := "/tmp"
	if runtime.GOOS == "windows" {
		base = os.TempDir()
	}
	dir, err := os.MkdirTemp(base, "gcé")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// TestMaxLen pins the usable socket path in bytes, which excludes the terminating NUL: Go's
// SockaddrUnix refuses a name of len(raw.Path) bytes or more (spec fact 13, §9 item 4).
func TestMaxLen(t *testing.T) {
	want := map[string]int{"darwin": 103, "linux": 107, "windows": 107}[runtime.GOOS]
	if want == 0 {
		t.Skipf("no limit is recorded for %s", runtime.GOOS)
	}
	if got := MaxLen(); got != want {
		t.Fatalf("MaxLen() = %d, want %d on %s", got, want, runtime.GOOS)
	}
}

// TestPathLength is spec §7 *Path length*: paths of limit-1 and limit bytes are accepted and
// bind, limit+1 is refused with the length message and the platform refuses it too, with
// non-ASCII bytes in the directory; and the job-name budget is
// limit - len(socket_dir) - 1 - len(".sock").
func TestPathLength(t *testing.T) {
	limit := MaxLen()
	dir := shortDir(t)
	budget := limit - len(dir) - 1 - len(".sock")
	if budget < 2 {
		t.Skipf("the temporary directory %q leaves no room for a job name under %d bytes", dir, limit)
	}
	for _, c := range []struct {
		name  string
		delta int
		ok    bool
	}{
		{"limit_minus_1", -1, true},
		{"limit", 0, true},
		{"limit_plus_1", 1, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			job := strings.Repeat("a", budget+c.delta)
			path, err := Resolve(true, dir, job, os.Geteuid())
			want := filepath.Join(dir, job+".sock")
			if c.ok {
				if err != nil {
					t.Fatalf("Resolve: %v", err)
				}
				if path != want || len(path) != limit+c.delta {
					t.Fatalf("path %q (%d bytes), want %q (%d bytes)", path, len(path), want, limit+c.delta)
				}
				l, err := net.Listen("unix", path)
				if err != nil {
					t.Fatalf("a path of %d bytes must bind: %v", len(path), err)
				}
				_ = l.Close()
				return
			}
			if err == nil {
				t.Fatalf("Resolve accepted %d bytes over the limit of %d: %q", len(want), limit, path)
			}
			for _, part := range []string{want, strconv.Itoa(len(want)), strconv.Itoa(limit)} {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("the length message must name %q: %v", part, err)
				}
			}
			// The platform draws its line in the same place: the check is not stricter
			// than the bind, and a path over it never reaches the bind.
			if l, err := net.Listen("unix", want); err == nil {
				_ = l.Close()
				t.Fatalf("the platform bound %d bytes, over the recorded limit of %d", len(want), limit)
			}
		})
	}
}

// TestCheckLengthCountsBytes: the limit counts bytes, not characters.
func TestCheckLengthCountsBytes(t *testing.T) {
	limit := MaxLen()
	// A path of limit bytes made of two-byte characters: half as many characters as bytes.
	ok := string(filepath.Separator) + strings.Repeat("é", (limit-1)/2)
	if len(ok) > limit {
		t.Fatalf("fixture is %d bytes, over %d", len(ok), limit)
	}
	if err := CheckLength(ok); err != nil {
		t.Fatalf("%d bytes within the limit of %d: %v", len(ok), limit, err)
	}
	over := ok + "é"
	if len(over) <= limit {
		t.Fatalf("fixture is %d bytes, not over %d", len(over), limit)
	}
	if utf8Runes(over) > limit {
		t.Fatalf("fixture has %d characters; it must be over by bytes only", utf8Runes(over))
	}
	if err := CheckLength(over); err == nil {
		t.Fatalf("%d bytes (%d characters) accepted, over the limit of %d", len(over), utf8Runes(over), limit)
	}
}

func utf8Runes(s string) int { return len([]rune(s)) }
