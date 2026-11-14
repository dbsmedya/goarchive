//go:build unix

package controlpath

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestJudgeDirOwnerAndMode is spec §7 *Directory rules*, the judging function's half: the
// directory must be owned by the run's effective user and carry no group or other bit.
func TestJudgeDirOwnerAndMode(t *testing.T) {
	euid := os.Geteuid()
	dir := shortDir(t)

	for _, c := range []struct {
		mode os.FileMode
		ok   bool
	}{
		{0o700, true},
		{0o500, true},
		{0o755, false},
		{0o750, false},
		{0o705, false},
		{0o770, false},
		{0o777, false},
		{0o710, false},
	} {
		t.Run(fmt.Sprintf("mode_%04o", c.mode), func(t *testing.T) {
			sub := filepath.Join(dir, fmt.Sprintf("m%04o", c.mode))
			if err := os.Mkdir(sub, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(sub, c.mode); err != nil { // Mkdir applies the umask
				t.Fatal(err)
			}
			err := JudgeDir(lstat(t, sub), euid)
			switch {
			case c.ok && err != nil:
				t.Fatalf("JudgeDir(%04o) = %v", c.mode, err)
			case !c.ok && err == nil:
				t.Fatalf("JudgeDir(%04o) accepted group or other bits", c.mode)
			case !c.ok && !strings.Contains(err.Error(), "group or other"):
				t.Fatalf("the refusal must say what it found: %v", err)
			}
		})
	}

	t.Run("owned by another user", func(t *testing.T) {
		// The owner rule is judged against the uid given, so a directory of this user
		// stands for one of another user.
		other := euid + 1
		err := JudgeDir(lstat(t, dir), other)
		if err == nil {
			t.Fatalf("JudgeDir accepted a directory of uid %d for uid %d", euid, other)
		}
		for _, part := range []string{fmt.Sprint(euid), fmt.Sprint(other), "owned"} {
			if !strings.Contains(err.Error(), part) {
				t.Errorf("the refusal must contain %q: %v", part, err)
			}
		}
	})
}
