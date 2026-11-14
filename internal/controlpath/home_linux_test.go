package controlpath

import (
	"strings"
	"testing"
)

// TestPasswdLookup: Linux reads /etc/passwd itself, so that every artifact, with cgo or
// without, behaves alike (plan §3 C1). The first entry for a uid wins, as getpwuid_r has it.
func TestPasswdLookup(t *testing.T) {
	const db = `# a comment
root:x:0:0:root:/root:/bin/bash

app:x:1000:1000:App,,,:/home/app:/bin/sh
dup:x:1000:1000::/home/dup:/bin/sh
nohome:x:2000:2000:::/bin/sh
relhome:x:2001:2001::rel/home:/bin/sh
noshell:x:2002:2002::/home/noshell
short:x:2003
badid:x:abc:1::/home/badid:/bin/sh
+@netgroup:::::::
last:x:2004:2004::/home/last:/bin/sh`
	for _, c := range []struct {
		uid   int
		home  string
		found bool
	}{
		{0, "/root", true},
		{1000, "/home/app", true},
		{2000, "", true},
		{2001, "rel/home", true},
		{2002, "/home/noshell", true},
		{2003, "", false},
		{2004, "/home/last", true}, // no trailing newline
		{4242, "", false},
		{-1, "", false},
	} {
		home, found, err := homeFromPasswd(strings.NewReader(db), c.uid)
		if err != nil || found != c.found || home != c.home {
			t.Errorf("uid %d: (%q, %v, %v), want (%q, %v, nil)", c.uid, home, found, err, c.home, c.found)
		}
	}
}
