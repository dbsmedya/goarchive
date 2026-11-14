package controlpath

import (
	"fmt"
	"os"
)

// JudgeSocket reports nil when fi, the result of an Lstat (which does not follow links),
// identifies a socket. Anything else -- a regular file, a directory, a symbolic link even to
// a socket, another reparse point -- is refused, saying what it is; the caller names the path
// and leaves the object where it is.
func JudgeSocket(fi os.FileInfo) error {
	if fi.Mode().Type() == os.ModeSocket {
		return nil
	}
	return fmt.Errorf("is %s, not a socket", describe(fi))
}

// JudgeDir reports nil when fi, the result of an Lstat, is a socket directory the run may
// trust (spec §4.1 rule 2): a real directory, not a link; where the platform has them, owned
// by euid and with no group or other permission bit. The owner and mode rules are not
// enforced on Windows. The caller names the path; nothing is changed, whatever is found.
func JudgeDir(fi os.FileInfo, euid int) error {
	if fi.Mode().Type() != os.ModeDir {
		return fmt.Errorf("is %s, not a directory", describe(fi))
	}
	return judgeOwnerAndMode(fi, euid)
}

// describe says what an Lstat found, in the words the refusals use.
func describe(fi os.FileInfo) string {
	switch m := fi.Mode(); {
	case m.IsRegular():
		return "a regular file"
	case m.IsDir():
		return "a directory"
	case m&os.ModeSymlink != 0:
		return "a symbolic link"
	case m&os.ModeSocket != 0:
		return "a socket"
	case m&os.ModeIrregular != 0:
		return "a file of another kind (a reparse point or other irregular file)"
	default:
		return "a file of another kind (" + m.Type().String() + ")"
	}
}
