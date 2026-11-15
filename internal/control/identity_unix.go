//go:build unix

package control

import (
	"errors"
	"os"
	"syscall"
)

// identityAt captures the identity of the object at path, without following a final symbolic
// link: the device and inode of its Lstat.
func identityAt(path string) (identity, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return identity{}, err
	}
	return identityOfInfo(fi)
}

// identityOfFile captures the identity of an open file from its descriptor (fstat): the held lock
// file, whatever its name has become.
func identityOfFile(f *os.File) (identity, error) {
	fi, err := f.Stat()
	if err != nil {
		return identity{}, err
	}
	return identityOfInfo(fi)
}

func identityOfInfo(fi os.FileInfo) (identity, error) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return identity{}, errors.New("the file's status carries no device and inode")
	}
	return identity{volume: uint64(st.Dev), index: uint64(st.Ino), captured: true}, nil
}
