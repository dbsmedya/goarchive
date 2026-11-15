package control

import (
	"io/fs"
	"os"

	"golang.org/x/sys/windows"
)

// identityAt captures the identity of the object at path now: it opens a handle on it without
// following a link (FILE_FLAG_OPEN_REPARSE_POINT, which also opens an AF_UNIX socket's reparse
// point itself; FILE_FLAG_BACKUP_SEMANTICS, which a directory needs) and reads the volume serial
// number and file index from the handle (GetFileInformationByHandle), as os.SameFile would
// later and lazily. The handle is closed before it returns; the identity is a plain value.
func identityAt(path string) (identity, error) {
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return identity{}, &fs.PathError{Op: "identity", Path: path, Err: err}
	}
	h, err := windows.CreateFile(name, 0,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil,
		windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		return identity{}, &fs.PathError{Op: "identity", Path: path, Err: err}
	}
	defer func() { _ = windows.CloseHandle(h) }()
	return handleIdentity(path, h)
}

// identityOfFile captures the identity of an open file from its own handle: the held lock file.
func identityOfFile(f *os.File) (identity, error) {
	return handleIdentity(f.Name(), windows.Handle(f.Fd()))
}

func handleIdentity(name string, h windows.Handle) (identity, error) {
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(h, &info); err != nil {
		return identity{}, &fs.PathError{Op: "identity", Path: name, Err: err}
	}
	return identity{
		volume:   uint64(info.VolumeSerialNumber),
		index:    uint64(info.FileIndexHigh)<<32 | uint64(info.FileIndexLow),
		captured: true,
	}, nil
}
