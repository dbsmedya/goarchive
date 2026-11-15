//go:build !unix

package control

import "os"

// inspect looks at path without following a final link and captures its identity. Here the two
// are separate calls: identityAt opens the object itself, and there is no file status to take both
// from.
func inspect(path string) (os.FileInfo, identity, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return nil, identity{}, err
	}
	id, err := identityAt(path)
	return fi, id, err
}
