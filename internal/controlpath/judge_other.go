//go:build !unix

package controlpath

import "os"

// judgeOwnerAndMode: the directory's owner and mode are not enforced here (Windows), where
// the same-user rule is not enforceable either (spec §4.1).
func judgeOwnerAndMode(os.FileInfo, int) error { return nil }
