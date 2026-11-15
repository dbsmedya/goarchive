package control

// identity is what a file system object is, captured when it was inspected or opened (spec §4.2
// Identities): the volume and the object's number on it. The run records the identity of its
// socket directory, its lock file and its socket when it creates or opens them, and later decides
// whether what is at a path is still its own by capturing the path's identity again and comparing
// the two. A path is only a name; a comparison proves nothing about a name.
//
// An identity is a plain value. It is captured by identityAt (an object at a path, not following a
// final link) or by identityOfFile (an open file, such as the held lock), and an error is reported
// then, at the time of capture. sameFile compares two captured identities and does no I/O: on
// Windows os.SameFile on two os.Lstat results would load each one's volume and file index lazily,
// at the first comparison, and an object replaced in between would be read from the replacement
// both times (batch 1 review, finding 1).
type identity struct {
	volume, index uint64
	captured      bool
}

// sameFile reports whether a and b are one object. An identity that was never captured is the
// same as nothing, itself included.
func sameFile(a, b identity) bool { return a.captured && b.captured && a == b }
