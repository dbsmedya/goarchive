package control

import (
	"errors"
	"fmt"

	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// peerDeniedError is a connection the run refuses: it did not come from the run's own user, or its
// credentials could not be read. Its message goes to the peer as `ERR denied` and to the log.
type peerDeniedError struct {
	uid, euid int
	cause     error
}

func (e *peerDeniedError) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("this control socket accepts only the run's own user (uid %d), and the credentials of the connection could not be read: %v", e.euid, e.cause)
	}
	return fmt.Sprintf("this control socket accepts only the run's own user (uid %d); the connection came from uid %d", e.euid, e.uid)
}

// judgePeer is the same-user rule (spec §4.1, D11): the peer's uid must equal the run's effective
// uid, every other uid refused, uid 0 included, and a connection whose credentials cannot be read
// refused the same way. Where the platform has no peer credentials (Windows) the rule is not
// enforced: enforced is false there, and a change is logged without a uid.
func judgePeer(uid, pid int, err error, euid int, enforced bool) (tuning.Peer, error) {
	switch {
	case err != nil && !enforced && errors.Is(err, errNoPeerCredentials):
		return tuning.Peer{UID: -1}, nil
	case err != nil:
		return tuning.Peer{}, &peerDeniedError{uid: uid, euid: euid, cause: err}
	case enforced && uid != euid:
		return tuning.Peer{}, &peerDeniedError{uid: uid, euid: euid}
	}
	return tuning.Peer{UID: uid, PID: pid}, nil
}
