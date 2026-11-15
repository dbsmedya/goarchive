package control

import (
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// The identity check (spec §4.2, D18). Once a minute the run compares what is at its directory's,
// its lock's and its socket's path with the identities it recorded. A loss is reported once per
// object, loudly, through the emitter; the run repairs nothing: it creates no new lock file, which
// would be held by this process only from now on and say nothing about the past, and binds no new
// socket. The run itself continues, and a signal still stops it.

// defaultWatchEvery is how often the identities are compared.
const defaultWatchEvery = time.Minute

func (s *Server) watch() {
	defer close(s.watchDone)
	tick := time.NewTicker(s.o.tune.watchEvery)
	defer tick.Stop()
	for {
		select {
		case <-s.watchStop:
			return
		case <-tick.C:
			s.checkIdentities()
		}
	}
}

// stopWatch stops the identity check and waits for it, so that nothing it does can follow the end.
func (s *Server) stopWatch() {
	s.watchStopOnce.Do(func() { close(s.watchStop) })
	<-s.watchDone
}

func (s *Server) checkIdentities() {
	for _, o := range []struct {
		key, path string
		id        identity
		what      string
	}{
		{"directory", s.o.Dir, s.dirID, "Control socket directory lost: %q was removed or replaced; a second start is no longer refused by the lock, " +
			"and this run can no longer be reached over its control socket. The run repairs nothing and continues; a signal still stops it."},
		{"lock", s.lockPath, s.lockID, "Control lock lost: %q was removed or replaced; a second start is no longer refused by the lock " +
			"(a live socket still refuses it). The run repairs nothing and continues; a signal still stops it."},
		{"socket", s.sockPath, s.sockID, "Control socket lost: %q was removed or replaced; this run can no longer be reached over its control socket. " +
			"The run repairs nothing and continues; a signal still stops it."},
	} {
		if s.lost[o.key] {
			continue
		}
		current, err := identityAt(o.path)
		switch {
		case err == nil && sameFile(current, o.id):
			continue
		case err != nil && !errors.Is(err, fs.ErrNotExist):
			continue // a read that failed is not a loss; the next check asks again
		}
		s.lost[o.key] = true
		s.em.note(tuning.LevelWarn, tuning.Peer{UID: -1}, fmt.Sprintf(o.what, o.path))
	}
}
