package control

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/controlpath"
	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// Options builds a server.
type Options struct {
	// Job is the job's name, which names the socket and its lock; it must be a safe file name
	// (controlpath.JobNameSafe).
	Job string
	// Dir is the socket's directory, absolute. DefaultDir says that it is the default,
	// <home>/.goarchive/run, so that its two elements are created when absent; otherwise only its last
	// element is, and its parent must exist.
	Dir        string
	DefaultDir bool
	// Euid is the run's effective user id: the owner the directory must have, and the only uid a
	// connection may come from where the platform gives credentials.
	Euid int

	// Store is the run's settings store; every mutation goes through it.
	Store *tuning.Store
	// Info is what the status says of the run that does not change.
	Info Info
	// Tracker, Gate and Shutdown are the status's other sources. Gate is nil for a run without the
	// gate; Shutdown may be nil.
	Tracker  TrackerSource
	Gate     GateRecordSource
	Shutdown ShutdownSource
	// Sink receives the audit entries; Banner receives the WARNING of a tolerance raise above the
	// configured value, as the run banners are.
	Sink   Sink
	Banner func(string)

	// tune holds the values that tests change; its zero value is the production behaviour.
	tune timings
}

// timings are the server's constants and the seams a test uses to observe or shorten them.
type timings struct {
	connDeadline time.Duration // each connection's reads and writes; 5 s
	dialDeadline time.Duration // Start's connection attempt to an existing socket; 5 s
	watchEvery   time.Duration // the identity check; one minute
	handlers     int           // connections handled at once; 8

	// afterLock runs after the lock is held and before the socket path is looked at. afterBind runs
	// after the bind and the recording of the socket's identity, before the mode is set: an error
	// from it fails the start there. peer replaces the credential read and the same-user judgement.
	// Observers for tests; all nil in a run.
	afterLock func()
	afterBind func() error
	peer      func(net.Conn) (tuning.Peer, error)
}

const (
	defaultConnDeadline = 5 * time.Second
	defaultDialDeadline = 5 * time.Second
	defaultHandlers     = 8
)

func (t timings) withDefaults() timings {
	if t.connDeadline <= 0 {
		t.connDeadline = defaultConnDeadline
	}
	if t.dialDeadline <= 0 {
		t.dialDeadline = defaultDialDeadline
	}
	if t.watchEvery <= 0 {
		t.watchEvery = defaultWatchEvery
	}
	if t.handlers <= 0 {
		t.handlers = defaultHandlers
	}
	return t
}

// InUseError is a start that finds the control socket in use by a live run: the socket lock is held,
// or the lock was free and something accepted a connection on the socket. The command exits 4,
// "not started" (spec §4.11). Every other failure of a start exits 1.
type InUseError struct {
	// Path is the lock file (Lock true) or the socket that answered (Lock false).
	Path string
	Lock bool
	Job  string
	Dir  string
}

func (e *InUseError) Error() string {
	what := "another run holds the lock file " + e.Path
	if !e.Lock {
		what = "another run answers on the control socket " + e.Path + ", whose lock file was removed or replaced"
	}
	return fmt.Sprintf("the control socket is in use: %s; a run of the job %q already uses this socket directory (%s)", what, e.Job, e.Dir)
}

// Server is a run's control socket: the listener, the audit emitter and the identity check. It is
// started before the run connects to MySQL and ends after the run's final output is written.
type Server struct {
	o        Options
	sockPath string
	lockPath string

	lock     *os.File
	listener *net.UnixListener
	dirID    identity
	lockID   identity
	sockID   identity

	em   *emitter
	cmds *commands

	sem        chan struct{}
	closing    chan struct{}
	acceptDone chan struct{}
	handlers   sync.WaitGroup

	watchStop     chan struct{}
	watchDone     chan struct{}
	watchStopOnce sync.Once
	lost          map[string]bool // only the identity check reads or writes it

	closeOnce sync.Once
}

// Start brings the control socket up (spec §4.2 Start): it checks or creates the directory, takes
// the lock, decides what to do with a socket that is already there, binds, and starts serving. A
// start that fails after the bind removes the socket it created before it releases the lock; one
// that fails at the bind touches nothing at the path.
func Start(o Options) (srv *Server, err error) {
	switch {
	case o.Store == nil:
		return nil, errors.New("control: no settings store")
	case !controlpath.JobNameSafe(o.Job):
		return nil, fmt.Errorf("control: the job name %q cannot name a socket", o.Job)
	case !filepath.IsAbs(o.Dir):
		return nil, fmt.Errorf("control: the socket directory %q is not an absolute path", o.Dir)
	}
	if o.Tracker == nil {
		o.Tracker = noTracker{}
	}
	o.tune = o.tune.withDefaults()
	s := &Server{
		o:        o,
		sockPath: filepath.Join(o.Dir, o.Job+".sock"),
		lockPath: filepath.Join(o.Dir, o.Job+".lock"),
		sem:      make(chan struct{}, o.tune.handlers),
		closing:  make(chan struct{}), acceptDone: make(chan struct{}),
		watchStop: make(chan struct{}), watchDone: make(chan struct{}),
		lost: map[string]bool{},
	}
	defer func() {
		if err != nil {
			s.release()
		}
	}()

	// 1. The directory.
	if s.dirID, err = prepareDir(o); err != nil {
		return nil, err
	}

	// 2. The lock, kept open and held until the run ends.
	f, err := os.OpenFile(s.lockPath, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open the control socket's lock file %q: %w", s.lockPath, err)
	}
	s.lock = f
	switch err := lockFile(f); {
	case errors.Is(err, errLockHeld):
		return nil, &InUseError{Path: s.lockPath, Lock: true, Job: o.Job, Dir: o.Dir}
	case err != nil:
		return nil, err
	}
	if s.lockID, err = identityOfFile(f); err != nil {
		return nil, fmt.Errorf("record the identity of the lock file %q: %w", s.lockPath, err)
	}
	if o.tune.afterLock != nil {
		o.tune.afterLock()
	}

	// 4. What is at the socket path.
	if err = s.clearStaleSocket(); err != nil {
		return nil, err
	}

	// 5. Bind, record, set the mode, listen. Go's listener removes its path when it is closed; that
	// is switched off, so that the only removals of a socket path are the stale one's above and the
	// end's, both by identity.
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: s.sockPath, Net: "unix"})
	if err != nil {
		return nil, fmt.Errorf("bind the control socket %q: %w", s.sockPath, err)
	}
	l.SetUnlinkOnClose(false)
	s.listener = l
	if s.sockID, err = identityAt(s.sockPath); err != nil {
		return nil, fmt.Errorf("record the identity of the control socket %q: %w", s.sockPath, err)
	}
	if o.tune.afterBind != nil {
		if err = o.tune.afterBind(); err != nil {
			return nil, fmt.Errorf("start the control socket %q: %w", s.sockPath, err)
		}
	}
	if runtime.GOOS != "windows" {
		if err = os.Chmod(s.sockPath, 0o600); err != nil {
			return nil, fmt.Errorf("set the mode of the control socket %q: %w", s.sockPath, err)
		}
	}

	view := &view{
		info: o.Info, socket: s.sockPath, store: o.Store, tracker: o.Tracker,
		gate: o.Gate, shutdown: o.Shutdown, now: time.Now,
	}
	s.cmds = newCommands(o.Store, view, o.Info.SentinelFile)
	s.em = newEmitter(o.Store, o.Sink, o.Banner)
	s.em.start()
	go s.acceptLoop()
	go s.watch()
	return s, nil
}

// noTracker is the tracker of a caller that gave none: a run that has not started anything.
type noTracker struct{}

func (noTracker) Status() TrackerStatus { return TrackerStatus{Phase: PhaseStarting} }

// Path is the socket's path.
func (s *Server) Path() string { return s.sockPath }

// clearStaleSocket is Start step 4. Absent: go on. Not a socket (a regular file, a directory, a
// symbolic link, anything else): refuse and leave it. A socket: connect to it, because the lock is
// held on an open file and not on a name. Connected: a live process listens: refuse, exit 4, and
// leave it. Refused, and by ECONNREFUSED only: stale, removed if it is still the object that was
// inspected. Anything else is undecided: refuse and leave it.
func (s *Server) clearStaleSocket() error {
	fi, id, err := inspect(s.sockPath)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil
	case err != nil:
		return fmt.Errorf("cannot inspect the control socket path %q: %w", s.sockPath, err)
	}
	if err := controlpath.JudgeSocket(fi); err != nil {
		return fmt.Errorf("the control socket path %q %w; it is left in place: remove it yourself, or choose another control.socket_dir", s.sockPath, err)
	}
	conn, derr := net.DialTimeout("unix", s.sockPath, s.o.tune.dialDeadline)
	switch classifyStartAttempt(derr) {
	case startLive:
		_ = conn.Close()
		return &InUseError{Path: s.sockPath, Job: s.o.Job, Dir: s.o.Dir}
	case startStale:
		removed, err := removeIfSame(s.sockPath, id)
		if err != nil {
			return fmt.Errorf("remove the stale control socket %q: %w", s.sockPath, err)
		}
		if _, lerr := os.Lstat(s.sockPath); !removed && lerr == nil {
			return fmt.Errorf("the control socket path %q changed after it was inspected; it is left in place", s.sockPath)
		}
		return nil
	}
	return fmt.Errorf("cannot tell whether a run still listens on the control socket %q (%v); it is left in place: "+
		"remove it yourself if no run uses it", s.sockPath, derr)
}

// release undoes a start that did not complete: the socket this start bound, by identity, then the
// listener, then the lock. The lock file is never removed.
func (s *Server) release() {
	if s.listener != nil {
		if s.sockID.captured {
			_, _ = removeIfSame(s.sockPath, s.sockID)
		}
		_ = s.listener.Close()
	}
	if s.lock != nil {
		_ = s.lock.Close()
	}
}

// Ending is the terminal transition and the diagnostics cutoff, in one step (spec §4.2 End steps 1
// and 2): from here no change is accepted, the summary is frozen and returned, and best-effort
// entries are counted and not queued, so nothing can lengthen what Drain writes. status keeps
// answering, with the state `ending`. It may be called again.
func (s *Server) Ending() tuning.Summary {
	sum := s.o.Store.Terminal()
	s.em.cutoff()
	return sum
}

// Drain stops the identity check and joins it, then has the emitter write what was queued before
// the cutoff and its one count line (steps 3 and 4). The command writes its final output after it.
// Without Ending first it takes the transition itself.
func (s *Server) Drain() {
	s.Ending()
	s.stopWatch()
	s.em.drain()
}

// Close is the rest of the end (steps 6 and 7): the listener closes, the handlers are waited for,
// each bounded by its deadline, the run's own socket is removed if what is at its path is still it,
// by identity, and the lock is released. A socket that is not this run's, a file of another type
// among them, is left, and the error says so. The lock file is never removed. It takes the earlier
// steps itself if the command did not; a second call returns nil.
func (s *Server) Close() error {
	var err error
	first := false
	s.closeOnce.Do(func() {
		first = true
		s.Drain()
		close(s.closing)
		_ = s.listener.Close()
		<-s.acceptDone
		s.handlers.Wait()
		removed, rerr := removeIfSame(s.sockPath, s.sockID)
		if _, lerr := os.Lstat(s.sockPath); !removed && (rerr != nil || lerr == nil) {
			err = fmt.Errorf("the control socket %q was not removed: what is at that path is no longer this run's socket", s.sockPath)
		}
		_ = s.lock.Close()
	})
	if !first {
		return nil
	}
	return err
}

// nextBackoff is the wait after an accept error: a few milliseconds, doubling to one second.
func nextBackoff(d time.Duration) time.Duration {
	switch {
	case d <= 0:
		return 5 * time.Millisecond
	case d >= 500*time.Millisecond:
		return time.Second
	}
	return d * 2
}

// acceptLoop accepts connections, at most the configured number being handled at once: it takes a
// handler's place before it accepts, so the rest wait in the listen queue. On net.ErrClosed it
// returns; on any other error it reports a warning, backs off and goes on. It never handles a nil
// connection.
func (s *Server) acceptLoop() {
	defer close(s.acceptDone)
	var backoff time.Duration
	for {
		select {
		case s.sem <- struct{}{}:
		case <-s.closing:
			return
		}
		conn, err := s.listener.Accept()
		if err != nil {
			<-s.sem
			if errors.Is(err, net.ErrClosed) {
				return
			}
			backoff = nextBackoff(backoff)
			s.em.note(tuning.LevelWarn, tuning.Peer{UID: -1}, fmt.Sprintf("Control socket accept failed: %v; retrying in %s", err, backoff))
			select {
			case <-time.After(backoff):
			case <-s.closing:
				return
			}
			continue
		}
		backoff = 0
		s.handlers.Add(1)
		go func() {
			defer s.handlers.Done()
			defer func() { <-s.sem }()
			s.serve(conn)
		}()
	}
}

// judge reads the connection's credentials and applies the same-user rule.
func (s *Server) judge(c net.Conn) (tuning.Peer, error) {
	if s.o.tune.peer != nil {
		return s.o.tune.peer(c)
	}
	uid, pid, err := peerUID(c)
	return judgePeer(uid, pid, err, s.o.Euid, runtime.GOOS != "windows")
}

// serve handles one connection: one request, one reply, and the close. It writes to the socket only;
// whatever it has to log goes to the emitter, which never blocks it. A panic is recovered, reported
// as an error and closes this connection only.
func (s *Server) serve(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	peer := tuning.Peer{UID: -1}
	defer func() {
		if r := recover(); r != nil {
			s.em.note(tuning.LevelError, peer, fmt.Sprintf("Control socket handler panicked: %v", r))
		}
	}()
	_ = conn.SetDeadline(time.Now().Add(s.o.tune.connDeadline))

	p, err := s.judge(conn)
	if err != nil {
		_, _ = conn.Write(errReply(reasonDenied, err.Error()).wire())
		s.em.note(tuning.LevelWarn, peer, "Control connection refused: denied: "+err.Error())
		return
	}
	peer = p

	line, err := readRequest(conn)
	switch {
	case errors.Is(err, errNoLineFeed), errors.Is(err, errTooLong):
		r := errReply(reasonMalformed, err.Error())
		_, _ = conn.Write(r.wire())
		s.refused(peer, "", r)
		return
	case err != nil:
		return // a reset or the deadline: no reply, and nothing is said about a request
	}
	r := s.cmds.execute(line, peer)
	if !r.ok {
		s.refused(peer, line, r)
	}
	_, _ = conn.Write(r.wire())
}

// refused hands a refusal to the emitter, best effort: the reason and message, the request quoted
// on one line and cut to 64 bytes, and who asked.
func (s *Server) refused(peer tuning.Peer, line string, r reply) {
	s.em.note(tuning.LevelWarn, peer, fmt.Sprintf("Control request refused: %s: %s; request %s (%s)",
		r.reason, oneLine.Replace(r.message), quoted(line), peerText(peer)))
}
