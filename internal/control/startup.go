package control

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"

	"github.com/dbsmedya/goarchive/v3/internal/controlpath"
)

// startVerdict is what a connection attempt to an existing socket says about it (spec §4.2 Start
// step 4).
type startVerdict int

const (
	// startLive: the connection was made. A live process listens; nothing is sent.
	startLive startVerdict = iota
	// startStale: the connection was refused, and by that exact error only (ECONNREFUSED).
	startStale
	// startUndecided: anything else, which removes nothing: a permission error, "try again", any
	// error that reports itself as a timeout, the attempt's own deadline, an error nobody foresaw.
	startUndecided
)

// classifyStartAttempt classifies the error of a connection attempt. Nothing but ECONNREFUSED is
// ever stale, so an error nobody foresaw removes nothing. A timeout is judged first: Linux says
// EAGAIN for a live listener whose queue is full, which net.Error calls a timeout though nothing
// waited.
func classifyStartAttempt(err error) startVerdict {
	var ne net.Error
	switch {
	case err == nil:
		return startLive
	case errors.As(err, &ne) && ne.Timeout(), errors.Is(err, os.ErrDeadlineExceeded):
		return startUndecided
	case IsRefused(err):
		return startStale
	}
	return startUndecided
}

// removeIfSame removes the object at path if it is still the one whose identity was captured: a
// path is only a name, so what is there is asked again, at the moment of removal. It reports
// whether it removed it; a name already gone, or taken by another object, is left and is not an
// error.
func removeIfSame(path string, ours identity) (bool, error) {
	current, err := identityAt(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return false, nil
	case err != nil:
		return false, err
	case !sameFile(current, ours):
		return false, nil
	}
	if err := os.Remove(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// mkdirIfAbsent creates a directory with mode 0700 when nothing is there. The process umask is
// untouched; it can only take bits from 0700.
func mkdirIfAbsent(path string) error {
	if _, err := os.Lstat(path); err == nil {
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return err
	}
	return nil
}

// prepareDir is Start step 1 (spec §4.1, §4.2): check or create the socket directory, judge it, and
// return its identity. The default directory needs its home to exist and creates `.goarchive` and
// `run` with mode 0700 when absent; a configured one has its last element created when absent and
// its parent must exist. A directory that exists must be a real directory, owned by the run's user,
// with no group or other bits; GoArchive never changes one it did not create. The server is the only
// creator.
func prepareDir(o Options) (identity, error) {
	dir := o.Dir
	if o.DefaultDir {
		home := filepath.Dir(filepath.Dir(dir))
		if filepath.Dir(home) == home {
			return identity{}, fmt.Errorf("the default control socket directory would be %q, because the home directory recorded for this user is %q, the root of the file system: "+
				"set control.socket_dir to a directory this user owns", dir, home)
		}
		if fi, err := os.Stat(home); err != nil || !fi.IsDir() {
			return identity{}, fmt.Errorf("the home directory %q does not exist or is not a directory, so the default control socket directory %q cannot be made: "+
				"set control.socket_dir", home, dir)
		}
		if err := mkdirIfAbsent(filepath.Dir(dir)); err != nil {
			return identity{}, fmt.Errorf("create %q: %w", filepath.Dir(dir), err)
		}
	} else if fi, err := os.Stat(filepath.Dir(dir)); err != nil || !fi.IsDir() {
		return identity{}, fmt.Errorf("the parent %q of control.socket_dir %q does not exist or is not a directory: the run creates only the last element",
			filepath.Dir(dir), dir)
	}
	if err := mkdirIfAbsent(dir); err != nil {
		return identity{}, fmt.Errorf("create control socket directory %q: %w", dir, err)
	}
	fi, id, err := inspect(dir)
	if err != nil {
		return identity{}, fmt.Errorf("inspect control socket directory %q: %w", dir, err)
	}
	if err := controlpath.JudgeDir(fi, o.Euid); err != nil {
		return identity{}, fmt.Errorf("control socket directory %q %w: GoArchive never changes the mode or owner of a directory it did not create; "+
			"use a directory that only this user can enter (mode 0700, owned by this user)", dir, err)
	}
	return id, nil
}
