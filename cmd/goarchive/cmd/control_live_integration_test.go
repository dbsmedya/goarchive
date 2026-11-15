//go:build integration

package cmd

import (
	"net"
	"path/filepath"
	"testing"
	"time"
)

// The live-run helpers of control_integration_test.go for a run that ends between two requests. A
// poll on a run's state that goes on until the state changes meets the run's own end when the last
// state change lets it finish: its socket is removed, and the request after that finds no socket
// (or finds one that is going). TestMySQLControlToleranceRaisedSocket met it (GR C2's qa run): five
// raises, the hold ended, three roots in 40 ms, and the next status-json found no socket. These cases
// need no server: they give a liveRun a socket of their own.

// helperRun is a liveRun on a socket in a short directory of its own.
func helperRun(t *testing.T) *liveRun {
	t.Helper()
	return &liveRun{t: t, sock: filepath.Join(shortTemp(t), "helper.sock"), done: make(chan int, 1)}
}

// exitsSoon makes the run's exit code arrive after a moment, as it does after its socket is gone.
func (r *liveRun) exitsSoon(code int) {
	go func() {
		time.Sleep(100 * time.Millisecond)
		r.done <- code
	}()
}

func TestLiveRunRequestMeetsTheRunsEnd(t *testing.T) {
	t.Run("the socket is gone and the run has exited", func(t *testing.T) {
		r := helperRun(t)
		r.exitsSoon(0)
		if reply, ended := r.tryAsk("status-json"); !ended || reply != "" {
			t.Fatalf("tryAsk = %q, ended %v, want the run's end", reply, ended)
		}
		if code := r.finish(); code != 0 {
			t.Errorf("finish() = %d, want the run's own code, left for it", code)
		}
	})

	t.Run("the socket is a file nobody listens on any more", func(t *testing.T) {
		// The window between the look and the dial: the file is there and the server is not.
		r := helperRun(t)
		ln, err := net.Listen("unix", r.sock)
		if err != nil {
			t.Fatal(err)
		}
		ln.(*net.UnixListener).SetUnlinkOnClose(false)
		_ = ln.Close()
		r.exitsSoon(3)
		if _, ended := r.tryAsk("status-json"); !ended {
			t.Fatal("tryAsk on a dead socket file did not report the run's end")
		}
		if code := r.finish(); code != 3 {
			t.Errorf("finish() = %d, want 3", code)
		}
	})

	t.Run("the server closes without a reply", func(t *testing.T) {
		r := helperRun(t)
		ln, err := net.Listen("unix", r.sock)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		go func() {
			if c, err := ln.Accept(); err == nil {
				_ = c.Close()
			}
		}()
		r.exitsSoon(0)
		if _, ended := r.tryAsk("status-json"); !ended {
			t.Fatal("tryAsk on a server that closed without a reply did not report the run's end")
		}
	})

	t.Run("an answer is not an end", func(t *testing.T) {
		r := helperRun(t)
		ln, err := net.Listen("unix", r.sock)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		go func() {
			if c, err := ln.Accept(); err == nil {
				_, _ = c.Write([]byte("OK\n{\"state\":\"held\"}\n"))
				_ = c.Close()
			}
		}()
		st, ended := r.tryStatus()
		if ended || st["state"] != "held" {
			t.Fatalf("tryStatus = %v, ended %v, want the held state", st, ended)
		}
		select {
		case code := <-r.done:
			t.Errorf("an answered request consumed the run's exit code %d", code)
		default:
		}
	})

	t.Run("a run that never ends is not taken for an end", func(t *testing.T) {
		r := helperRun(t)
		start := time.Now()
		done := make(chan bool, 1)
		go func() { _, ended := r.tryAsk("status-json"); done <- ended }()
		select {
		case ended := <-done:
			t.Fatalf("tryAsk returned (ended %v) after %v with no socket and no exit code", ended, time.Since(start))
		case <-time.After(300 * time.Millisecond):
		}
		r.done <- 0 // let the helper return
		if ended := <-done; !ended {
			t.Errorf("after the exit code, ended = %v, want true", ended)
		}
	})
}
