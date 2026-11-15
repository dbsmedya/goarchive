package engine

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// testSettings are a run's configured values for a store.
func testSettings() tuning.Settings {
	return tuning.Settings{BatchSize: 10, BatchDeleteSize: 10, SleepSeconds: 0.6, DeleteSleepSeconds: 0.6, SecondsBehindSourceWithin: 10, CheckInterval: 5}
}

func newTestStore(opts ...func(*tuning.Options)) *tuning.Store {
	o := tuning.Options{Start: testSettings(), Applies: tuning.Applicability{Delete: true, Gate: true}}
	for _, f := range opts {
		f(&o)
	}
	return tuning.New(o)
}

var testPeer = tuning.Peer{UID: 1000, PID: 1}

func sleepSetting(s tuning.Settings) float64 { return s.SleepSeconds }

// TestSleepFollowsChange (spec §7 row 44, D6): a sleep in progress follows a change of its
// setting at once: it ends when the time already slept reaches the new value, and a longer value
// extends it. The value is read at the start of the sleep and again on each change.
func TestSleepFollowsChange(t *testing.T) {
	type result struct {
		err  error
		took time.Duration
	}
	run := func(t *testing.T, s *tuning.Store, stop <-chan struct{}, ctx context.Context) <-chan result {
		t.Helper()
		out := make(chan result, 1)
		go func() {
			start := time.Now()
			err := followSleep(ctx, stop, s, sleepSetting)
			out <- result{err, time.Since(start)}
		}()
		return out
	}
	set := func(t *testing.T, s *tuning.Store, text string) {
		t.Helper()
		if _, err := s.Set(tuning.SleepSeconds, text, testPeer); err != nil {
			t.Fatal(err)
		}
	}
	wait := func(t *testing.T, ch <-chan result) result {
		t.Helper()
		select {
		case r := <-ch:
			return r
		case <-time.After(5 * time.Second):
			t.Fatal("the sleep did not end")
			return result{}
		}
	}

	t.Run("the configured value", func(t *testing.T) {
		s := newTestStore()
		r := wait(t, run(t, s, nil, context.Background()))
		if r.err != nil || r.took < 550*time.Millisecond || r.took > 1200*time.Millisecond {
			t.Fatalf("slept %v, %v; want about 0.6 s", r.took, r.err)
		}
	})
	t.Run("a shorter value ends it when it is already slept", func(t *testing.T) {
		s := newTestStore()
		ch := run(t, s, nil, context.Background())
		time.Sleep(250 * time.Millisecond)
		set(t, s, "0.1") // already slept: the sleep ends at once, not at 0.6 s
		r := wait(t, ch)
		if r.err != nil || r.took > 450*time.Millisecond || r.took < 200*time.Millisecond {
			t.Fatalf("slept %v after being shortened below the time slept, %v; want it to end at the change (about 0.25 s)", r.took, r.err)
		}
	})
	t.Run("a shorter value still ahead shortens it", func(t *testing.T) {
		s := newTestStore(func(o *tuning.Options) { o.Start.SleepSeconds = 2 })
		ch := run(t, s, nil, context.Background())
		time.Sleep(100 * time.Millisecond)
		set(t, s, "0.4")
		r := wait(t, ch)
		if r.err != nil || r.took < 350*time.Millisecond || r.took > 900*time.Millisecond {
			t.Fatalf("slept %v, %v; want about 0.4 s in all", r.took, r.err)
		}
	})
	t.Run("a longer value extends it", func(t *testing.T) {
		s := newTestStore(func(o *tuning.Options) { o.Start.SleepSeconds = 0.3 })
		ch := run(t, s, nil, context.Background())
		time.Sleep(100 * time.Millisecond)
		set(t, s, "0.9")
		r := wait(t, ch)
		if r.err != nil || r.took < 850*time.Millisecond || r.took > 1500*time.Millisecond {
			t.Fatalf("slept %v, %v; want about 0.9 s in all", r.took, r.err)
		}
	})
	t.Run("a change of another setting does not end it", func(t *testing.T) {
		s := newTestStore(func(o *tuning.Options) { o.Start.SleepSeconds = 0.5 })
		ch := run(t, s, nil, context.Background())
		time.Sleep(100 * time.Millisecond)
		if _, err := s.Set(tuning.BatchDeleteSize, "5", testPeer); err != nil {
			t.Fatal(err)
		}
		r := wait(t, ch)
		if r.err != nil || r.took < 450*time.Millisecond {
			t.Fatalf("slept %v, %v; an unrelated change cut it short", r.took, r.err)
		}
	})
	t.Run("zero is no sleep", func(t *testing.T) {
		s := newTestStore(func(o *tuning.Options) { o.Start.SleepSeconds = 0 })
		if r := wait(t, run(t, s, nil, context.Background())); r.err != nil || r.took > 100*time.Millisecond {
			t.Fatalf("a zero sleep took %v, %v", r.took, r.err)
		}
	})
	t.Run("a stop ends it, and no stop does not", func(t *testing.T) {
		s := newTestStore()
		stop := make(chan struct{})
		ch := run(t, s, stop, context.Background())
		time.Sleep(100 * time.Millisecond)
		close(stop)
		if r := wait(t, ch); r.err != nil || r.took > 400*time.Millisecond {
			t.Fatalf("a stop left the sleep running for %v, %v", r.took, r.err)
		}
		if r := wait(t, run(t, newTestStore(func(o *tuning.Options) { o.Start.SleepSeconds = 0.3 }), nil, context.Background())); r.took < 250*time.Millisecond {
			t.Fatalf("a nil stop ended the sleep after %v", r.took)
		}
	})
	t.Run("a cancelled context ends it", func(t *testing.T) {
		s := newTestStore()
		ctx, cancel := context.WithCancel(context.Background())
		ch := run(t, s, nil, ctx)
		time.Sleep(50 * time.Millisecond)
		cancel()
		if r := wait(t, ch); !errors.Is(r.err, context.Canceled) {
			t.Fatalf("a cancelled sleep = %v, want context.Canceled", r.err)
		}
	})
}

// TestSleepIsTheConversionTheValidatorsJudgeBy: seconds() and config.SleepDuration are one
// conversion.
func TestSleepIsTheConversionTheValidatorsJudgeBy(t *testing.T) {
	for _, s := range []float64{0, 0.25, 1, 3600, 9223372036} {
		if seconds(s) != config.SleepDuration(s) {
			t.Errorf("seconds(%v) = %v, SleepDuration = %v", s, seconds(s), config.SleepDuration(s))
		}
	}
}

// syncBuffer is a stream the gate's goroutine writes and the test reads.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// pauseLog returns a logger whose live lines (the pause and its end) reach the returned stream,
// as they reach the console with a file log.
func pauseLog(t *testing.T) (*logger.Logger, *syncBuffer) {
	t.Helper()
	errb := &syncBuffer{}
	log, err := logger.New(&config.LoggingConfig{Level: "error", Format: "json", Output: filepath.Join(t.TempDir(), "run.log")},
		logger.Options{Stderr: errb, Tee: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	return log, errb
}

// TestPauseInputs (spec §7 row 65, §4.7, D9): the pause gate has two inputs, the sentinel file
// and the store's socket pause. It waits while either holds, and the socket's changes wake it
// at once, without waiting for the one-second sentinel poll: socket only, sentinel only, both;
// `resume` ends a socket-only pause at once; under a present sentinel it clears the socket pause
// and the run stays paused, and says so; a stop ends the wait.
func TestPauseInputs(t *testing.T) {
	type gate struct {
		g       *sentinelGate
		present *atomic.Bool
		store   *tuning.Store
		done    chan error
		errb    *syncBuffer
	}
	start := func(t *testing.T, path string, sentinel bool, socket bool, stop <-chan struct{}, ctx context.Context) *gate {
		t.Helper()
		log, errb := pauseLog(t)
		g := &gate{present: &atomic.Bool{}, store: newTestStore(), done: make(chan error, 1), errb: errb}
		g.present.Store(sentinel)
		g.g = newSentinelGate(path, log)
		g.g.tune = g.store
		g.g.presentFn = func(string) bool { return g.present.Load() }
		if socket {
			if _, err := g.store.Pause(testPeer); err != nil {
				t.Fatal(err)
			}
		}
		go func() { g.done <- g.g.wait(ctx, stop) }()
		return g
	}
	blocked := func(t *testing.T, g *gate, why string) {
		t.Helper()
		select {
		case err := <-g.done:
			t.Fatalf("the gate returned (%v) %s", err, why)
		case <-time.After(300 * time.Millisecond):
		}
	}
	returns := func(t *testing.T, g *gate, within time.Duration, why string) error {
		t.Helper()
		select {
		case err := <-g.done:
			return err
		case <-time.After(within):
			t.Fatalf("the gate did not return within %v: %s", within, why)
			return nil
		}
	}

	t.Run("neither input holds", func(t *testing.T) {
		g := start(t, "pause", false, false, nil, context.Background())
		if err := returns(t, g, time.Second, "nothing holds the run"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("socket only: resume wakes it at once", func(t *testing.T) {
		g := start(t, "", false, true, nil, context.Background())
		blocked(t, g, "while the socket pause is set")
		if !strings.Contains(g.errb.String(), "control socket") {
			t.Errorf("the pause was not logged as the control socket's: %q", g.errb.String())
		}
		resumed := time.Now()
		if res, err := g.store.Resume(testPeer); err != nil || !res.Changed {
			t.Fatalf("Resume() = %+v, %v", res, err)
		}
		// The sentinel poll is one second: only the store's signal can end the wait this soon.
		if err := returns(t, g, 400*time.Millisecond, "resume must wake the gate through the snapshot's signal, not the poll"); err != nil {
			t.Fatal(err)
		}
		if since := time.Since(resumed); since > 400*time.Millisecond {
			t.Errorf("resume took %v to end the pause", since)
		}
		if got := g.errb.String(); !strings.Contains(got, "Resumed") || !strings.Contains(got, "control socket") {
			t.Errorf("the end of the pause was not logged: %q", got)
		}
	})

	t.Run("a pause set while the run is working holds the next wait", func(t *testing.T) {
		log, _ := pauseLog(t)
		store := newTestStore()
		g := newSentinelGate("", log)
		g.tune = store
		if err := g.wait(context.Background(), nil); err != nil { // not paused: returns at once
			t.Fatal(err)
		}
		if _, err := store.Pause(testPeer); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- g.wait(context.Background(), nil) }()
		select {
		case err := <-done:
			t.Fatalf("a paused store did not hold the gate: %v", err)
		case <-time.After(200 * time.Millisecond):
		}
		if _, err := store.Resume(testPeer); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(400 * time.Millisecond):
			t.Fatal("resume did not end the wait")
		}
	})

	t.Run("sentinel only: the file's removal ends it at the next poll", func(t *testing.T) {
		g := start(t, "pause", true, false, nil, context.Background())
		blocked(t, g, "while the sentinel file is present")
		g.present.Store(false)
		if err := returns(t, g, 2*time.Second, "the poll must see the file gone"); err != nil {
			t.Fatal(err)
		}
		if got := g.errb.String(); !strings.Contains(got, "WARN  Paused: sentinel file") || !strings.Contains(got, "INFO  Resumed: sentinel file") {
			t.Errorf("the sentinel pause's lines changed: %q", got)
		}
	})

	t.Run("both: resume clears the socket pause and the sentinel still holds, and says so", func(t *testing.T) {
		g := start(t, "pause", true, true, nil, context.Background())
		blocked(t, g, "while both inputs hold")
		if _, err := g.store.Resume(testPeer); err != nil {
			t.Fatal(err)
		}
		blocked(t, g, "after resume, while the sentinel file is still present: the run must not resume silently against the file")
		got := g.errb.String()
		if !strings.Contains(got, "sentinel file") || !strings.Contains(got, "still") {
			t.Errorf("the log does not say that the sentinel still holds the run: %q", got)
		}
		if strings.Contains(got, "INFO  Resumed") {
			t.Errorf("the run was reported resumed while the sentinel file is present: %q", got)
		}
		g.present.Store(false)
		if err := returns(t, g, 2*time.Second, "removing the file must end the pause"); err != nil {
			t.Fatal(err)
		}
		if got := g.errb.String(); !strings.Contains(got, "Resumed") {
			t.Errorf("the end of the pause was not logged: %q", got)
		}
	})

	t.Run("both: removing the file leaves the socket pause", func(t *testing.T) {
		g := start(t, "pause", true, true, nil, context.Background())
		blocked(t, g, "while both inputs hold")
		g.present.Store(false)
		blocked(t, g, "after the file is removed, while the socket pause is still set")
		if _, err := g.store.Resume(testPeer); err != nil {
			t.Fatal(err)
		}
		if err := returns(t, g, 400*time.Millisecond, "resume must end the last input"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("a stop ends the wait", func(t *testing.T) {
		stop := make(chan struct{})
		g := start(t, "", false, true, stop, context.Background())
		blocked(t, g, "while the socket pause is set")
		close(stop)
		if err := returns(t, g, 400*time.Millisecond, "a first stop ends a pause"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("a cancelled context ends the wait", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		g := start(t, "", false, true, nil, ctx)
		blocked(t, g, "while the socket pause is set")
		cancel()
		if err := returns(t, g, 400*time.Millisecond, "a cancel ends a pause"); !errors.Is(err, context.Canceled) {
			t.Fatalf("a cancelled pause = %v", err)
		}
	})

	t.Run("a stop already requested does not wait", func(t *testing.T) {
		stop := make(chan struct{})
		close(stop)
		g := start(t, "", false, true, stop, context.Background())
		if err := returns(t, g, time.Second, "a stop that is already requested"); err != nil {
			t.Fatal(err)
		}
	})
}

// TestTuningMustMatchProcessing: a store whose starting values differ from the job's
// processing block would make the ceilings something other than the configuration, so the
// engine refuses it. A nil store is the configuration's constant one.
func TestTuningMustMatchProcessing(t *testing.T) {
	p := config.ProcessingConfig{BatchSize: 10, BatchDeleteSize: 10, SleepSeconds: 0.6, DeleteSleepSeconds: 0.6}
	cfg := Config{Processing: p}
	if err := cfg.checkTuning(); err != nil {
		t.Fatalf("no store: %v", err)
	}
	cfg.Tuning = newTestStore()
	if err := cfg.checkTuning(); err != nil {
		t.Fatalf("a store that starts at the processing values: %v", err)
	}
	for name, mutate := range map[string]func(*tuning.Settings){
		"batch_size":           func(s *tuning.Settings) { s.BatchSize = 11 },
		"batch_delete_size":    func(s *tuning.Settings) { s.BatchDeleteSize = 11 },
		"sleep_seconds":        func(s *tuning.Settings) { s.SleepSeconds = 7 },
		"delete_sleep_seconds": func(s *tuning.Settings) { s.DeleteSleepSeconds = 7 },
	} {
		cfg.Tuning = newTestStore(func(o *tuning.Options) { mutate(&o.Start) })
		err := cfg.checkTuning()
		if err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("a store that starts elsewhere on %s: %v, want a refusal naming it", name, err)
		}
	}
}
