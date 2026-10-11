package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
)

// Verdict is one matrix row judged against its expectation.
type Verdict struct {
	OK      bool
	Reasons []string // why the row is not OK; empty when it is
	// Pass, Fail and Skip count test results at every level (subtests included).
	Pass, Fail, Skip int
	// Failing names the failing tests that hold an assertion ("package Test/sub"), in
	// order: every failing leaf, and a failing parent with lines of its own.
	Failing []string

	failed bool // the stream holds a failing test or package
}

// Exit reconciles go test's exit status with the stream (Codex M7c-2 review B2): 0 with no
// failure, 1 with one; anything else is a signal or a crash and fails the row whatever
// the stream says. A negative code is unknown and checks nothing.
func (v *Verdict) Exit(code int) {
	switch {
	case code < 0:
		return
	case code == 0 && v.failed:
		v.Reasons = append(v.Reasons, "go test exited 0 though its stream failed")
	case code == 1 && !v.failed:
		v.Reasons = append(v.Reasons, "go test exited 1 with no failure in its stream")
	case code > 1:
		v.Reasons = append(v.Reasons, fmt.Sprintf("go test exited %d: killed or crashed", code))
	}
	v.OK = len(v.Reasons) == 0
}

// assertionLine matches a line testing writes for t.Error, t.Fatal and t.Log: indented,
// "<file>_test.go:<line>: ". go1.26 does not tell an error from a log in go test -json,
// so a failing parent's own such lines are judged as assertions (Codex M7c-2 review B3).
var assertionLine = regexp.MustCompile(`^\s+\S+_test\.go:\d+: `)

// event is one line of `go test -json` (cmd/test2json), build events included.
type event struct {
	Action      string
	Package     string
	Test        string
	Output      string
	FailedBuild string
}

type testID struct{ pkg, test string }

// Judge reads a `go test -json` stream and decides whether it meets expect: "pass" (no
// test fails) or "refuse:CODE[|CODE…]" (every failing leaf test's own output cites one of
// the codes: the M7-8 rule that a run is exact, or refused at startup or failed closed by
// name before any change). Under either, a panic, a timeout, a build failure or a package that
// fails with no failing test fails the row (M7c-2 P2), and so does a test or a package
// that started and never finished: an interrupted stream (Codex M7c-2 review B2). A
// failing parent's own lines are judged like a leaf's (B3).
func Judge(expect string, events io.Reader) Verdict {
	var v Verdict
	codes, err := parseExpect(expect)
	if err != nil {
		v.Reasons = append(v.Reasons, err.Error())
		return v
	}

	output := map[testID]*strings.Builder{}
	var failed []testID
	pkgFailed := map[string]string{} // package → FailedBuild ("" when it built)
	var pkgOrder []string
	seen := false
	var started []testID // tests ("run") and packages ("start", empty test), in order
	finished := map[testID]bool{}

	dec := json.NewDecoder(events)
	for {
		var e event
		if err := dec.Decode(&e); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			v.Reasons = append(v.Reasons, fmt.Sprintf("unreadable go test -json stream: %v", err))
			return v
		}
		id := testID{e.Package, e.Test}
		switch e.Action {
		case "start", "run":
			started = append(started, id)
		case "output":
			if output[id] == nil {
				output[id] = &strings.Builder{}
			}
			output[id].WriteString(e.Output)
			if strings.HasPrefix(e.Output, "panic: ") {
				where := strings.TrimSpace(e.Package + " " + e.Test)
				if strings.HasPrefix(e.Output, "panic: test timed out") {
					v.Reasons = append(v.Reasons, fmt.Sprintf("%s: test timed out", where))
				} else {
					v.Reasons = append(v.Reasons, fmt.Sprintf("%s: %s", where, strings.TrimSpace(e.Output)))
				}
			}
		case "pass", "fail", "skip":
			seen = true
			finished[id] = true
			if e.Test == "" {
				if e.Action == "fail" {
					pkgFailed[e.Package] = e.FailedBuild
					pkgOrder = append(pkgOrder, e.Package)
				}
				continue
			}
			switch e.Action {
			case "pass":
				v.Pass++
			case "fail":
				v.Fail++
				failed = append(failed, id)
			case "skip":
				v.Skip++
			}
		}
	}
	if !seen {
		v.Reasons = append(v.Reasons, "no test ran: the stream has no test or package result")
	}
	for _, id := range started {
		if finished[id] {
			continue
		}
		if id.test == "" {
			v.Reasons = append(v.Reasons, fmt.Sprintf("package %s started but did not finish", id.pkg))
		} else {
			v.Reasons = append(v.Reasons, fmt.Sprintf("%s %s started but did not finish", id.pkg, id.test))
		}
	}

	// A leaf is a failing test none of whose subtests failed. The parent of a failing
	// subtest fails with it; its output does not speak for the subtest, but its own lines
	// may be its own assertions, which are judged like a leaf's.
	isParent := map[testID]bool{}
	for _, id := range failed {
		for i := strings.LastIndex(id.test, "/"); i > 0; i = strings.LastIndex(id.test[:i], "/") {
			isParent[testID{id.pkg, id.test[:i]}] = true
		}
	}
	failingIn := map[string]bool{}
	for _, id := range failed {
		failingIn[id.pkg] = true
		if isParent[id] && !ownLines(output[id]) {
			continue
		}
		name := id.pkg + " " + id.test
		v.Failing = append(v.Failing, name)
		switch {
		case codes == nil:
			v.Reasons = append(v.Reasons, name+" failed")
		case !cites(output[id], codes):
			v.Reasons = append(v.Reasons, name+" cites no expected code ("+strings.Join(codes, "|")+")")
		}
	}
	for _, pkg := range pkgOrder {
		if build := pkgFailed[pkg]; build != "" {
			v.Reasons = append(v.Reasons, fmt.Sprintf("package %s failed to build (%s)", pkg, build))
		} else if !failingIn[pkg] {
			v.Reasons = append(v.Reasons, fmt.Sprintf("package %s failed with no failing test", pkg))
		}
	}
	v.failed = v.Fail > 0 || len(pkgOrder) > 0
	v.OK = len(v.Reasons) == 0
	return v
}

// ownLines reports whether out holds a line testing writes for the test itself.
func ownLines(out *strings.Builder) bool {
	if out == nil {
		return false
	}
	for line := range strings.Lines(out.String()) {
		if assertionLine.MatchString(line) {
			return true
		}
	}
	return false
}

// parseExpect returns nil codes for "pass", the codes of "refuse:A|B", or an error.
func parseExpect(expect string) ([]string, error) {
	if expect == "pass" {
		return nil, nil
	}
	rest, ok := strings.CutPrefix(expect, "refuse:")
	if !ok {
		return nil, fmt.Errorf("invalid expectation %q: want pass or refuse:CODE[|CODE…]", expect)
	}
	codes := strings.Split(rest, "|")
	for _, c := range codes {
		if c == "" {
			return nil, fmt.Errorf("invalid expectation %q: an empty code", expect)
		}
	}
	return codes, nil
}

// cites reports whether out cites one of codes: an occurrence counts only when no "want"
// precedes it on its line, so a test that merely expected the code ("Run() = <nil>, want
// CODE") does not cite it (Fable M7c-2 review F1).
func cites(out *strings.Builder, codes []string) bool {
	if out == nil {
		return false
	}
	for line := range strings.Lines(out.String()) {
		for _, c := range codes {
			for i := strings.Index(line, c); i >= 0; {
				if !strings.Contains(line[:i], "want") {
					return true
				}
				next := strings.Index(line[i+1:], c)
				if next < 0 {
					break
				}
				i += 1 + next
			}
		}
	}
	return false
}
