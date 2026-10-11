package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
)

// Verdict is one matrix row judged against its expectation.
type Verdict struct {
	OK      bool
	Reasons []string // why the row is not OK; empty when it is
	// Pass, Fail and Skip count test results at every level (subtests included).
	Pass, Fail, Skip int
	// Failing names the failing leaf tests ("package Test/sub"), in order.
	Failing []string
}

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
// test fails) or "refuse:CODE[|CODE…]" (every failing leaf test's own output contains one
// of the codes). Under either, a panic, a timeout, a build failure or a package that
// fails with no failing test fails the row (M7c-2 P2).
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

	// A leaf is a failing test none of whose subtests failed: the parent of a failing
	// subtest fails with it, and its own output does not speak for the subtest.
	isParent := map[testID]bool{}
	for _, id := range failed {
		for i := strings.LastIndex(id.test, "/"); i > 0; i = strings.LastIndex(id.test[:i], "/") {
			isParent[testID{id.pkg, id.test[:i]}] = true
		}
	}
	failingIn := map[string]bool{}
	for _, id := range failed {
		failingIn[id.pkg] = true
		if isParent[id] {
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
	v.OK = len(v.Reasons) == 0
	return v
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

func cites(out *strings.Builder, codes []string) bool {
	if out == nil {
		return false
	}
	text := out.String()
	return slices.ContainsFunc(codes, func(c string) bool { return strings.Contains(text, c) })
}
