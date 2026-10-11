// Command judge decides whether one environment-matrix row met its expected outcome
// (tests/envmatrix/rows.tsv), from the row's `go test -json` output. It prints the verdict
// and exits 1 when the row is not OK.
//
//	go run ./tests/envmatrix/judge -expect 'refuse:MAX_ALLOWED_PACKET_CHECK' -json go-test.json -exit 1
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	expect := flag.String("expect", "", "the row's expectation: pass or refuse:CODE[|CODE…]")
	path := flag.String("json", "", "the row's go test -json output")
	exit := flag.Int("exit", -1, "go test's exit status, reconciled with the stream (-1: unknown)")
	flag.Parse()
	f, err := os.Open(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "judge:", err)
		os.Exit(2)
	}
	v := Judge(*expect, f)
	_ = f.Close()
	v.Exit(*exit)
	verdict := "OK"
	if !v.OK {
		verdict = "FAIL"
	}
	fmt.Printf("verdict: %s  expect: %s  pass: %d  fail: %d  skip: %d\n", verdict, *expect, v.Pass, v.Fail, v.Skip)
	for _, r := range v.Reasons {
		fmt.Println("  reason:", r)
	}
	for _, f := range v.Failing {
		fmt.Println("  failing:", f)
	}
	if !v.OK {
		os.Exit(1)
	}
}
