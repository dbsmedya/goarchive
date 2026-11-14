package control

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// TestShellClientsReturnWhenTheServerCloses is spec §9 item 6, a **pin**: the lines that the
// protocol contract will publish (spec §4.3), with no flag for the end of input, send one
// request and return when the server closes. The server is a listener of this test: it reads
// the first line, replies and closes at once, without waiting for the client's end of
// input -- the way the control server answers (spec §4.3, *The request ends at its first LF*).
// The line must return by itself: a client that waited for the server to see EOF first would
// hang, and the deadline here would fail the test.
//
// A host without the tool skips, with the reason; the evidence file says which hosts have it.
func TestShellClientsReturnWhenTheServerCloses(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("not available: the published lines are for a POSIX shell")
	}
	for _, c := range []struct{ tool, request, line string }{
		{"nc", "status-json\n", `printf 'status-json\n' | nc -U %s`},
		{"socat", "batch_delete_size=100\n", `printf 'batch_delete_size=100\n' | socat - UNIX-CONNECT:%s`},
	} {
		t.Run(c.tool, func(t *testing.T) {
			bin, err := exec.LookPath(c.tool)
			if err != nil {
				t.Skipf("not available: %s is not installed (%v)", c.tool, err)
			}
			t.Logf("%s on %s/%s: %s", c.tool, runtime.GOOS, runtime.GOARCH, bin)

			path := filepath.Join(shortDir(t), "ctl.sock")
			l, err := net.Listen("unix", path)
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			got := make(chan string, 1)
			go func() {
				conn, err := l.Accept()
				if err != nil {
					got <- "accept: " + err.Error()
					return
				}
				defer conn.Close()
				line, err := bufio.NewReader(conn).ReadString('\n')
				if err != nil {
					got <- "read: " + err.Error()
					return
				}
				_, _ = conn.Write([]byte("OK\nprotocol 1\n"))
				got <- line // the deferred Close ends the exchange; nothing waits for EOF
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			start := time.Now()
			out, err := exec.CommandContext(ctx, "sh", "-c", fmt.Sprintf(c.line, path)).CombinedOutput()
			took := time.Since(start)
			if ctx.Err() != nil {
				t.Fatalf("the line did not return after the server closed (%v): %q", took, out)
			}
			if err != nil {
				t.Fatalf("the line failed after %v: %v: %q", took, err, out)
			}
			t.Logf("returned after %v with %q", took, out)
			if string(out) != "OK\nprotocol 1\n" {
				t.Fatalf("the reply was %q", out)
			}
			select {
			case line := <-got:
				if line != c.request {
					t.Fatalf("the server read %q, want %q", line, c.request)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the server saw no request")
			}
		})
	}
}
