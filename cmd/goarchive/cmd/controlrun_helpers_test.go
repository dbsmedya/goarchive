package cmd

import (
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/database"
)

// newTestShutdown is a shutdown handler whose log lines go nowhere.
func newTestShutdown(t *testing.T) *database.Shutdown {
	t.Helper()
	return database.SetupGracefulShutdown(func(os.Signal) {}, func(os.Signal) {})
}

// askSocket sends one request line to the socket at path and returns everything it answers.
func askSocket(t *testing.T, path, request string) string {
	t.Helper()
	c, err := net.DialTimeout("unix", path, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", path, err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	_, _ = c.Write([]byte(request + "\n"))
	b, _ := io.ReadAll(c)
	return string(b)
}
