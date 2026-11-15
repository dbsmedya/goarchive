package engine

import (
	"strings"
	"testing"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
)

// TestRunLogsTheGroupReportOnce: a run's log carries spec §4.7's block as one entry, for a member
// source, and carries nothing for a plain pair (GR C2). validate and dry-run print it instead.
func TestRunLogsTheGroupReportOnce(t *testing.T) {
	proc := config.ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500}
	member := &preflight.Result{Group: preflight.Group{
		Source: dbsrepl.GroupReplication{PluginStatus: "ACTIVE", GroupName: "8a94f357-0000-4000-8000-0000000000aa",
			TransactionSizeLimit: 150000000, TransactionSizeLimitKnown: true},
		Members: []dbsrepl.GroupMember{{MemberID: "11111111-1111-4111-8111-111111111111", State: "ONLINE", Role: "PRIMARY", Self: true}},
	}}
	for _, tc := range []struct {
		name string
		res  *preflight.Result
		want int
	}{{"a member source", member, 1}, {"a plain pair", &preflight.Result{}, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			core, logs := observer.New(zapcore.DebugLevel)
			logGroupReport(&logger.Logger{SugaredLogger: zap.New(core).Sugar()}, tc.res, proc)
			if got := logs.Len(); got != tc.want {
				t.Fatalf("log entries = %d, want %d: %v", got, tc.want, logs.All())
			}
			if tc.want == 0 {
				return
			}
			e := logs.All()[0]
			if e.Level != zapcore.InfoLevel || !strings.HasPrefix(e.Message, "Group Replication:\n  source:      member of group ") ||
				!strings.Contains(e.Message, "batch_size 1000, batch_delete_size 500") {
				t.Errorf("entry = %s %q, want the block at info level", e.Level, e.Message)
			}
		})
	}
}
