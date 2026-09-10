package archiver

import (
	"github.com/dbsmedya/goarchive/internal/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"strings"
	"testing"
)

func TestConversionNoticeText(t *testing.T) {
	for _, clause := range []string{"Copied values will not be compared.", "Recognized conversion/truncation warnings are accepted and reported.", "Archive may permanently DELETE originals after MySQL changes their copied values.", "SQL errors, unknown/incomplete diagnostics and temporal identity failures still stop the run."} {
		if !strings.Contains(skipVerificationBanner, clause) {
			t.Fatal("missing notice clause", clause)
		}
	}
	if strings.Contains(skipVerificationBanner, "If INSERT IGNORE skips") {
		t.Fatal("stale unsafe banner")
	}
	if !strings.Contains(copyOnlySkipVerificationNote, "Source rows will not be deleted.") {
		t.Fatal("copy-only source promise missing")
	}
}
func TestConversionTotalsDeterministic(t *testing.T) {
	for _, success := range []bool{false, true} {
		core, logs := observer.New(zap.WarnLevel)
		log := &logger.Logger{SugaredLogger: zap.New(core).Sugar()}
		reportConversionTotals(log, "archive", success, map[string]map[uint16]uint64{"z": {1366: 2, 1264: 1}, "a": {1292: 3}})
		entries := logs.All()
		if len(entries) != 4 {
			t.Fatal(entries)
		}
		for i, want := range []struct {
			table string
			code  int
		}{{"a", 1292}, {"z", 1264}, {"z", 1366}} {
			fields := entries[i].ContextMap()
			if fields["table"] != want.table || fields["code"] != int64(want.code) || fields["scope"] != "committed-copy" {
				t.Fatal(fields)
			}
		}
		want := "Stopped after accepting conversion warnings"
		if success {
			want = "Completed with accepted conversion warnings"
		}
		if entries[3].Message != want {
			t.Fatal(entries[3])
		}
	}
}
func TestConversionTotalsExcludeRolledBackBatch(t *testing.T) {
	cp, _, err := copyBatchFixture(t, 2, 2, 1, true, false, false)
	if err == nil || len(cp.ConversionTotals()) != 0 {
		t.Fatalf("rolled-back conversion totals published: %+v err=%v", cp.ConversionTotals(), err)
	}
}
