package archiver

import (
	"github.com/dbsmedya/goarchive/internal/logger"
	"sort"
)

const forceLockBypassBanner = "" +
	"================================================================\n" +
	"  WARNING: --force bypassed lock acquisition (lock holder is stale)\n" +
	"  The previous instance has not heartbeated within the staleness\n" +
	"  threshold. Proceeding under the assumption that it crashed.\n" +
	"\n" +
	"  This is a best-effort takeover only. It prevents additional\n" +
	"  startups after this run refreshes the heartbeat, but it cannot\n" +
	"  stop a stale holder that is still alive and still owns MySQL's\n" +
	"  GET_LOCK. Verify the old process is dead before forcing.\n" +
	"================================================================"

const skipVerificationBanner = "================================================================\n" +
	"SAFETY WARNING: skip_verification is enabled\n" +
	"Copied values will not be compared. Recognized conversion/truncation warnings are accepted and reported.\n" +
	"Archive may permanently DELETE originals after MySQL changes their copied values.\n" +
	"SQL errors, unknown/incomplete diagnostics and temporal identity failures still stop the run.\n" +
	"================================================================"
const copyOnlySkipVerificationNote = "================================================================\n" +
	"NOTICE: skip_verification is enabled for copy-only\n" +
	"Copied values will not be compared. Recognized conversion/truncation warnings are accepted and reported.\n" +
	"Source rows will not be deleted. SQL errors and mandatory identity checks remain active.\n" +
	"================================================================"

func reportConversionTotals(log *logger.Logger, mode string, success bool, totals map[string]map[uint16]uint64) {
	tables := make([]string, 0, len(totals))
	for table := range totals {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	nonempty := false
	for _, table := range tables {
		codes := make([]int, 0, len(totals[table]))
		for code, count := range totals[table] {
			if count > 0 {
				codes = append(codes, int(code))
			}
		}
		sort.Ints(codes)
		for _, code := range codes {
			nonempty = true
			log.Warnw("Accepted conversion warnings", "table", table, "code", code, "diagnostics", totals[table][uint16(code)], "scope", "committed-copy", "mode", mode)
		}
	}
	if nonempty {
		if success {
			log.Warn("Completed with accepted conversion warnings")
		} else {
			log.Warn("Stopped after accepting conversion warnings")
		}
	}
}
