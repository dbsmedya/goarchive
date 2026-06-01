package archiver

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

const skipVerificationBanner = "" +
	"================================================================\n" +
	"  SAFETY WARNING: skip_verification is enabled\n" +
	"  Archive will copy rows without verification and then permanently\n" +
	"  DELETE the source rows. If INSERT IGNORE skips or changes rows,\n" +
	"  this run can lose data without detecting it.\n" +
	"================================================================"

const copyOnlySkipVerificationNote = "" +
	"================================================================\n" +
	"  NOTICE: skip_verification is enabled for copy-only\n" +
	"  Copied rows will not be verified. Source rows will not be deleted.\n" +
	"================================================================"
