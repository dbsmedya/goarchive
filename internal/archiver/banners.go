package archiver

const forceLockBypassBanner = "" +
	"================================================================\n" +
	"  WARNING: --force bypassed lock acquisition (lock holder is stale)\n" +
	"  The previous instance has not heartbeated within the staleness\n" +
	"  threshold. Proceeding under the assumption that it crashed.\n" +
	"\n" +
	"  If the previous instance is actually still alive but slow,\n" +
	"  this run can corrupt data. Verify before continuing in\n" +
	"  production.\n" +
	"================================================================"
