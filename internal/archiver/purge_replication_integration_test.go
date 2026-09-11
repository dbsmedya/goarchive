//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/internal/archiver/testsupport"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
	"github.com/dbsmedya/goarchive/internal/logger"
)

func wireControlledPurgeGate(
	t *testing.T,
	setup *IntegrationTestSetup,
	dbm *database.Manager,
	cfg *config.Config,
) {
	t.Helper()
	cfg.Replication = config.ReplicationConfig{
		Enabled: true,
		Servers: []config.ReplicationServerConfig{replicationServerFor(t, setup, nil)},
	}
	// The controlled waiter does not read this handle. Its presence exercises
	// purge's fleet and factory wiring without claiming a live status read.
	dbm.Replicas = []*sql.DB{dbm.Source}
}

func newControlledPurge(
	t *testing.T,
	setup *IntegrationTestSetup,
	jobName string,
	waiter lagWaiter,
	batchSize int,
) (*PurgeOrchestrator, *database.Manager, *config.Config) {
	t.Helper()
	dbm, cfg := resumeScenarioDBManager(t, setup, "sha256", batchSize)
	wireControlledPurgeGate(t, setup, dbm, cfg)
	orch, err := NewPurgeOrchestrator(cfg, jobName, resumeScenarioJobConfig(), dbm)
	if err != nil {
		t.Fatalf("NewPurgeOrchestrator: %v", err)
	}
	orch.lagFactory = func(dbs []*sql.DB, _ config.ReplicationConfig, _ *logger.Logger) (lagWaiter, error) {
		if len(dbs) != 1 {
			t.Fatalf("lag factory received %d handles, want 1", len(dbs))
		}
		return waiter, nil
	}
	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	return orch, dbm, cfg
}

func TestPurgeReplicationCallSites_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, ctx := SetupIntegrationTest(t)
	t.Cleanup(setup.Close)
	sourceDB, destDB := resumeScenarioDBs(t, setup)
	destSchema := getDestSchema(setup)
	sourceObserver := getVerificationDB(t, setup, "source")
	t.Cleanup(func() { _ = sourceObserver.Close() })
	destObserver := getVerificationDB(t, setup, "destination")
	t.Cleanup(func() { _ = destObserver.Close() })

	t.Run("prebatch-error", func(t *testing.T) {
		const jobName = "purge_calls_prebatch"
		resetReleaseRecoveryData(t, sourceDB, destDB)
		sentinel := errors.New("prebatch sentinel")
		waiter := &scriptedLagWaiter{errors: []error{sentinel}}
		orch, _, _ := newControlledPurge(t, setup, jobName, waiter, 1)
		testsupport.CleanupArchiverState(t, destDB, jobName)

		_, err := orch.Execute(ctx)
		if !errors.Is(err, sentinel) || err == nil || err.Error() != "lag monitor error: prebatch sentinel" {
			t.Fatalf("Execute error = %v, want exact outer pre-batch gate error", err)
		}
		if strings.Contains(err.Error(), "before delete") {
			t.Fatalf("outer gate error was masked by pre-delete gate: %v", err)
		}
		if waiter.calls != 1 {
			t.Errorf("waiter calls = %d, want 1", waiter.calls)
		}
		assertRootSet(t, sourceObserver, "source after pre-batch hold", resumeScenarioRoots...)
		assertRootSet(t, destObserver, "destination after pre-batch hold")
	})

	t.Run("predelete-error", func(t *testing.T) {
		const jobName = "purge_calls_predelete"
		resetReleaseRecoveryData(t, sourceDB, destDB)
		sentinel := errors.New("predelete sentinel")
		waiter := &scriptedLagWaiter{errors: []error{nil, sentinel}}
		orch, _, _ := newControlledPurge(t, setup, jobName, waiter, 1)
		testsupport.CleanupArchiverState(t, destDB, jobName)

		_, err := orch.Execute(ctx)
		if !errors.Is(err, sentinel) || err == nil ||
			!strings.Contains(err.Error(), "processBatch failed: lag monitor error before delete: predelete sentinel") {
			t.Fatalf("Execute error = %v, want pre-delete gate error", err)
		}
		if waiter.calls != 2 {
			t.Errorf("waiter calls = %d, want 2 (outer then pre-delete)", waiter.calls)
		}
		assertRootSet(t, sourceObserver, "source after pre-delete hold", resumeScenarioRoots...)
		assertRootSet(t, destObserver, "destination after pre-delete hold")
	})

	t.Run("recovery-second-chunk", func(t *testing.T) {
		const jobName = "purge_calls_recovery_second"
		resetReleaseRecoveryData(t, sourceDB, destDB)
		sentinel := errors.New("recovery second chunk sentinel")
		waiter := &scriptedLagWaiter{errors: []error{nil, nil, sentinel}}
		orch, _, _ := newControlledPurge(t, setup, jobName, waiter, 1)
		logTable := bootstrapJobTracking(t, destDB, destSchema, jobName, "customers", JobTypePurge)
		seedLogStatus(t, destDB, logTable, LogStatusPending, "1", "2")
		seedLogStatus(t, destDB, logTable, LogStatusCompleted, "3", "4", "5")
		setJobCheckpoint(t, destDB, jobName, resumeScenarioCheckpoint)

		_, err := orch.Execute(ctx)
		if !errors.Is(err, sentinel) || err == nil ||
			!strings.Contains(err.Error(), "resume failed: pending recovery failed: lag monitor error: recovery second chunk sentinel") {
			t.Fatalf("Execute error = %v, want second recovery-chunk outer gate error", err)
		}
		if waiter.calls != 3 {
			t.Errorf("waiter calls = %d, want 3 (chunk1 outer/pre-delete, chunk2 outer)", waiter.calls)
		}
		assertRootSet(t, sourceObserver, "source after second recovery hold", 2, 3, 4, 5)
		assertRootSet(t, destObserver, "destination after second recovery hold")
		assertRecoveryLogState(t, destObserver, logTable, map[string]LogStatus{
			"1": LogStatusCompleted, "2": LogStatusPending,
			"3": LogStatusCompleted, "4": LogStatusCompleted, "5": LogStatusCompleted,
		})
		assertCheckpointUnchanged(t, destObserver, jobName)
	})

	t.Run("healthy", func(t *testing.T) {
		const jobName = "purge_calls_healthy"
		resetReleaseRecoveryData(t, sourceDB, destDB)
		waiter := &scriptedLagWaiter{}
		orch, _, _ := newControlledPurge(t, setup, jobName, waiter, 1)
		logTable := bootstrapJobTracking(t, destDB, destSchema, jobName, "customers", JobTypePurge)
		seedLogStatus(t, destDB, logTable, LogStatusPending, "1", "2")
		seedLogStatus(t, destDB, logTable, LogStatusCompleted, "3", "4", "5")
		setJobCheckpoint(t, destDB, jobName, resumeScenarioCheckpoint)

		result, err := orch.Execute(ctx)
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if result == nil || !result.Success {
			t.Fatalf("result = %+v, want success", result)
		}
		if waiter.calls != 4 {
			t.Errorf("waiter calls = %d, want 4 (outer/pre-delete for two chunks)", waiter.calls)
		}
		assertRootSet(t, sourceObserver, "source after healthy recovery", 3, 4, 5)
		assertRootSet(t, destObserver, "destination after healthy recovery")
		assertAllCompleted(t, destObserver, logTable, "1", "2", "3", "4", "5")
		assertCheckpointUnchanged(t, destObserver, jobName)
	})
}

type purgeLogObserver struct {
	mu   sync.Mutex
	log  *logger.Logger
	path string
}

func newPurgeLogObserver(t *testing.T) *purgeLogObserver {
	t.Helper()
	path := filepath.Join(t.TempDir(), "purge-replication.log")
	log, err := logger.New(&config.LoggingConfig{
		Level: "debug", Format: "json", Output: path, FileOnly: true,
	})
	if err != nil {
		t.Fatalf("new purge observer logger: %v", err)
	}
	t.Cleanup(func() { _ = log.Close() })
	return &purgeLogObserver{log: log, path: path}
}

func (o *purgeLogObserver) contains(t *testing.T, needle string) bool {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	_ = o.log.Sync()
	b, err := os.ReadFile(o.path)
	if err != nil {
		t.Fatalf("read purge observer log: %v", err)
	}
	return strings.Contains(string(b), needle)
}

func (o *purgeLogObserver) waitFor(t *testing.T, needle string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if o.contains(t, needle) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("did not observe %q within %s", needle, timeout)
}

type purgeExecuteOutcome struct {
	result *PurgeResult
	err    error
}

func livePurgeReplicationConfig(t *testing.T, setup *IntegrationTestSetup, tolerance int) config.ReplicationConfig {
	t.Helper()
	return config.ReplicationConfig{
		Enabled:                   true,
		SecondsBehindSourceWithin: tolerance,
		CheckInterval:             1,
		CacheTTL:                  0,
		Servers: []config.ReplicationServerConfig{
			replicationServerFor(t, setup, nil),
		},
	}
}

func newLivePurge(
	t *testing.T,
	setup *IntegrationTestSetup,
	jobName string,
	tolerance int,
	observer *purgeLogObserver,
	destDB *sql.DB,
) *PurgeOrchestrator {
	t.Helper()
	dbm, cfg := setupRealDBManagerWithReplication(t, setup, livePurgeReplicationConfig(t, setup, tolerance))
	t.Cleanup(func() { _ = dbm.Close() })
	testsupport.CleanupArchiverState(t, destDB, jobName)
	orch, err := NewPurgeOrchestrator(cfg, jobName, resumeScenarioJobConfig(), dbm)
	if err != nil {
		t.Fatalf("NewPurgeOrchestrator: %v", err)
	}
	orch.SetLogger(observer.log)
	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	return orch
}

func awaitPurgeOutcome(t *testing.T, ch <-chan purgeExecuteOutcome, timeout time.Duration) purgeExecuteOutcome {
	t.Helper()
	select {
	case out := <-ch:
		return out
	case <-time.After(timeout):
		t.Fatalf("purge Execute did not finish within %s", timeout)
		return purgeExecuteOutcome{}
	}
}

func assertPurgeStillRunning(t *testing.T, ch <-chan purgeExecuteOutcome) {
	t.Helper()
	select {
	case out := <-ch:
		t.Fatalf("purge returned while replication was held (err=%v result=%+v)", out.err, out.result)
	default:
	}
}

func TestPurgeReplicationLive_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, _ := SetupIntegrationTest(t)
	t.Cleanup(setup.Close)
	sourceDB, destDB := resumeScenarioDBs(t, setup)
	sourceObserver := getVerificationDB(t, setup, "source")
	t.Cleanup(func() { _ = sourceObserver.Close() })
	destObserver := getVerificationDB(t, setup, "destination")
	t.Cleanup(func() { _ = destObserver.Close() })
	replicaDB := replicaAdminDB(t, setup)
	waitForReplicaCaughtUp(t, replicaDB, 60*time.Second)

	t.Run("healthy", func(t *testing.T) {
		const jobName = "purge_live_healthy"
		resetReleaseRecoveryData(t, sourceDB, destDB)
		waitForReplicaCaughtUp(t, replicaDB, 60*time.Second)
		observer := newPurgeLogObserver(t)
		orch := newLivePurge(t, setup, jobName, 10, observer, destDB)

		result, err := orch.Execute(context.Background())
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if result == nil || !result.Success || result.RecordsDeleted == 0 {
			t.Fatalf("result = %+v, want successful nonempty purge", result)
		}
		if observer.contains(t, "replication hold:") {
			t.Fatal("healthy replica unexpectedly emitted a hold")
		}
		assertRootSet(t, sourceObserver, "source after healthy live purge")
		assertRootSet(t, destObserver, "destination after healthy live purge")
		waitForReplicaCaughtUp(t, replicaDB, 60*time.Second)
		t.Log("restored state: IO=Yes SQL=Yes Seconds_Behind_Source=0")
	})

	t.Run("stopped-applier", func(t *testing.T) {
		const jobName = "purge_live_stopped"
		resetReleaseRecoveryData(t, sourceDB, destDB)
		waitForReplicaCaughtUp(t, replicaDB, 60*time.Second)
		observer := newPurgeLogObserver(t)
		orch := newLivePurge(t, setup, jobName, 10, observer, destDB)

		// Register restoration before perturbing the shared replica.
		t.Cleanup(func() {
			_, _ = replicaDB.Exec("START REPLICA SQL_THREAD")
			waitForReplicaCaughtUp(t, replicaDB, 60*time.Second)
		})
		if _, err := replicaDB.Exec("STOP REPLICA SQL_THREAD"); err != nil {
			t.Fatalf("STOP REPLICA SQL_THREAD: %v", err)
		}
		if state, ok := replicaStatusField(t, replicaDB, "Replica_SQL_Running"); !ok || state != "No" {
			t.Fatalf("precondition Replica_SQL_Running = %q/%v, want No/true", state, ok)
		}
		t.Log("precondition: Replica_IO_Running=Yes Replica_SQL_Running=No")

		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		outcomeCh := make(chan purgeExecuteOutcome, 1)
		go func() {
			result, err := orch.Execute(ctx)
			outcomeCh <- purgeExecuteOutcome{result: result, err: err}
		}()
		observer.waitFor(t, "replication hold:", 20*time.Second)
		assertPurgeStillRunning(t, outcomeCh)
		assertRootSet(t, sourceObserver, "source while SQL applier stopped", resumeScenarioRoots...)
		assertRootSet(t, destObserver, "destination while SQL applier stopped")

		if _, err := replicaDB.Exec("START REPLICA SQL_THREAD"); err != nil {
			t.Fatalf("START REPLICA SQL_THREAD: %v", err)
		}
		out := awaitPurgeOutcome(t, outcomeCh, 60*time.Second)
		if out.err != nil || out.result == nil || !out.result.Success {
			t.Fatalf("same Execute after recovery = result %+v err %v", out.result, out.err)
		}
		assertRootSet(t, sourceObserver, "source after stopped-applier recovery")
		assertRootSet(t, destObserver, "destination after stopped-applier recovery")
		waitForReplicaCaughtUp(t, replicaDB, 60*time.Second)
		t.Log("restored state: IO=Yes SQL=Yes Seconds_Behind_Source=0")
	})

	t.Run("lagged-applier", func(t *testing.T) {
		const jobName = "purge_live_lagged"
		resetReleaseRecoveryData(t, sourceDB, destDB)
		waitForReplicaCaughtUp(t, replicaDB, 60*time.Second)
		observer := newPurgeLogObserver(t)
		orch := newLivePurge(t, setup, jobName, 1, observer, destDB)

		if _, err := sourceDB.Exec("CREATE TABLE IF NOT EXISTS repl_purge_lag_probe (id INT PRIMARY KEY AUTO_INCREMENT)"); err != nil {
			t.Fatalf("create lag probe: %v", err)
		}
		t.Cleanup(func() { _, _ = sourceObserver.Exec("DROP TABLE IF EXISTS repl_purge_lag_probe") })
		t.Cleanup(func() {
			setReplicaSourceDelay(t, replicaDB, 0)
			waitForReplicaCaughtUp(t, replicaDB, 60*time.Second)
		})
		setReplicaSourceDelay(t, replicaDB, 120)
		if _, err := sourceDB.Exec("INSERT INTO repl_purge_lag_probe () VALUES ()"); err != nil {
			t.Fatalf("write lag probe: %v", err)
		}
		observed := waitForReplicaLagAbove(t, replicaDB, 1, 30*time.Second)
		t.Logf("precondition: Replica_IO_Running=Yes Replica_SQL_Running=Yes Seconds_Behind_Source=%d (>1)", observed)

		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		outcomeCh := make(chan purgeExecuteOutcome, 1)
		go func() {
			result, err := orch.Execute(ctx)
			outcomeCh <- purgeExecuteOutcome{result: result, err: err}
		}()
		observer.waitFor(t, "replication hold:", 20*time.Second)
		assertPurgeStillRunning(t, outcomeCh)
		assertRootSet(t, sourceObserver, "source while replica lagged", resumeScenarioRoots...)
		assertRootSet(t, destObserver, "destination while replica lagged")

		setReplicaSourceDelay(t, replicaDB, 0)
		waitForReplicaCaughtUp(t, replicaDB, 60*time.Second)
		out := awaitPurgeOutcome(t, outcomeCh, 60*time.Second)
		if out.err != nil || out.result == nil || !out.result.Success {
			t.Fatalf("same Execute after lag recovery = result %+v err %v", out.result, out.err)
		}
		assertRootSet(t, sourceObserver, "source after lag recovery")
		assertRootSet(t, destObserver, "destination after lag recovery")
		t.Log("restored state: SQL_Delay=0 IO=Yes SQL=Yes Seconds_Behind_Source=0")
	})

	t.Run("cancel-held", func(t *testing.T) {
		const jobName = "purge_live_cancel"
		resetReleaseRecoveryData(t, sourceDB, destDB)
		waitForReplicaCaughtUp(t, replicaDB, 60*time.Second)
		observer := newPurgeLogObserver(t)
		orch := newLivePurge(t, setup, jobName, 10, observer, destDB)

		t.Cleanup(func() {
			_, _ = replicaDB.Exec("START REPLICA SQL_THREAD")
			waitForReplicaCaughtUp(t, replicaDB, 60*time.Second)
		})
		if _, err := replicaDB.Exec("STOP REPLICA SQL_THREAD"); err != nil {
			t.Fatalf("STOP REPLICA SQL_THREAD: %v", err)
		}
		if state, ok := replicaStatusField(t, replicaDB, "Replica_SQL_Running"); !ok || state != "No" {
			t.Fatalf("precondition Replica_SQL_Running = %q/%v, want No/true", state, ok)
		}
		t.Log("precondition: Replica_IO_Running=Yes Replica_SQL_Running=No (cancel case)")

		ctx, cancel := context.WithCancel(context.Background())
		outcomeCh := make(chan purgeExecuteOutcome, 1)
		go func() {
			result, err := orch.Execute(ctx)
			outcomeCh <- purgeExecuteOutcome{result: result, err: err}
		}()
		observer.waitFor(t, "replication hold:", 20*time.Second)
		assertPurgeStillRunning(t, outcomeCh)
		assertRootSet(t, sourceObserver, "source before held cancellation", resumeScenarioRoots...)
		assertRootSet(t, destObserver, "destination before held cancellation")
		cancel()
		out := awaitPurgeOutcome(t, outcomeCh, 10*time.Second)
		if !errors.Is(out.err, context.Canceled) {
			t.Fatalf("cancelled Execute error = %v, want context.Canceled", out.err)
		}
		assertRootSet(t, sourceObserver, "source after held cancellation", resumeScenarioRoots...)
		assertRootSet(t, destObserver, "destination after held cancellation")

		if _, err := replicaDB.Exec("START REPLICA SQL_THREAD"); err != nil {
			t.Fatalf("START REPLICA SQL_THREAD: %v", err)
		}
		waitForReplicaCaughtUp(t, replicaDB, 60*time.Second)
		t.Log("restored state: IO=Yes SQL=Yes Seconds_Behind_Source=0 after cancellation")
	})
}
