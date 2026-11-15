//go:build integration

package testestate

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"

	"github.com/dbsmedya/goarchive/v3/internal/database"
)

// rootPool opens a pool on the estate's replica as goarchive opens its own.
func rootPool(t *testing.T) *sql.DB {
	t.Helper()
	cfg, err := replicaEnv()
	if err != nil {
		t.Fatal(err)
	}
	db, err := database.OpenPool(t.Context(), &cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestMySQLReplicaLockIsExclusive: a second session asking for the replica waits until the first
// releases it, and gives up with a message naming the lock when it waits for the whole of its time.
func TestMySQLReplicaLockIsExclusive(t *testing.T) {
	cfg, err := replicaEnv()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first, err := acquire(ctx, cfg, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(first)

	got := make(chan error, 1)
	var second func()
	go func() {
		var err error
		second, err = acquire(ctx, cfg, 60*time.Second)
		got <- err
	}()
	select {
	case err := <-got:
		t.Fatalf("a second session got the replica while the first held it: %v", err)
	case <-time.After(1500 * time.Millisecond):
	}
	first()
	select {
	case err := <-got:
		if err != nil {
			t.Fatalf("the second session did not get the replica after the first released it: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the second session still waits 20 s after the first released the replica")
	}

	// a third gives up, naming what it waited for
	_, err = acquire(ctx, cfg, 1*time.Second)
	if err == nil || !strings.Contains(err.Error(), replicaLockName) {
		t.Errorf("a third session = %v, want it to give up and name the lock %q", err, replicaLockName)
	}
	second()
}

// TestMySQLReplicaLockIsHeldOncePerTopLevelTest: a subtest asks again for what its test holds and is
// not blocked by its own parent's session.
func TestMySQLReplicaLockIsHeldOncePerTopLevelTest(t *testing.T) {
	LockReplica(t)
	for _, name := range []string{"first", "second"} {
		t.Run(name, func(t *testing.T) {
			done := make(chan struct{})
			go func() { LockReplica(t); close(done) }()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("a subtest was blocked by the lock its own test holds")
			}
		})
	}
}

// TestMySQLReplicaStateNamesWhatDiffers: the reads the lock's base-state check makes, against a real replica
// that the test changes under the lock and restores, reading each restore back.
func TestMySQLReplicaStateNamesWhatDiffers(t *testing.T) {
	LockReplica(t)
	db := rootPool(t)
	ctx := context.Background()
	exec := func(stmts ...string) {
		t.Helper()
		for _, s := range stmts {
			if _, err := db.ExecContext(ctx, s); err != nil {
				t.Fatalf("%s: %v", s, err)
			}
		}
	}
	problems := func() []string {
		t.Helper()
		got, err := replicaState(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	wantProblem := func(what, want string) {
		t.Helper()
		if got := strings.Join(problems(), "; "); !strings.Contains(got, want) {
			t.Errorf("%s: problems = %q, want one naming %q", what, got, want)
		}
	}
	restored := func(what string) {
		t.Helper()
		if got := problems(); len(got) != 0 {
			t.Errorf("after restoring %s the replica still differs: %v", what, got)
		}
	}
	if got := problems(); len(got) != 0 {
		t.Fatalf("the replica starts in a changed state: %v", got)
	}

	var ro, sro int
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.read_only, @@GLOBAL.super_read_only").Scan(&ro, &sro); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(ctx, "SET GLOBAL super_read_only = 0")
		_, _ = db.ExecContext(ctx, "SET GLOBAL read_only = 0")
		for _, s := range []string{"STOP REPLICA SQL_THREAD", "CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 0", "START REPLICA SQL_THREAD"} {
			_, _ = db.ExecContext(ctx, s)
		}
	})

	exec("SET GLOBAL read_only = 1")
	wantProblem("read_only", "read_only is ON")
	exec("SET GLOBAL super_read_only = 1")
	wantProblem("super_read_only", "super_read_only is ON")
	exec("SET GLOBAL super_read_only = 0", "SET GLOBAL read_only = 0")
	restored("read_only")

	exec("STOP REPLICA SQL_THREAD")
	wantProblem("a stopped applier", "the SQL thread is No")
	exec("START REPLICA SQL_THREAD")
	restored("the applier")

	exec("STOP REPLICA SQL_THREAD", "CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 7", "START REPLICA SQL_THREAD")
	wantProblem("a delay", "SOURCE_DELAY is 7")
	exec("STOP REPLICA SQL_THREAD", "CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 0", "START REPLICA SQL_THREAD")
	restored("SOURCE_DELAY")

	// the library's own reads agree with what the check read
	cfg, err := dbsrepl.NewInspector(db).ReplicationConfig(ctx)
	if err != nil || cfg.ReadOnly || cfg.SuperReadOnly {
		t.Errorf("ReplicationConfig after the restores = %+v, %v", cfg, err)
	}
}
