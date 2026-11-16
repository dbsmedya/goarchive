// Package testestate is what the integration suites of different packages share on one test
// estate. It is test support: no production code imports it, and nothing in the binary reaches it.
//
// `go test` runs the packages of a stage at the same time, and the packages share the estate's
// servers. Two things follow, and this package holds both:
//
//   - The replica (db3, or gr-replica on the group estate) is used by tests that stop its
//     applier, delay it, make it read-only, or depend on its lag. LockReplica gives one test
//     the whole replica for the length of the test, across packages.
//   - A test that relies on a server being in its base state must not take it on trust: a killed
//     test process skips its cleanups, and what it changed stays. LockReplica and RequireGroupBase
//     read the state through the library and fail the test, naming what differs.
//
// The group estate has more to protect (a primary switch, a lowered size limit and a read-only
// destination disturb every test on it), and is run one package at a time instead
// (tests/README.md, The group estate).
package testestate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/database"
)

const (
	// replicaLockName is the GET_LOCK name every test that uses the replica takes, on the replica
	// itself. The lock belongs to a session, so a killed test process releases it with its connection.
	replicaLockName = "goarchive-test-estate-replica"
	// replicaWait is how long a test waits for another package's test to give the replica back. The
	// longest holder is a test of a minute or two.
	replicaWait = 10 * time.Minute
	// settle is how long a base-state check keeps looking at a state that a previous test's cleanup may
	// still be finishing (a thread that was just started), before it fails.
	settle = 10 * time.Second

	// The group estate's base values.
	estateSizeLimit = 150000000
	groupSize       = 3
)

// topLevel is the name of the top-level test that owns a test or subtest name.
func topLevel(name string) string {
	top, _, _ := strings.Cut(name, "/")
	return top
}

// replicaEnv is the replica's connection, from the estate's environment.
func replicaEnv() (config.DatabaseConfig, error) {
	return envConfig("TEST_REPLICA")
}

func envConfig(prefix string) (config.DatabaseConfig, error) {
	port, _ := strconv.Atoi(os.Getenv(prefix + "_PORT"))
	cfg := config.DatabaseConfig{Host: os.Getenv(prefix + "_HOST"), Port: port, User: os.Getenv(prefix + "_USER"),
		Password: os.Getenv(prefix + "_PASSWORD"), TLS: "disable"}
	if cfg.Host == "" || cfg.User == "" || port == 0 {
		return cfg, fmt.Errorf("integration environment not loaded (%s_*): set -a; source tests/.env; set +a", prefix)
	}
	return cfg, nil
}

// acquire takes the replica lock on its own session and returns the function that gives it back,
// which may be called more than once. It waits at most wait for another holder.
func acquire(ctx context.Context, cfg config.DatabaseConfig, wait time.Duration) (func(), error) {
	db, err := database.OpenPool(ctx, &cfg, true)
	if err != nil {
		return nil, fmt.Errorf("open the lock's session on %s:%d: %w", cfg.Host, cfg.Port, err)
	}
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("the lock's session on %s:%d: %w", cfg.Host, cfg.Port, err)
	}
	fail := func(err error) (func(), error) {
		_ = conn.Close()
		_ = db.Close()
		return nil, err
	}
	// GET_LOCK is a read-only SELECT, so a server-wide max_execution_time would cut its wait short.
	if _, err := conn.ExecContext(ctx, "SET SESSION max_execution_time = 0"); err != nil {
		return fail(fmt.Errorf("the lock's session on %s:%d: %w", cfg.Host, cfg.Port, err))
	}
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", replicaLockName, int(wait.Seconds())).Scan(&got); err != nil {
		return fail(fmt.Errorf("take the lock %q on %s:%d: %w", replicaLockName, cfg.Host, cfg.Port, err))
	}
	if !got.Valid || got.Int64 != 1 {
		return fail(fmt.Errorf("no session got the lock %q on %s:%d within %v: another suite holds the estate's replica "+
			"(its tests run in another package, or a test process that is still running)", replicaLockName, cfg.Host, cfg.Port, wait))
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			_, _ = conn.ExecContext(context.Background(), "SELECT RELEASE_LOCK(?)", replicaLockName)
			_ = conn.Close()
			_ = db.Close()
		})
	}, nil
}

var (
	mu   sync.Mutex
	held = map[string]*holding{}
)

// holding is a top-level test's claim on the replica; ready closes once the claim is made or has failed.
type holding struct {
	ready chan struct{}
	err   error
}

// LockReplica gives t the estate's replica for the rest of its top-level test, whatever package asks: it
// takes one exclusive lock on the replica itself, held on a session until the test's cleanups have run, so
// a test that stops the applier, sets SOURCE_DELAY, makes the replica read-only, or depends on its lag is
// never disturbed by another package's test. Cleanups run in reverse order, so the lock is given back after
// what the test registered after taking it.
//
// A test calls it, through the helper that opens the replica's pool, before it touches the replica. A
// subtest finds its test's claim and goes on; the claim of a subtest that comes first lasts as long as it.
// When the lock is taken the replica's base state is read and the test fails, naming what differs, if the
// replica is not in it: read_only and super_read_only off, both threads of the default channel running and
// no SOURCE_DELAY. That is state a killed test left behind, and it is not taken on trust.
func LockReplica(t testing.TB) {
	t.Helper()
	top := topLevel(t.Name())
	mu.Lock()
	if h, ok := held[top]; ok {
		mu.Unlock()
		<-h.ready
		if h.err != nil {
			t.Fatal(h.err)
		}
		return
	}
	h := &holding{ready: make(chan struct{})}
	held[top] = h
	mu.Unlock()

	fail := func(err error) {
		h.err = err
		close(h.ready)
		mu.Lock()
		delete(held, top)
		mu.Unlock()
		t.Fatal(err)
	}
	cfg, err := replicaEnv()
	if err != nil {
		fail(err)
	}
	release, err := acquire(context.Background(), cfg, replicaWait)
	if err != nil {
		fail(err)
	}
	t.Cleanup(func() {
		release()
		mu.Lock()
		delete(held, top)
		mu.Unlock()
	})
	if err := checkReplica(cfg); err != nil {
		h.err = err
		close(h.ready)
		t.Fatal(err)
	}
	close(h.ready)
}

// checkReplica reads the replica's state through the library until it is the base state, or until settle has
// passed, and returns an error naming what still differs.
func checkReplica(cfg config.DatabaseConfig) error {
	ctx := context.Background()
	db, err := database.OpenPool(ctx, &cfg, true)
	if err != nil {
		return fmt.Errorf("read the replica's state on %s:%d: %w", cfg.Host, cfg.Port, err)
	}
	defer func() { _ = db.Close() }()
	var problems []string
	for deadline := time.Now().Add(settle); ; time.Sleep(250 * time.Millisecond) {
		problems, err = replicaState(ctx, db)
		if err != nil {
			return fmt.Errorf("read the replica's state on %s:%d: %w", cfg.Host, cfg.Port, err)
		}
		if len(problems) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			break
		}
	}
	return fmt.Errorf("the estate's replica (%s:%d) is not in its base state: %s. Either a test that changed it did not restore it "+
		"(a killed test process skips its cleanups), or the estate was started a moment ago and the replica's I/O thread is still in its "+
		"reconnect wait (up to 60 s, Connect_Retry): wait for Replica_IO_Running: Yes and run again. To restore it by hand: "+
		"SET GLOBAL super_read_only = 0; SET GLOBAL read_only = 0; STOP REPLICA; CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 0; START REPLICA",
		cfg.Host, cfg.Port, strings.Join(problems, "; "))
}

// replicaState is what differs from the replica's base state, read through the library's facts. The
// library has no fact for SOURCE_DELAY, so the one value is read from the performance schema: this is test
// support, and the question is a library gap to put to its owner, not a statement the product sends.
func replicaState(ctx context.Context, db *sql.DB) ([]string, error) {
	insp := dbsrepl.NewInspector(db)
	cfg, err := insp.ReplicationConfig(ctx)
	if err != nil {
		return nil, err
	}
	channels, err := insp.ReplicaStatus(ctx)
	if err != nil {
		return nil, err
	}
	var delay sql.NullInt64
	err = db.QueryRowContext(ctx, "SELECT DESIRED_DELAY FROM performance_schema.replication_applier_configuration WHERE CHANNEL_NAME = ''").Scan(&delay)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("read SOURCE_DELAY: %w", err)
	}
	return replicaProblems(cfg, channels, delay.Int64), nil
}

// replicaProblems names every way the replica differs from its base state.
func replicaProblems(cfg dbsrepl.Config, channels []dbsrepl.ChannelStatus, delay int64) []string {
	var problems []string
	if cfg.ReadOnly {
		problems = append(problems, "read_only is ON")
	}
	if cfg.SuperReadOnly {
		problems = append(problems, "super_read_only is ON")
	}
	var def *dbsrepl.ChannelStatus
	for i := range channels {
		if channels[i].ChannelName == "" {
			def = &channels[i]
		}
	}
	if def == nil {
		problems = append(problems, "the replica has no default channel")
	} else {
		if def.IORunning != "Yes" {
			problems = append(problems, "the I/O thread is "+def.IORunning)
		}
		if def.SQLRunning != "Yes" {
			problems = append(problems, "the SQL thread is "+def.SQLRunning)
		}
	}
	if delay != 0 {
		problems = append(problems, fmt.Sprintf("SOURCE_DELAY is %d", delay))
	}
	return problems
}

// estateConsistency is the value of group_replication_consistency the group estate holds on every member:
// each series' own default, which the AdminAPI persists (tests/README.md, The group estate).
func estateConsistency(version string) string {
	if version == "8.0" || strings.HasPrefix(version, "8.0.") {
		return "EVENTUAL"
	}
	return "BEFORE_ON_PRIMARY_FAILOVER"
}

// memberFacts is what is read from one member of the group estate.
type memberFacts struct {
	name        string
	limit       dbsrepl.GroupReplication
	consistency dbsrepl.GroupConsistency
}

// groupProblems names every way the group differs from its base state: the three members all ONLINE in
// the view of the first, which is the PRIMARY, and on each member the mode single-primary, the size limit
// and the consistency at the estate's values.
func groupProblems(view []dbsrepl.GroupMember, members []memberFacts, wantLimit uint64, wantConsistency string) []string {
	var problems []string
	first := "the source"
	if len(members) > 0 {
		first = members[0].name
	}
	var rows []string
	for _, m := range view {
		rows = append(rows, strings.TrimSpace(m.Host+" "+m.Role+" "+m.State))
	}
	if len(view) != groupSize {
		problems = append(problems, fmt.Sprintf("the group has %d members in %s's view, want %d", len(view), first, groupSize))
	}
	for _, m := range view {
		if m.Self && m.Role != "PRIMARY" {
			problems = append(problems, fmt.Sprintf("%s is not the PRIMARY of its group (its view: %s)", first, strings.Join(rows, ", ")))
		}
		if m.State != "ONLINE" {
			problems = append(problems, fmt.Sprintf("%s is %s, not ONLINE", m.Host, m.State))
		}
	}
	for _, m := range members {
		switch {
		case !m.limit.TransactionSizeLimitKnown:
			problems = append(problems, m.name+": group_replication_transaction_size_limit is not reported")
		case m.limit.TransactionSizeLimit != wantLimit:
			problems = append(problems, fmt.Sprintf("%s: group_replication_transaction_size_limit is %d, want %d", m.name, m.limit.TransactionSizeLimit, wantLimit))
		}
		switch {
		case !m.limit.SinglePrimaryModeKnown:
			problems = append(problems, m.name+": group_replication_single_primary_mode is not reported")
		case m.limit.SinglePrimaryMode != "ON":
			problems = append(problems, fmt.Sprintf("%s: group_replication_single_primary_mode is %s, want ON", m.name, m.limit.SinglePrimaryMode))
		}
		switch {
		case !m.consistency.GlobalConsistencyKnown:
			problems = append(problems, m.name+": group_replication_consistency is not reported")
		case m.consistency.GlobalConsistency != wantConsistency:
			problems = append(problems, fmt.Sprintf("%s: group_replication_consistency is %s, want %s", m.name, m.consistency.GlobalConsistency, wantConsistency))
		}
	}
	return problems
}

// groupMembers are the estate's members as the environment names them: the source is gr1, the primary.
var groupMembers = []struct{ name, prefix string }{
	{"gr1", "TEST_SOURCE"}, {"gr2", "TEST_GROUP_MEMBER2"}, {"gr3", "TEST_GROUP_MEMBER3"},
}

// RequireGroupBase fails t, naming what differs, unless the group estate is in its base state: gr1 the
// PRIMARY and all three members ONLINE, single-primary, group_replication_transaction_size_limit at 150000000
// and group_replication_consistency at the series' own value on every member. A test that took the group
// already changed would build on state it did not make, so it does not go on. A member whose
// connection the environment does not name is not read, and the view still counts three.
//
// The group's tests call it after they have found the source to be a member, so it is never asked of a
// plain estate. The reads use sessions at EVENTUAL, which a fence does not hold, and are bounded.
func RequireGroupBase(t testing.TB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*settle+30*time.Second)
	defer cancel()
	var problems []string
	for deadline := time.Now().Add(settle); ; time.Sleep(500 * time.Millisecond) {
		var err error
		problems, err = readGroup(ctx)
		if err != nil {
			t.Fatalf("read the group estate's state: %v", err)
		}
		if len(problems) == 0 {
			return
		}
		if time.Now().After(deadline) {
			break
		}
	}
	t.Fatalf("the group estate is not in its base state: %s. A test that changed it did not restore it: a killed test process "+
		"skips its cleanups. Restore it by hand (SELECT group_replication_switch_to_single_primary_mode('<gr1's server_uuid>') "+
		"if the group is multi-primary; SELECT group_replication_set_as_primary('<gr1's server_uuid>'); "+
		"SET GLOBAL group_replication_transaction_size_limit = %d; SET GLOBAL group_replication_consistency = '<the series' value>' on each member)",
		strings.Join(problems, "; "), estateSizeLimit)
}

func readGroup(ctx context.Context) ([]string, error) {
	eventual := database.Global{Name: "group_replication_consistency", Value: "'EVENTUAL'"}
	var (
		view    []dbsrepl.GroupMember
		members []memberFacts
		want    string
	)
	for i, m := range groupMembers {
		cfg, err := envConfig(m.prefix)
		if err != nil {
			if i == 0 {
				return nil, err
			}
			continue
		}
		db, err := database.OpenPool(ctx, &cfg, true, eventual)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", m.name, err)
		}
		insp := dbsrepl.NewInspector(db)
		facts := memberFacts{name: m.name}
		facts.limit, err = insp.GroupReplication(ctx)
		if err == nil {
			facts.consistency, err = insp.GroupConsistency(ctx)
		}
		if err == nil && i == 0 {
			var version string
			if err = db.QueryRowContext(ctx, "SELECT @@version").Scan(&version); err == nil {
				want = estateConsistency(version)
				view, err = insp.GroupMembers(ctx)
			}
		}
		_ = db.Close()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", m.name, err)
		}
		members = append(members, facts)
	}
	return groupProblems(view, members, estateSizeLimit, want), nil
}
