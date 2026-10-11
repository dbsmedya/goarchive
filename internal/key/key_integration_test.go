//go:build integration

package key

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
	"github.com/go-sql-driver/mysql"
)

// sourceConn opens one pinned connection to the integration source (tests/.env TEST_SOURCE_*),
// with the session setup production uses: UTC pin, sql_notes, prepared statements. It fails,
// never skips, when the environment is not loaded (tests/README.md § Prerequisites).
func sourceConn(t *testing.T) *sql.Conn {
	t.Helper()
	host, user, pass, db := os.Getenv("TEST_SOURCE_HOST"), os.Getenv("TEST_SOURCE_USER"),
		os.Getenv("TEST_SOURCE_PASSWORD"), os.Getenv("TEST_SOURCE_DB")
	if host == "" || user == "" || pass == "" || db == "" {
		t.Fatal("integration environment not loaded: set -a; source tests/.env; set +a")
	}
	port := 3305
	if p := os.Getenv("TEST_SOURCE_PORT"); p != "" {
		port, _ = strconv.Atoi(p)
	}
	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd, cfg.Net, cfg.Addr, cfg.DBName = user, pass, "tcp", fmt.Sprintf("%s:%d", host, port), db
	cfg.ParseTime = true
	cfg.Params = map[string]string{"time_zone": "'+00:00'", "sql_notes": "1", "autocommit": "1"}
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB := sql.OpenDB(connector)
	t.Cleanup(func() { _ = sqlDB.Close() })
	conn, err := sqlDB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	// Fixture rows are generated with recursive CTEs; the default depth is 1,000.
	if _, err := conn.ExecContext(context.Background(), "SET SESSION cte_max_recursion_depth = 100000"); err != nil {
		t.Fatal(err)
	}
	return conn
}

func mustExec(t *testing.T, c *sql.Conn, q string, args ...any) {
	t.Helper()
	if _, err := c.ExecContext(context.Background(), q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

func count(t *testing.T, c *sql.Conn, q string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := c.QueryRowContext(context.Background(), q, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

func readKeys(t *testing.T, c *sql.Conn, d Def, q string, args ...any) []Key {
	t.Helper()
	rows, err := c.QueryContext(context.Background(), q, args...)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer rows.Close()
	var out []Key
	for rows.Next() {
		k, err := d.Scan(rows)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, k)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestAfterWalksEveryRowOnceUnderCaseInsensitiveCollation pages through a composite key
// whose second column sorts case- and accent-insensitively, using only After/OrderBy, and
// checks that every row is visited exactly once (spec §4.3, probe P1).
func TestAfterWalksEveryRowOnceUnderCaseInsensitiveCollation(t *testing.T) {
	c := sourceConn(t)
	mustExec(t, c, "CREATE TEMPORARY TABLE ga3_walk (a INT NOT NULL, s VARCHAR(20) COLLATE utf8mb4_0900_ai_ci NOT NULL, PRIMARY KEY (a, s))")
	mustExec(t, c, `INSERT INTO ga3_walk
		WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 3000)
		SELECT 1 + i % 30, CONCAT(IF(i % 2 = 0, 'x', 'X'), LPAD(i, 5, '0'), IF(i % 3 = 0, 'é', 'e')) FROM n`)
	d := Def{Table: "ga3_walk", Columns: []Column{
		{Name: "a", Family: FamilySigned},
		{Name: "s", Family: FamilyString, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"},
	}}
	total := count(t, c, "SELECT COUNT(*) FROM ga3_walk")
	seen := NewSet()
	first := "SELECT " + Projection(d) + " FROM ga3_walk ORDER BY " + OrderBy(d) + " LIMIT 97"
	next := "SELECT " + Projection(d) + " FROM ga3_walk WHERE " + After(d) + " ORDER BY " + OrderBy(d) + " LIMIT 97"
	page := readKeys(t, c, d, first)
	for len(page) > 0 {
		for _, k := range page {
			if !seen.Add(k) {
				t.Fatalf("key %v visited twice", k)
			}
		}
		page = readKeys(t, c, d, next, AfterArgs(page[len(page)-1])...)
	}
	if int64(len(seen.Keys())) != total {
		t.Fatalf("visited %d rows, table has %d", len(seen.Keys()), total)
	}
}

// TestInMatchesExactlyPerFamily reads keys back with Projection/Scan and checks that In
// with Args selects exactly those rows, one key at a time and all at once, for the key
// families where a lossy bind would over- or under-match (spec §4.2 rule 3, probes P3/P5).
func TestInMatchesExactlyPerFamily(t *testing.T) {
	c := sourceConn(t)
	cases := []struct {
		name, ddl, insert string
		col               Column
	}{
		{"decimal20", "DECIMAL(20,0)", "(12345678901234567),(12345678901234568),(12345678901234569),(5)",
			Column{Family: FamilyDecimal, Precision: 20, Scale: 0}},
		{"decimal30_10_unsigned", "DECIMAL(30,10) UNSIGNED", "(1.0000000001),(1.0000000002),(1.0000000003)",
			Column{Family: FamilyDecimal, Precision: 30, Scale: 10}},
		{"bigint_unsigned", "BIGINT UNSIGNED", "(9007199254740992),(9007199254740993),(9223372036854775808),(18446744073709551615)",
			Column{Family: FamilyUnsigned}},
		{"timestamp6", "TIMESTAMP(6)", "('2024-03-31 00:59:59.999999'),('2024-03-31 01:00:00.000000'),('2024-03-31 01:00:00.000001')",
			Column{Family: FamilyTimestamp, Fsp: 6}},
		{"datetime3", "DATETIME(3)", "('2024-01-01 00:00:00.001'),('2024-01-01 00:00:00.002')",
			Column{Family: FamilyDateTime, Fsp: 3}},
		{"char_nopad", "CHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci", "('a'),('b  '),('c')",
			Column{Family: FamilyString, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}},
		// utf8mb4_0900_bin is NO PAD, so 'a' and 'a ' are two keys; utf8mb4_bin (PAD SPACE) would refuse them.
		{"varchar_nopad_bin_case", "VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin", "('abc'),('ABC'),('a'),('a ')",
			Column{Family: FamilyString, Charset: "utf8mb4", Collation: "utf8mb4_0900_bin"}},
		{"long_varchar", "VARCHAR(300) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin", "(CONCAT(REPEAT('x', 255), 'y')),(REPEAT('x', 255))",
			Column{Family: FamilyString, Charset: "utf8mb4", Collation: "utf8mb4_0900_bin"}},
		{"latin1", "VARCHAR(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci", "('café'),('cafe1'),('Ñu')",
			Column{Family: FamilyString, Charset: "latin1", Collation: "latin1_swedish_ci"}},
		{"varbinary", "VARBINARY(16)", "(UNHEX('FF00FE')),(UNHEX('FF00')),(UNHEX('C3'))",
			Column{Family: FamilyBinary}},
		{"binary16", "BINARY(16)", "(UNHEX('00112233445566778899AABBCCDDEEFF')),(UNHEX('FF000000000000000000000000000000'))",
			Column{Family: FamilyBinary, Length: 16}},
		{"date", "DATE", "('2024-02-29'),('0001-01-01'),('9999-12-31')",
			Column{Family: FamilyDate}},
		// R46: values CAST(? AS TIME(n)) mis-converts on 8.0, 8.4 and 9.7, beside the edges.
		{"time2", "TIME(2)", "('-01:00:00.50'),('00:00:00.00'),('838:59:59.00'),('10:00:00.01'),('09:06:03.18'),('14:06:23.05')",
			Column{Family: FamilyTime, Fsp: 2}},
		{"time0", "TIME", "('03:03:00'),('04:00:09'),('05:11:06'),('-838:59:59')",
			Column{Family: FamilyTime}},
		{"time1", "TIME(1)", "('01:09:13.1'),('06:12:22.2'),('09:08:13.1')",
			Column{Family: FamilyTime, Fsp: 1}},
		{"year", "YEAR", "(1901),(2000),(2155)",
			Column{Family: FamilyYear}},
		// Empty keys are values, not NULL: they must bind as '' and self-match.
		{"varchar_empty", "VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin", "(''),('a')",
			Column{Family: FamilyString, Charset: "utf8mb4", Collation: "utf8mb4_0900_bin"}},
		{"varbinary_empty", "VARBINARY(4)", "(''),(UNHEX('00'))",
			Column{Family: FamilyBinary}},
		// An all-space CHAR value is read back stripped to ''.
		{"char_all_spaces", "CHAR(3) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci", "('   '),('b')",
			Column{Family: FamilyString, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			table := "ga3_in_" + tc.name
			mustExec(t, c, "CREATE TEMPORARY TABLE "+table+" (k "+tc.ddl+" NOT NULL PRIMARY KEY)")
			mustExec(t, c, "INSERT INTO "+table+" VALUES "+tc.insert)
			col := tc.col
			col.Name = "k"
			d := Def{Table: table, Columns: []Column{col}}
			keys := readKeys(t, c, d, "SELECT "+Projection(d)+" FROM "+table)
			total := count(t, c, "SELECT COUNT(*) FROM "+table)
			if int64(len(keys)) != total {
				t.Fatalf("read %d keys, table has %d", len(keys), total)
			}
			for _, k := range keys {
				q := "SELECT COUNT(*) FROM " + table + " WHERE " + In(d.Names(), d.Columns, 1)
				if n := count(t, c, q, Args([]Key{k})...); n != 1 {
					t.Fatalf("key %v matched %d rows, want 1", k, n)
				}
			}
			q := "SELECT COUNT(*) FROM " + table + " WHERE " + In(d.Names(), d.Columns, len(keys))
			if n := count(t, c, q, Args(keys)...); n != total {
				t.Fatalf("all keys matched %d rows, want %d", n, total)
			}
		})
	}
}

// skipPastPacket skips the test when the server's max_allowed_packet cannot carry a
// statement of need bytes (key.StatementBytes, the bound MAX_ALLOWED_PACKET_CHECK uses).
// These tests send their statements straight to MySQL, past preflight, so under the
// environment matrix's max_allowed_packet_64k row their premise cannot hold (M7c-2).
func skipPastPacket(t *testing.T, c *sql.Conn, need uint64) {
	t.Helper()
	var packet uint64
	if err := c.QueryRowContext(context.Background(), "SELECT @@SESSION.max_allowed_packet").Scan(&packet); err != nil {
		t.Fatal(err)
	}
	if need > packet {
		t.Skipf("the test's statement needs about %d bytes and max_allowed_packet is %d (the environment matrix row max_allowed_packet_64k): preflight would refuse such a run", need, packet)
	}
}

// rangeFixture is a 10,000-row composite-key table on c under a 1 MiB range budget. run
// reads n of its keys in one IN list and returns CheckRangeWarning's verdict; capped is
// MaxTuples for that budget.
func rangeFixture(t *testing.T, c *sql.Conn) (run func(n int) error, capped int) {
	t.Helper()
	ctx := context.Background()
	mustExec(t, c, "CREATE TEMPORARY TABLE ga3_cap (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b))")
	mustExec(t, c, `INSERT INTO ga3_cap
		WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < 9999)
		SELECT i DIV 100, i % 100 FROM n`)
	mustExec(t, c, "SET SESSION range_optimizer_max_mem_size = 1048576")
	t.Cleanup(func() { _, _ = c.ExecContext(ctx, "SET SESSION range_optimizer_max_mem_size = DEFAULT") })
	d := Def{Table: "ga3_cap", Columns: []Column{{Name: "a", Family: FamilySigned}, {Name: "b", Family: FamilySigned}}}
	keysOf := func(n int) []Key {
		out := make([]Key, n)
		for i := range out {
			out[i] = Key{int64(i / 100), int64(i % 100)}
		}
		return out
	}
	run = func(n int) error {
		keys := keysOf(n)
		skipPastPacket(t, c, StatementBytes(d.Names(), d.Columns, n, 0, 64))
		rows, err := c.QueryContext(ctx, "SELECT a, b FROM ga3_cap WHERE "+In(d.Names(), d.Columns, n), Args(keys)...)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		return CheckRangeWarning(ctx, c)
	}
	return run, MaxTuples(d.Columns, 1048576)
}

// TestCheckRangeWarningDetectsFullScanFallback lowers the session's range budget so a long
// IN list exceeds it, and checks that CheckRangeWarning reports ErrRangeScan, while a list
// inside the cap reports nothing (spec §4.3 statement size cap, §7.1a, probe P2).
func TestCheckRangeWarningDetectsFullScanFallback(t *testing.T) {
	run, capped := rangeFixture(t, sourceConn(t))
	if err := run(capped); err != nil {
		t.Fatalf("a statement at the cap reported %v", err)
	}
	if err := run(5000); !errors.Is(err, ErrRangeScan) {
		t.Fatalf("a statement past the budget reported %v, want ErrRangeScan", err)
	}
}

// TestCheckRangeWarningFailsClosedWithoutRetainedWarnings (M7c F2): a session whose
// max_error_count is 0 keeps no warnings, so SHOW WARNINGS cannot show 3170 and the check
// must refuse a statement that raised any warning it cannot read, naming max_error_count.
// Pair (R56, at session scope): the same statement past the budget is refused as
// unreadable here and as ErrRangeScan under the default capacity
// (TestCheckRangeWarningDetectsFullScanFallback); a statement that raised no warning still
// passes at capacity 0.
func TestCheckRangeWarningFailsClosedWithoutRetainedWarnings(t *testing.T) {
	c := sourceConn(t)
	run, capped := rangeFixture(t, c)
	mustExec(t, c, "SET SESSION max_error_count = 0")
	t.Cleanup(func() { _, _ = c.ExecContext(context.Background(), "SET SESSION max_error_count = DEFAULT") })
	if err := run(capped); err != nil {
		t.Fatalf("a statement at the cap, no warnings, at capacity 0 reported %v", err)
	}
	err := run(5000)
	if err == nil || errors.Is(err, ErrRangeScan) || !strings.Contains(err.Error(), "max_error_count") {
		t.Fatalf("a statement past the budget at capacity 0 reported %v, want a refusal naming max_error_count", err)
	}
}

// TestFromFactsReadsSakilaKeys builds Defs from dbsgomysql facts for a composite and a
// single-column key on the source's Sakila schema (spec §4.1, §8.4).
func TestFromFactsReadsSakilaKeys(t *testing.T) {
	c := sourceConn(t)
	ctx := context.Background()
	schema := os.Getenv("TEST_SOURCE_DB")
	in := validations.NewInspector(c, schema)
	pks, err := in.PrimaryKeys(ctx, []string{"film_actor", "payment"})
	if err != nil {
		t.Fatal(err)
	}
	if len(pks) != 2 {
		t.Fatalf("PrimaryKeys returned %d facts", len(pks))
	}
	want := map[string]string{"film_actor": "actor_id,film_id", "payment": "payment_id"}
	for _, pk := range pks {
		spec, err := in.TableSpec(ctx, validations.Ref(schema, pk.Table), validations.WithIndexes())
		if err != nil {
			t.Fatal(err)
		}
		d, err := FromFacts(pk, spec)
		if err != nil {
			t.Fatalf("FromFacts(%s): %v", pk.Table, err)
		}
		if got := strings.Join(d.Names(), ","); got != want[pk.Table] {
			t.Fatalf("%s key = %s, want %s", pk.Table, got, want[pk.Table])
		}
		for _, col := range d.Columns {
			if col.Family != FamilyUnsigned {
				t.Fatalf("%s.%s family = %s, want unsigned (Sakila keys are SMALLINT UNSIGNED)", pk.Table, col.Name, col.Family)
			}
		}
	}
}

// TestAfterWalksEmptyKeyComponents pages through a composite key whose string component is
// empty in some rows: continuation must not compare against NULL and skip rows.
func TestAfterWalksEmptyKeyComponents(t *testing.T) {
	c := sourceConn(t)
	mustExec(t, c, "CREATE TEMPORARY TABLE ga3_empty (a INT NOT NULL, s VARCHAR(10) COLLATE utf8mb4_0900_bin NOT NULL, PRIMARY KEY (a, s))")
	mustExec(t, c, "INSERT INTO ga3_empty VALUES (1,''),(1,'a'),(2,''),(2,'b'),(3,'')")
	d := Def{Table: "ga3_empty", Columns: []Column{
		{Name: "a", Family: FamilySigned},
		{Name: "s", Family: FamilyString, Charset: "utf8mb4", Collation: "utf8mb4_0900_bin"},
	}}
	seen := NewSet()
	first := "SELECT " + Projection(d) + " FROM ga3_empty ORDER BY " + OrderBy(d) + " LIMIT 1"
	next := "SELECT " + Projection(d) + " FROM ga3_empty WHERE " + After(d) + " ORDER BY " + OrderBy(d) + " LIMIT 1"
	page := readKeys(t, c, d, first)
	for len(page) > 0 {
		if !seen.Add(page[0]) {
			t.Fatalf("key %v visited twice", page[0])
		}
		page = readKeys(t, c, d, next, AfterArgs(page[0])...)
	}
	if len(seen.Keys()) != 5 {
		t.Fatalf("visited %d rows, want 5", len(seen.Keys()))
	}
}

// TestMaxTuplesStaysUnderBudgetForWideKeys runs a membership statement of exactly MaxTuples
// tuples, at the default 8 MiB budget, for keys whose parts are wide. The range optimizer's
// memory grows with each part's byte width, not only with the number of parts: on 8.0.46,
// 8.4.11 and 9.7.1 a utf8mb4 VARCHAR(255) key falls back to a full scan past about 6,400
// tuples (decisions/2026-10-09-m1-width-aware-statement-cap.md).
func TestMaxTuplesStaysUnderBudgetForWideKeys(t *testing.T) {
	c := sourceConn(t)
	ctx := context.Background()
	const budget = 8 << 20
	mustExec(t, c, "SET SESSION range_optimizer_max_mem_size = 8388608")
	t.Cleanup(func() { _, _ = c.ExecContext(ctx, "SET SESSION range_optimizer_max_mem_size = DEFAULT") })
	wide := Column{Name: "s", Family: FamilyString, Length: 255, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}
	cases := []struct {
		name string
		ddl  string
		cols []Column
	}{
		{"varchar255_utf8mb4", "s VARCHAR(255) CHARACTER SET utf8mb4 NOT NULL, PRIMARY KEY (s)", []Column{wide}},
		{"int_varchar255_utf8mb4", "a INT NOT NULL, s VARCHAR(255) CHARACTER SET utf8mb4 NOT NULL, PRIMARY KEY (a, s)",
			[]Column{{Name: "a", Family: FamilySigned}, wide}},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			table := fmt.Sprintf("ga3_wide_%d", i)
			mustExec(t, c, "CREATE TEMPORARY TABLE "+table+" ("+tc.ddl+")")
			d := Def{Table: table, Columns: tc.cols}
			n := MaxTuples(d.Columns, budget)
			keys := make([]Key, n)
			for j := range keys {
				k := make(Key, len(d.Columns))
				for p, col := range d.Columns {
					if col.Family == FamilySigned {
						k[p] = int64(j)
					} else {
						k[p] = []byte(fmt.Sprintf("k%07d", j))
					}
				}
				keys[j] = k
			}
			rows, err := c.QueryContext(ctx, "SELECT 1 FROM "+table+" WHERE "+In(d.Names(), d.Columns, n), Args(keys)...)
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			if err := CheckRangeWarning(ctx, c); err != nil {
				t.Fatalf("MaxTuples = %d tuples: %v", n, err)
			}
		})
	}
}

// TestAfterUsesRangeAccessOnThreeColumnKey checks that the expanded disjunction stays a range
// scan for a key wider than the two columns probe P1 measured. Near the end of a
// 64,000-row table, a page of 100 must read about 100 index entries. The row-constructor
// form (a,b,c) > (?,?,?) walks the index from its start and reads about 63,000.
func TestAfterUsesRangeAccessOnThreeColumnKey(t *testing.T) {
	c := sourceConn(t)
	ctx := context.Background()
	mustExec(t, c, "CREATE TEMPORARY TABLE ga3_three (a INT NOT NULL, b INT NOT NULL, c INT NOT NULL, pad INT, PRIMARY KEY (a, b, c))")
	mustExec(t, c, `INSERT INTO ga3_three
		WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < 63999)
		SELECT i DIV 1600, i DIV 40 % 40, i % 40, i FROM n`)
	mustExec(t, c, "ANALYZE TABLE ga3_three")
	d := Def{Table: "ga3_three", Columns: []Column{
		{Name: "a", Family: FamilySigned}, {Name: "b", Family: FamilySigned}, {Name: "c", Family: FamilySigned}}}
	last := Key{int64(39), int64(20), int64(5)}
	q := "SELECT " + Projection(d) + " FROM ga3_three WHERE " + After(d) + " ORDER BY " + OrderBy(d) + " LIMIT 100"

	mustExec(t, c, "FLUSH STATUS")
	keys := readKeys(t, c, d, q, AfterArgs(last)...)
	var name string
	var reads int64
	if err := c.QueryRowContext(ctx, "SHOW SESSION STATUS LIKE 'Handler_read_next'").Scan(&name, &reads); err != nil {
		t.Fatal(err)
	}

	if len(keys) != 100 || Encode(keys[0]) != Encode(Key{int64(39), int64(20), int64(6)}) {
		t.Fatalf("page starts at %v with %d keys, want (39,20,6) and 100", keys[0], len(keys))
	}
	if reads > 1000 {
		t.Fatalf("Handler_read_next = %d for a 100-row page: the continuation is not a range scan", reads)
	}
}

// TestKeyWireBytesFollowTheResultCharset (M7c F3, Codex M7c-1 review B1): a character key is
// read back in the session's character_set_results, utf8mb4, whatever its column's charset,
// and bound with those bytes. A latin1 '€' is one byte stored and three read, so the packet
// bound must charge the read bytes, not the stored ones. Read through the production path:
// the key projection and Def.Scan on a session like the DSN's.
func TestKeyWireBytesFollowTheResultCharset(t *testing.T) {
	c := sourceConn(t)
	ctx := context.Background()
	var results string
	if err := c.QueryRowContext(ctx, "SELECT @@SESSION.character_set_results").Scan(&results); err != nil {
		t.Fatal(err)
	}
	mustExec(t, c, "CREATE TEMPORARY TABLE ga3_wire (k VARCHAR(10) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL, PRIMARY KEY (k))")
	mustExec(t, c, "INSERT INTO ga3_wire VALUES (REPEAT('€', 10))")
	d := Def{Table: "ga3_wire", Columns: []Column{{Name: "k", Family: FamilyString, Length: 10, Charset: "latin1", Collation: "latin1_bin"}}}
	rows, err := c.QueryContext(ctx, "SELECT "+Projection(d)+" FROM ga3_wire")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatal("no row")
	}
	k, err := d.Scan(rows)
	if err != nil {
		t.Fatal(err)
	}
	read := k[0].([]byte)
	t.Logf("character_set_results = %s; a latin1 VARCHAR(10) of ten '€' reads as %d bytes", results, len(read))
	if len(read) != 30 {
		t.Fatalf("read %d bytes, want 30 (utf8mb4 '€' is three)", len(read))
	}
	if got, sent := StatementBytes(d.Names(), d.Columns, 1, 0, 0), driverExecuteBytes([]any{read}); got < uint64(sent) {
		t.Errorf("StatementBytes = %d for one key, below the %d bytes the driver sends", got, sent)
	}
}
