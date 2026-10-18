//go:build integration

package preflight

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/database"
)

// loss counts what a character set's round trip through the session lost.
type loss struct {
	merged      int // a character read as the same value as an earlier one
	question    int // read as '?' although it is not '?'
	replacement int // read as U+FFFD although it is not U+FFFD
	empty       int // read as nothing
	invalid     int // read as bytes that are not UTF-8
	mismatch    int // the read value, bound back, does not match exactly its own row
	sample      []string
	stored      int             // the candidates MySQL stored as one character
	merges      map[string]bool // every merged pair, as "first=later" stored hex
	notUTF8     map[string]bool // the stored hex of every read that is not UTF-8
}

func (l loss) zero() bool {
	return l.merged == 0 && l.question == 0 && l.replacement == 0 && l.empty == 0 && l.invalid == 0 && l.mismatch == 0
}

func (l loss) String() string {
	return fmt.Sprintf("merged %d, '?' %d, U+FFFD %d, empty %d, not UTF-8 %d, bind mismatch %d; e.g. %s",
		l.merged, l.question, l.replacement, l.empty, l.invalid, l.mismatch, strings.Join(l.sample, "  "))
}

func (l *loss) note(s string) {
	if len(l.sample) < 5 {
		l.sample = append(l.sample, s)
	}
}

// sessionRoundTrip stores every candidate MySQL accepts as one character of cs, reads each
// through the session the pool's DSN pins, and, when bind is set, binds each read value back
// as GoArchive binds a key. Candidates MySQL does not store as given are dropped first.
func sessionRoundTrip(t *testing.T, db *sql.DB, cs string, cands [][]byte, bind bool) loss {
	t.Helper()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := conn.ExecContext(ctx, q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	coll := " COLLATE " + cs + "_bin"
	if cs == "binary" {
		coll = ""
	}
	exec("DROP TEMPORARY TABLE IF EXISTS rt")
	exec("CREATE TEMPORARY TABLE rt (id INT PRIMARY KEY, h VARBINARY(4) NOT NULL, c VARCHAR(1) CHARACTER SET " + cs + coll + ")")
	const batch = 2000
	for i := 0; i < len(cands); i += batch {
		j := min(i+batch, len(cands))
		var sb strings.Builder
		args := make([]any, 0, 2*(j-i))
		for k := i; k < j; k++ {
			if k > i {
				sb.WriteByte(',')
			}
			sb.WriteString("ROW(?,?)")
			args = append(args, k, fmt.Sprintf("%X", cands[k]))
		}
		exec("INSERT IGNORE INTO rt SELECT v.id, UNHEX(v.h), CONVERT(UNHEX(v.h) USING "+cs+") FROM (VALUES "+sb.String()+") AS v(id, h)", args...)
	}
	exec("DELETE FROM rt WHERE c IS NULL OR CHAR_LENGTH(c) <> 1 OR CAST(c AS BINARY) <> h")

	var qHex, fHex string
	if err := conn.QueryRowContext(ctx, "SELECT HEX(CONVERT('?' USING "+cs+")), HEX(CONVERT(_utf8mb4 X'EFBFBD' USING "+cs+"))").Scan(&qHex, &fHex); err != nil {
		t.Fatal(err)
	}
	type row struct {
		id   int
		hex  string
		read []byte
	}
	var stored []row
	rs, err := conn.QueryContext(ctx, "SELECT id, HEX(h), c FROM rt ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	for rs.Next() {
		var r row
		if err := rs.Scan(&r.id, &r.hex, &r.read); err != nil {
			t.Fatal(err)
		}
		stored = append(stored, r)
	}
	if err := rs.Err(); err != nil {
		t.Fatal(err)
	}
	_ = rs.Close()
	if len(stored) == 0 {
		t.Fatalf("%s: no candidate was stored", cs)
	}

	l := loss{stored: len(stored), merges: map[string]bool{}, notUTF8: map[string]bool{}}
	seen := map[string]string{}
	for _, r := range stored {
		merged := false
		if first, ok := seen[string(r.read)]; ok {
			l.merged++
			l.merges[first+"="+r.hex] = true
			merged = true
			l.note(fmt.Sprintf("%s=%s→%X", first, r.hex, r.read))
		} else {
			seen[string(r.read)] = r.hex
		}
		switch {
		case len(r.read) == 0:
			l.empty++
			l.note(r.hex + "→empty")
		case string(r.read) == "?" && r.hex != qHex:
			l.question++
			l.note(r.hex + "→?")
		case string(r.read) == "\uFFFD" && r.hex != fHex:
			l.replacement++
			l.note(r.hex + "→U+FFFD")
		case cs != "binary" && !utf8.Valid(r.read):
			// A ucs2 lone surrogate reads as EDA080, which MySQL refuses to bind back (3988).
			l.invalid++
			l.notUTF8[r.hex] = true
			l.note(fmt.Sprintf("%s→%X (not UTF-8)", r.hex, r.read))
			continue
		}
		if !bind || merged {
			continue
		}
		ids, err := conn.QueryContext(ctx, "SELECT id FROM rt WHERE c = ?", r.read)
		if err != nil {
			t.Fatal(err)
		}
		var got []int
		for ids.Next() {
			var id int
			if err := ids.Scan(&id); err != nil {
				t.Fatal(err)
			}
			got = append(got, id)
		}
		_ = ids.Close()
		if len(got) != 1 || got[0] != r.id {
			l.mismatch++
			l.note(fmt.Sprintf("%s→%X→ids %v", r.hex, r.read, got))
		}
	}
	return l
}

// serverRoundTrip converts every code point (the BMP for utf8mb3), surrogates excluded, to
// cs, to utf8mb4 and back, in one statement: the whole of a Unicode set is too large to bind
// from the client, and the session converts with these same functions.
func serverRoundTrip(t *testing.T, db *sql.DB, cs string) loss {
	t.Helper()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "SET SESSION cte_max_recursion_depth = 2000"); err != nil {
		t.Fatal(err)
	}
	maxCP, want := 0x10FFFF, 0x110000-0x800
	if cs == "utf8mb3" {
		maxCP, want = 0xFFFF, 0x10000-0x800
	}
	// CHAR(n USING utf32) is code point n, left-padded to four bytes.
	q := fmt.Sprintf(`WITH RECURSIVE n (i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < 1087),
cps AS (SELECT hi.i * 1024 + lo.i AS cp FROM n hi JOIN n lo ON lo.i < 1024
        WHERE hi.i * 1024 + lo.i <= %d AND hi.i * 1024 + lo.i NOT BETWEEN 0xD800 AND 0xDFFF),
x AS (SELECT cp, CONVERT(CHAR(cp USING utf32) USING %s) AS x FROM cps)
SELECT COUNT(*),
       COUNT(*) - COUNT(DISTINCT CAST(CONVERT(x USING utf8mb4) AS BINARY)),
       COALESCE(SUM(cp <> 0x3F AND CAST(CONVERT(x USING utf8mb4) AS BINARY) = X'3F'), 0),
       COALESCE(SUM(cp <> 0xFFFD AND CAST(CONVERT(x USING utf8mb4) AS BINARY) = X'EFBFBD'), 0),
       COALESCE(SUM(CAST(CONVERT(CONVERT(x USING utf8mb4) USING %s) AS BINARY) <> CAST(x AS BINARY)), 0)
FROM x`, maxCP, cs, cs)
	var n int
	var l loss
	if err := conn.QueryRowContext(ctx, q).Scan(&n, &l.merged, &l.question, &l.replacement, &l.mismatch); err != nil {
		t.Fatal(err)
	}
	if n != want {
		t.Fatalf("%s: the server path enumerated %d code points, want %d", cs, n, want)
	}
	return l
}

// candidates are the byte sequences tried as one character of cs.
func candidates(cs string) [][]byte {
	var out [][]byte
	for b := 0; b < 256; b++ {
		out = append(out, []byte{byte(b)})
	}
	switch cs {
	case "sjis":
		for lead := 0x81; lead <= 0xFE; lead++ {
			for trail := 0x40; trail <= 0xFE; trail++ {
				out = append(out, []byte{byte(lead), byte(trail)})
			}
		}
	case "ucs2":
		out = out[:0]
		for u := 0; u <= 0xFFFF; u++ {
			out = append(out, []byte{byte(u >> 8), byte(u)})
		}
	case "utf8mb4", "utf8mb3", "utf16", "utf16le", "utf32":
		out = out[:0]
		for _, r := range samplePoints(cs == "utf8mb3") {
			out = append(out, encode(cs, r))
		}
	}
	return out
}

// samplePoints is the session sample of a Unicode set: ASCII, Latin-1, every 0x400th BMP
// point, U+FFFD and supplementary points, surrogates excluded.
func samplePoints(bmp bool) []rune {
	var out []rune
	add := func(r rune) {
		if (r < 0xD800 || r > 0xDFFF) && (!bmp || r <= 0xFFFF) {
			out = append(out, r)
		}
	}
	for r := rune(0); r <= 0xFF; r++ {
		add(r)
	}
	for r := rune(0x100); r <= 0xFFFF; r += 0x400 {
		add(r)
	}
	for _, r := range []rune{0xFFFD, 0x10000, 0x1F600, 0x2A6D6, 0x10FFFF} {
		add(r)
	}
	return out
}

func encode(cs string, r rune) []byte {
	switch cs {
	case "utf16", "utf16le":
		order := binary.AppendByteOrder(binary.BigEndian)
		if cs == "utf16le" {
			order = binary.LittleEndian
		}
		var b []byte
		for _, u := range utf16.Encode([]rune{r}) {
			b = order.AppendUint16(b, u)
		}
		return b
	case "utf32":
		return binary.BigEndian.AppendUint32(nil, uint32(r))
	}
	return []byte(string(r))
}

// TestMySQLCharsetRoundTrip (CHS-1): supportedCharsets is exactly the sets whose every
// character survives GoArchive's session. A pair (R56): side A requires every supported set
// to lose nothing, read through a pool built by BuildDSN and bound back as GoArchive binds a
// key, and the Unicode sets in full on the server too; side B requires the same measure to
// see a known loss of each kind, so a test that sees nothing cannot pass side A: cp1250 reads
// a code as '?', sjis reads 0x5C and 0x815F as one, tis620 reads a code as U+FFFD, and ucs2
// reads a lone surrogate as bytes that are not UTF-8, which MySQL refuses to bind back.
func TestMySQLCharsetRoundTrip(t *testing.T) {
	schema := schemaOf(t, scratch(t, "TEST_SOURCE", "chsrt"))
	port, _ := strconv.Atoi(os.Getenv("TEST_SOURCE_PORT"))
	db, err := sql.Open("mysql", database.BuildDSN(&config.DatabaseConfig{Host: os.Getenv("TEST_SOURCE_HOST"), Port: port,
		User: os.Getenv("TEST_SOURCE_USER"), Password: os.Getenv("TEST_SOURCE_PASSWORD"), Database: schema, TLS: "disable"}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	start := time.Now()

	t.Run("side A", func(t *testing.T) {
		for _, cs := range supportedCharsets {
			began := time.Now()
			cands := candidates(cs)
			l := sessionRoundTrip(t, db, cs, cands, true)
			if !l.zero() {
				t.Errorf("%s through the session: %s", cs, l)
			}
			// Every valid character is measured: a server that stopped storing some would shrink
			// the set unseen. ascii's bytes 0x80-0xFF are not storable (1366).
			want := len(cands)
			if cs == "ascii" {
				want = 128
			}
			if l.stored != want {
				t.Errorf("%s: %d of %d candidates stored as one character, want %d", cs, l.stored, len(cands), want)
			}
			switch cs {
			case "utf8mb4", "utf8mb3", "utf16", "utf16le", "utf32":
				if l := serverRoundTrip(t, db, cs); !l.zero() {
					t.Errorf("%s on the server: %s", cs, l)
				}
			}
			t.Logf("%s: %v", cs, time.Since(began).Round(time.Millisecond))
		}
	})

	t.Run("side B", func(t *testing.T) {
		if l := sessionRoundTrip(t, db, "cp1250", candidates("cp1250"), false); l.question == 0 {
			t.Errorf("cp1250: no code read as '?' (%s); the measure cannot see a loss", l)
		}
		if l := sessionRoundTrip(t, db, "sjis", candidates("sjis"), false); !l.merges["5C=815F"] {
			t.Errorf("sjis: 0x5C and 0x815F not read as one (%s); the measure cannot see a merge", l)
		}
		if l := sessionRoundTrip(t, db, "tis620", candidates("tis620"), false); l.replacement == 0 {
			t.Errorf("tis620: no code read as U+FFFD (%s)", l)
		}
		if l := sessionRoundTrip(t, db, "ucs2", candidates("ucs2"), false); !l.notUTF8["D800"] {
			t.Errorf("ucs2: no lone surrogate read as bytes that are not UTF-8 (%s)", l)
		}
	})
	t.Logf("total %v", time.Since(start).Round(time.Millisecond))
}
