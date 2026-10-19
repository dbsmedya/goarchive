//go:build integration

package cmd

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/engine"
)

// pageJobs archives old documents with their pages.
const pageJobs = `
  archive_docs:
    job_type: archive
    root_table: docs
    primary_key: id
    where: "placed < '2020-01-01'"
    relations:
      - table: pages
        primary_key: [doc_id, page_no]
        foreign_key: doc_id
`

// TestMySQLArchiveRowsWiderThanThePacket (M7c-F1): a row, and so an INSERT, may be larger
// than the destination's max_allowed_packet; only each value must fit it. Every page has two
// values of three fifths of the destination's packet (at most 2.5 MiB), so a page is 1.2
// packets and each INSERT (batch_size 2 rows) 2.4. Environment pair (R56): on the estate
// (64 MiB) the values are 2.5 MiB and everything fits either way; under the matrix row
// max_allowed_packet_4m the server dropped the connection over the INSERT while the driver
// assumed its own 64 MiB and sent the values inline.
func TestMySQLArchiveRowsWiderThanThePacket(t *testing.T) {
	f := newFixture(t, "packet", pageJobs)
	ddl := []string{
		"CREATE TABLE docs (id INT NOT NULL, placed DATE NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB",
		"CREATE TABLE pages (doc_id INT NOT NULL, page_no INT NOT NULL, body LONGBLOB, scan LONGBLOB, " +
			"PRIMARY KEY (doc_id, page_no), CONSTRAINT fk_pages_doc FOREIGN KEY (doc_id) REFERENCES docs (id)) ENGINE=InnoDB",
	}
	mustExec(t, f.src, ddl...)
	mustExec(t, f.dst, ddl...)
	packet := scalar(t, f.dst, "SELECT @@GLOBAL.max_allowed_packet")
	size := min(5<<19, packet*3/5)
	for doc, placed := range []string{"2019-01-01", "2019-02-01", "2024-01-01"} {
		mustExec(t, f.src, fmt.Sprintf("INSERT INTO docs VALUES (%d, '%s')", doc, placed))
		for page := range 2 {
			mustExec(t, f.src, fmt.Sprintf("INSERT INTO pages VALUES (%d, %d, REPEAT('b', %d), NULL)", doc, page, size),
				fmt.Sprintf("UPDATE pages SET scan = REPEAT('s', %d) WHERE doc_id = %d AND page_no = %d", size, doc, page))
		}
	}

	if out, err := execute(t, "archive", "-c", f.config, "--job", "archive_docs"); err != nil {
		t.Fatalf("archive of %d-byte pages into a %d-byte packet: %v\n%s", 2*size, packet, err, out)
	}
	if got := scalar(t, f.src, "SELECT COUNT(*) FROM pages"); got != 2 {
		t.Errorf("source pages = %d, want the recent document's 2", got)
	}
	if got := scalar(t, f.dst, fmt.Sprintf("SELECT COUNT(*) FROM pages WHERE LENGTH(body) = %d AND LENGTH(scan) = %d", size, size)); got != 4 {
		t.Errorf("destination pages of 2 × %d bytes = %d, want the old documents' 4", size, got)
	}
}

// packetDstRow is the environment-matrix row that lowers the destination's
// max_allowed_packet only (tests/envmatrix/rows.tsv). TestMatrixPacketRowDeclared
// (tests/harness) ties the two, so a rename of either cannot make the test below skip on
// its own row.
const packetDstRow = "max_allowed_packet_dst_4m"

// TestMySQLArchiveValueLargerThanThePacket (PKT-1, M7c-F1): one value larger than the
// destination's max_allowed_packet cannot be sent, so its batch fails with PacketError
// naming the table, column and size, and nothing of it is copied, logged or deleted. Such a
// value exists only where the source's packet is larger than the destination's: the matrix
// row packetDstRow. Environment pair (R56): the test runs there and skips on the estate and
// every other row; on packetDstRow a skip is a failure, so a setup that did not apply cannot
// pass empty.
func TestMySQLArchiveValueLargerThanThePacket(t *testing.T) {
	row := os.Getenv("TEST_MATRIX_ROW")
	srcPacket := scalar(t, serverFor(t, "TEST_SOURCE").open(t, ""), "SELECT @@GLOBAL.max_allowed_packet")
	dstPacket := scalar(t, serverFor(t, "TEST_DEST").open(t, ""), "SELECT @@GLOBAL.max_allowed_packet")
	size := dstPacket + 1<<20
	// The seed's REPEAT must fit the source session: string functions return NULL past it.
	if srcPacket < size+1<<20 {
		why := fmt.Sprintf("row %q: source max_allowed_packet %d, destination %d: no value can exceed the destination's and fit the source's",
			row, srcPacket, dstPacket)
		if row == packetDstRow {
			t.Fatalf("the row's destination-only setup did not apply: %s", why)
		}
		t.Skip(why)
	}
	t.Logf("row %q: source max_allowed_packet %d, destination %d; a value of %d bytes", row, srcPacket, dstPacket, size)

	f := newFixture(t, "packetvalue", pageJobs)
	ddl := []string{
		"CREATE TABLE docs (id INT NOT NULL, placed DATE NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB",
		"CREATE TABLE pages (doc_id INT NOT NULL, page_no INT NOT NULL, body LONGBLOB, scan LONGBLOB, " +
			"PRIMARY KEY (doc_id, page_no), CONSTRAINT fk_pages_doc FOREIGN KEY (doc_id) REFERENCES docs (id)) ENGINE=InnoDB",
	}
	mustExec(t, f.src, ddl...)
	mustExec(t, f.dst, ddl...)
	for doc, placed := range []string{"2019-01-01", "2019-02-01", "2024-01-01"} {
		mustExec(t, f.src, fmt.Sprintf("INSERT INTO docs VALUES (%d, '%s')", doc, placed),
			fmt.Sprintf("INSERT INTO pages VALUES (%d, 0, 'body', NULL)", doc))
	}
	mustExec(t, f.src, fmt.Sprintf("UPDATE pages SET scan = REPEAT('s', %d) WHERE doc_id = 0", size))
	if got := scalar(t, f.src, "SELECT COALESCE(LENGTH(scan), 0) FROM pages WHERE doc_id = 0"); got != size {
		t.Fatalf("seeded scan = %d bytes, want %d", got, size)
	}

	out, err := execute(t, "archive", "-c", f.config, "--job", "archive_docs")
	var pe *engine.PacketError
	if !errors.As(err, &pe) {
		t.Fatalf("archive of a %d-byte value into a %d-byte packet: err = %v, want a PacketError\n%s", size, dstPacket, err, out)
	}
	if pe.Table != "pages" || pe.Column != "scan" || int64(pe.Bytes) != size {
		t.Errorf("PacketError = %+v, want pages.scan of %d bytes", *pe, size)
	}
	for table, want := range map[string]int64{"docs": 3, "pages": 3} {
		if got := scalar(t, f.src, "SELECT COUNT(*) FROM "+table); got != want {
			t.Errorf("source %s = %d, want all %d kept", table, got, want)
		}
		if got := scalar(t, f.dst, "SELECT COUNT(*) FROM "+table); got != 0 {
			t.Errorf("destination %s = %d, want none", table, got)
		}
	}
	id := scalar(t, f.dst, "SELECT id FROM archiver_job WHERE job_name = 'archive_docs'")
	if got := scalar(t, f.dst, fmt.Sprintf("SELECT COUNT(*) FROM archiver_job_log_%d", id)); got != 0 {
		t.Errorf("log rows = %d, want none", got)
	}
}
