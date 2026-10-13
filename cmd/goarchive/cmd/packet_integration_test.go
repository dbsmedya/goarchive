//go:build integration

package cmd

import (
	"fmt"
	"testing"
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
