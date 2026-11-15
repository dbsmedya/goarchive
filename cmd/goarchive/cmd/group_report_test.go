package cmd

import (
	"bytes"
	"strings"
	"testing"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
)

// The Group Replication block of spec §4.7, in validate and dry-run (GR C2). The block's text is
// preflight's (TestTheGroupReport); these tests pin where the commands print it and that a plain
// pair gets nothing.

const reportGroup = "8a94f357-0000-4000-8000-0000000000aa"

// memberResult is what preflight returns for an ONLINE primary with a plain destination.
func memberResult() *preflight.Result {
	return &preflight.Result{
		Caps: []preflight.Cap{{Table: "orders", Server: "source", Statement: "by key", Columns: []string{"id"}, Tuples: 16000}},
		Group: preflight.Group{
			Source: dbsrepl.GroupReplication{PluginStatus: "ACTIVE", GroupName: reportGroup, TransactionSizeLimit: 150000000, TransactionSizeLimitKnown: true},
			Members: []dbsrepl.GroupMember{
				{MemberID: "11111111-1111-4111-8111-111111111111", State: "ONLINE", Role: "PRIMARY", Self: true},
			},
		},
	}
}

var reportProcessing = config.ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500}

const memberBlock = "Group Replication:\n" +
	"  source:      member of group " + reportGroup + ", ONLINE, PRIMARY; transaction size limit 150000000 bytes\n" +
	"  destination: Group Replication plugin not active\n" +
	"  sizes:       batch_size 1000, batch_delete_size 500 (each transaction must fit the limit of its side)\n"

func TestPrintGroupReport(t *testing.T) {
	var out bytes.Buffer
	if printGroupReport(&out, &preflight.Result{}, reportProcessing) || out.Len() != 0 {
		t.Errorf("a plain pair printed %q, want nothing", out.String())
	}
	out.Reset()
	if !printGroupReport(&out, memberResult(), reportProcessing) {
		t.Error("a member source printed nothing")
	}
	if out.String() != memberBlock {
		t.Errorf("block:\n got %q\nwant %q", out.String(), memberBlock)
	}
}

// TestValidationReportShowsTheBlockBeforeTheCaps: validate's per-job report (printValidation)
// puts the block under PASSED and ahead of the caps, and prints no block for a plain pair.
func TestValidationReportShowsTheBlockBeforeTheCaps(t *testing.T) {
	var out bytes.Buffer
	printValidation(&out, memberResult(), reportProcessing)
	got := out.String()
	block, caps := strings.Index(got, "Group Replication:"), strings.Index(got, "Statement caps")
	if block < 0 || caps < 0 || block > caps {
		t.Fatalf("report:\n%s\nwant the block, then the caps", got)
	}
	if !strings.HasPrefix(got, memberBlock) {
		t.Errorf("report starts %q, want the block", got[:min(len(got), len(memberBlock))])
	}

	out.Reset()
	printValidation(&out, &preflight.Result{Caps: memberResult().Caps}, reportProcessing)
	if strings.Contains(out.String(), "Group Replication") || !strings.HasPrefix(out.String(), "Statement caps") {
		t.Errorf("a plain pair's report = %q, want the caps alone, as before", out.String())
	}
}

// TestDryRunReportsAGroup: dry-run prints the block ahead of the caps for a member source, and
// nothing about groups for a plain pair.
func TestDryRunReportsAGroup(t *testing.T) {
	var out bytes.Buffer
	ex := dryRunExplanation()
	ex.Preflight = memberResult()
	printExplanation(&out, "j", jobspec.Archive, "c.yaml", dryRunDoc(), dryRunSpec(jobspec.Archive), jobspec.VerifySHA256, ex)
	got := out.String()
	block, caps := strings.Index(got, "Group Replication:\n  source:"), strings.Index(got, "Statement caps")
	if block < 0 || caps < 0 || block > caps {
		t.Errorf("dry-run report lacks the block ahead of the caps:\n%s", got)
	}
	if !strings.Contains(got, "batch_size 1000, batch_delete_size 500 (each transaction") {
		t.Errorf("the block's sizes are not the job's:\n%s", got)
	}

	out.Reset()
	printExplanation(&out, "j", jobspec.Archive, "c.yaml", dryRunDoc(), dryRunSpec(jobspec.Archive), jobspec.VerifySHA256, dryRunExplanation())
	if strings.Contains(out.String(), "Group Replication") {
		t.Errorf("a plain pair's dry-run mentions Group Replication:\n%s", out.String())
	}
}
