package jobspec

import (
	"strings"
	"testing"
)

// TestVerificationSetting is R41 and R42: verification is one value, sha256 by default,
// set globally or per job, the job's value winning.
func TestVerificationSetting(t *testing.T) {
	const job = "\njobs:\n  j:\n    job_type: archive\n    root_table: t\n    primary_key: id\n    where: \"1=1\"\n"
	for _, tc := range []struct {
		name, yaml        string
		global, effective Verification
	}{
		{"nothing set", job, VerifySHA256, VerifySHA256},
		{"global count", "\nverification: count" + job, VerifyCount, VerifyCount},
		{"global none", "\nverification: none" + job, VerifyNone, VerifyNone},
		{"job count", job + "    verification: count\n", VerifySHA256, VerifyCount},
		{"job sha256 over a global none", "\nverification: none" + job + "    verification: sha256\n", VerifyNone, VerifySHA256},
		{"an empty job value inherits", "\nverification: count" + job + "    verification:\n", VerifyCount, VerifyCount},
		{"an empty global value is the default", "\nverification:" + job, VerifySHA256, VerifySHA256},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := parse(t, tc.yaml)
			if err != nil {
				t.Fatal(err)
			}
			if d.Verification != tc.global {
				t.Errorf("global = %q, want %q", d.Verification, tc.global)
			}
			if got := d.Jobs["j"].EffectiveVerification(d.Verification); got != tc.effective {
				t.Errorf("effective = %q, want %q", got, tc.effective)
			}
		})
	}
}

// TestVerificationRefusals: a value outside the three, including YAML's false, is
// reported with the other errors; the 2.x block stops the load. An empty value is unset.
func TestVerificationRefusals(t *testing.T) {
	const job = "\njobs:\n  j:\n    job_type: archive\n    root_table: t\n    primary_key: id\n    where: \"1=1\"\n"
	for _, tc := range []struct{ name, yaml, field string }{
		{"global md5", "\nverification: md5" + job, "verification"},
		{"global false", "\nverification: false" + job, "verification"},
		{"job skip", job + "    verification: skip\n", "jobs.j.verification"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			errs := mustFail(t, tc.yaml+"    where_extra_key: 1\n")
			if !hasError(errs, tc.field, "verification must be sha256, count or none") {
				t.Errorf("want %s refused, got:\n%v", tc.field, errs)
			}
			if !hasError(errs, "jobs.j.where_extra_key", "unknown key") {
				t.Errorf("the value was not reported with the other errors:\n%v", errs)
			}
		})
	}
	for _, tc := range []struct{ name, yaml, msg string }{
		{"the 2.x block", "\nverification:\n  method: count\n  skip_verification: true" + job, "replaced in 3.0, see " + UpgradeGuide},
		{"the 2.x block in a job", job + "    verification:\n      method: sha256\n", "replaced in 3.0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			errs := mustFail(t, tc.yaml)
			if len(errs) != 1 || !strings.Contains(errs[0].Message, tc.msg) {
				t.Errorf("want one error containing %q, got:\n%v", tc.msg, errs)
			}
		})
	}
}
