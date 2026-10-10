package cmd

import (
	"fmt"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/spf13/cobra"
)

// registerVerificationFlag adds --verification, which overrides the job's and the file's
// verification for this run (spec §5, R42). Purge copies nothing and has no such flag.
func registerVerificationFlag(c *cobra.Command, target *string) {
	c.Flags().StringVar(target, "verification", "",
		"Override the job's verification for this run: sha256 (compare every copied row), count (compare which keys landed), or none")
}

// checkVerificationFlag refuses a --verification value that is not one of the three.
func checkVerificationFlag(raw string) error {
	if raw != "" && !jobspec.Verification(raw).Valid() {
		return fmt.Errorf("invalid --verification %q: use sha256, count or none", raw)
	}
	return nil
}

// verificationFor is the job's effective verification: --verification over the job's own
// setting over the file's (spec §5). A purge job has none.
func verificationFor(raw string, doc *jobspec.Document, spec *jobspec.Spec) (jobspec.Verification, error) {
	if err := checkVerificationFlag(raw); err != nil {
		return "", err
	}
	if spec.Type == jobspec.Purge {
		return "", nil
	}
	if raw != "" {
		return jobspec.Verification(raw), nil
	}
	return spec.EffectiveVerification(doc.Verification), nil
}
