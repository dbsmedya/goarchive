package preflight

import (
	"context"
	"slices"
	"strings"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// checkJSONVerification names the job's JSON columns in one warning when verification is count
// or none (JSN-1). A JSON value reaches the client only as MySQL's printed text, so the copy
// rebuilds it through the destination's JSON parser: a member's type is not kept, a long DECIMAL
// loses digits, and an 8.0 or 8.4 destination changes some doubles (MySQL Bug #116160). sha256
// refuses such a change; count and none cannot see it, and the operator chose them, so the run
// proceeds once it has said which columns are exposed (docs/README_COMPAT.md, JSON values that do
// not survive a text copy). The destination is judged only where its column is JSON and the
// source's same-named column is not: a column only on the destination is never written. The
// check never refuses.
func checkJSONVerification(ctx context.Context, f *facts) error {
	v := f.cfg.Verification
	if v != jobspec.VerifyCount && v != jobspec.VerifyNone {
		return nil
	}
	src, err := f.sourceSpecs(ctx)
	if err != nil {
		return inspectionError("json_verification", err)
	}
	dst, err := f.destSpecs(ctx)
	if err != nil {
		return inspectionError("json_verification", err)
	}
	isJSON := func(c validations.ColumnSpec) bool { return strings.EqualFold(c.NormalizedType, "json") }
	var cols []string
	for _, t := range f.tables {
		for _, c := range src[t].Columns {
			if isJSON(c) {
				cols = append(cols, t+"."+c.Name)
			}
		}
		for _, c := range dst[t].Columns {
			i := slices.IndexFunc(src[t].Columns, func(s validations.ColumnSpec) bool { return strings.EqualFold(s.Name, c.Name) })
			if i >= 0 && isJSON(c) && !isJSON(src[t].Columns[i]) {
				cols = append(cols, "destination:"+t+"."+c.Name)
			}
		}
	}
	if len(cols) == 0 {
		return nil
	}
	seen := "count does not compare values"
	if v == jobspec.VerifyNone {
		seen = "none compares nothing"
	}
	effect := "an archive then deletes the original"
	if ProfileFor(f.cfg.Spec.Type) == ProfileNonDestructive {
		effect = "a copy-only run keeps the changed copy"
	}
	msg := "WARNING: verification: " + string(v) + ": JSON columns " + strings.Join(cols, ", ") +
		": a JSON value is copied as MySQL's printed text, so a member's type is not kept, a DECIMAL can lose digits, " +
		"and an 8.0 or 8.4 destination can change a double (MySQL Bug #116160). " + seen + ", so such a change goes unseen; " +
		effect + ". See docs/README_COMPAT.md → JSON values that do not survive a text copy."
	if f.cfg.Banner != nil {
		f.cfg.Banner(msg)
	} else {
		f.cfg.Logger.Warn(msg)
	}
	return nil
}
