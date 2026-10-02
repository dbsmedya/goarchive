package archiver

import (
	"errors"
	"strings"
)

// errEmptyWhere is returned by every site that would embed an empty or
// whitespace-only where. Config validation refuses such a job, so reaching a
// site with one is a broken invariant, not an operator mistake.
var errEmptyWhere = errors.New("job where is empty; config validation should have refused this job")

// wherePredicate returns the job's where as the body of a WHERE clause. Every
// site that embeds the where uses it, so dry-run and the run parse the
// operator's text identically: it is inserted unmodified between parentheses.
// An empty or whitespace-only where is refused rather than selecting every row.
func wherePredicate(where string) (string, error) {
	if strings.TrimSpace(where) == "" {
		return "", errEmptyWhere
	}
	return "(" + where + ")", nil
}
