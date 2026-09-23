package archiver

// wherePredicate returns the job's where as the body of a WHERE clause. Every
// site that embeds the where uses it, so dry-run and the run parse the
// operator's text identically: it is inserted unmodified between parentheses,
// and an empty where selects every row.
func wherePredicate(where string) string {
	if where == "" {
		return "(1=1)"
	}
	return "(" + where + ")"
}
