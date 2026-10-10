package tracking

import "testing"

// TestStatusNumbers pins the stored values. 2.x's pending (0) and failed (3) are not
// reused, so a 3.0 log row can never be read as a 2.x one.
func TestStatusNumbers(t *testing.T) {
	for st, want := range map[Status]int8{StatusCopied: 1, StatusCompleted: 2, StatusDeleting: 4} {
		if int8(st) != want || !st.valid() {
			t.Errorf("%s = %d, want %d", st, st, want)
		}
	}
	for _, n := range []int8{0, 3, 5, -1} {
		if Status(n).valid() {
			t.Errorf("Status(%d) is valid", n)
		}
	}
	if Status(3).String() != "Status(3)" || StatusDeleting.String() != "deleting" {
		t.Error("Status.String")
	}
	for st, want := range map[JobStatus]bool{JobIdle: true, JobRunning: true, JobFailed: true, 2: false} {
		if st.valid() != want {
			t.Errorf("JobStatus(%d).valid() = %v", st, !want)
		}
	}
}
