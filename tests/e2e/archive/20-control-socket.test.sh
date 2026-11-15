# Test20 — an archive driven over its runtime control socket (TEL-1 C7).
# lib/control.sh runs the job three times through the real binary: driven with `goarchive control`
# (status, status-json, pause, resume, -s, -g, stop -> exit 3) and, as a script without the binary
# would, with the doc's `nc -U` and `socat` lines; a second start of the job from a second config file
# exits 4; the rerun is killed with SIGKILL and `control` exits 5 with the stale-socket message; the next
# run replaces the socket, completes and removes it, and `control` then exits 5. The printed path of
# `dry-run` is the log's and the status's. The ordinary assertions read the FINAL run's own output.
# Needs jq, nc (OpenBSD) and socat on the PATH; a missing tool fails the test.
# min_duration is 0.0: the final run is what is left after a stop and a kill, so it has no full-slice floor.
test_name="Test20_ControlSocket"
test_desc="Working archive: payment driven over its control socket, stopped, killed and finished"
config_file="test20_control_socket.yaml"
tables="payment"
mode="working"
command="archive"
verify_method="sha256"
min_duration="0.0"
# The full slice, after all three runs. 499 rows, measured: payment_id is not contiguous.
expected_rows="499"
control_socket="driven"
