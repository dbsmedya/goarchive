# Test13 — a stopped SQL applier must hold the actual purge process.
# The helper observes the hold, checks retained source rows, restores the
# applier and waits for the same process. Ordinary exact-count checks still run.
test_name="Test13_PurgeReplicationHold"
test_desc="Purge holds on a stopped replica and resumes on the same invocation"
config_file="test13_purge_replication.yaml"
tables="payment"
mode="working"
command="purge"
verify_method="none"
min_duration="0.0"
expected_rows="1999"
replication_hold="stopped-applier"
