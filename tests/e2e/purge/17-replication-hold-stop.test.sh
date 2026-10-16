# Test17 — a first SIGTERM during a replication hold ends it (LAG-1).
# The helper observes the hold, sends one SIGTERM and requires exit 3 within
# 10 s with the stop line and the stopped closing entry, the source unchanged;
# then it restarts the applier and runs a second purge, which completes with
# nothing to recover. Ordinary exact-count checks still run.
# min_duration is 0.0: the config sleeps nothing.
test_name="Test17_PurgeReplicationHoldStop"
test_desc="A first SIGTERM ends a purge's replication hold; a second purge completes"
config_file="test17_purge_replication_stop.yaml"
tables="payment"
mode="working"
command="purge"
verify_method="none"
min_duration="0.0"
expected_rows="1999"
replication_hold="stop-signal"
