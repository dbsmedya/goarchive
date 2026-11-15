#!/bin/sh
# Builds the group estate's three-member InnoDB Cluster (tests/compose.group.yml) with the MySQL Shell
# AdminAPI, then idles so `up --wait` has a service that stays up. It starts from dbsgomysql's
# gr-bootstrap.sh (tests/envmatrix/group/, the unchanged library copy) with three differences:
#   - the root password is the estate's MYSQL_ROOT_PASSWORD, not "root", and reaches the Shell as an option
#     and as a dictionary, never inside a URI (a password with a character that a URI reserves would break
#     one);
#   - there is no gr-ext: the estate's replica is attached after seeding, by tests/scripts/setup-replication.sh;
#   - a cluster that already exists is REBOOTED from a complete outage, which is what `make test-down` followed
#     by `make test-up` leaves: every member stopped, every member's data kept, none able to bootstrap.
#
# The topology is disposable: it forms a cluster from fresh servers, or reboots the one on the volumes. If any
# step fails the script exits non-zero, the member health checks never pass and `up --wait` fails; use
# `make test-reset TOPOLOGY=group`.
set -eu

q() { MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -h"$1" -uroot -N -B -e "$2"; }
state() { q "$1" "SELECT MEMBER_STATE FROM performance_schema.replication_group_members WHERE MEMBER_ID = @@server_uuid" 2>/dev/null || true; }

# The supported range spans both spellings of the GTID reset: renamed in 8.2.
reset_gtids() { q "$1" "RESET BINARY LOGS AND GTIDS" 2>/dev/null || q "$1" "RESET MASTER"; }

for n in gr1 gr2 gr3; do
  tries=0
  until q "$n" "SELECT 1" >/dev/null 2>&1; do
    tries=$((tries + 1))
    if [ "$tries" -ge 90 ]; then echo "gr-bootstrap: $n not accepting connections after 180 s" >&2; exit 1; fi
    sleep 2
  done
done

# The connection data of a member, as the AdminAPI takes it: a dictionary, not a URI.
JS_CONN="const c = h => ({user: 'root', password: os.getenv('MYSQL_ROOT_PASSWORD'), host: h, port: 3306});"
# The Shell with no connection tries the local socket and fails in these images, so it starts on gr1.
sh_gr1() { mysqlsh --no-wizard --js --host gr1 --port 3306 --user root --password="$MYSQL_ROOT_PASSWORD" -e "$1"; }

if [ "$(state gr1)" != "ONLINE" ]; then
  if [ "$(q gr1 "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'mysql_innodb_cluster_metadata'")" = "1" ]; then
    echo "gr-bootstrap: the cluster exists on the volumes and is not running: rebooting it"
    sh_gr1 "$JS_CONN
      dba.rebootClusterFromCompleteOutage('goarchive');
    "
  else
    # Drop the GTIDs the image's initialization wrote, so incremental recovery
    # finds no joiner carrying transactions the group lacks.
    for n in gr1 gr2 gr3; do reset_gtids "$n"; done
    sh_gr1 "$JS_CONN
      for (const h of ['gr1', 'gr2', 'gr3']) {
        dba.configureInstance(c(h), {restart: false});
      }
      shell.connect(c('gr1'));
      var cl = dba.createCluster('goarchive', {gtidSetIsComplete: true});
      cl.addInstance(c('gr2'), {recoveryMethod: 'incremental'});
      cl.addInstance(c('gr3'), {recoveryMethod: 'incremental'});
    "
  fi
fi

for n in gr1 gr2 gr3; do
  s=$(state "$n")
  if [ "$s" != "ONLINE" ]; then
    echo "gr-bootstrap: $n is '${s:-not a member}', not ONLINE; use 'make test-reset TOPOLOGY=group'" >&2
    exit 1
  fi
done

echo "cluster formed: gr1, gr2, gr3 ONLINE"
exec sleep infinity
