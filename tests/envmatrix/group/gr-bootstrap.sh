#!/bin/sh
# Builds the three-member InnoDB Cluster of compose_group_replication.yaml with
# the MySQL Shell AdminAPI, and prepares gr-ext as an async source.
#
# The topology is disposable: it forms a cluster from fresh servers only. If
# any step fails, the script exits non-zero, the member healthchecks never
# pass, and `up --wait` fails; tear the project down (`down -v`) and start
# again. A restart finds an existing cluster and succeeds only if all three
# members are ONLINE; it never repairs a partial formation.
set -eu

q() { mysql -h"$1" -uroot -proot -N -B -e "$2"; }
state() { q "$1" "SELECT MEMBER_STATE FROM performance_schema.replication_group_members WHERE MEMBER_ID = @@server_uuid" 2>/dev/null || true; }

# The supported range spans both spellings of the GTID reset: renamed in 8.2.
reset_gtids() { q "$1" "RESET BINARY LOGS AND GTIDS" 2>/dev/null || q "$1" "RESET MASTER"; }

for n in gr1 gr2 gr3 gr-ext; do
  tries=0
  until q "$n" "SELECT 1" >/dev/null 2>&1; do
    tries=$((tries + 1))
    if [ "$tries" -ge 90 ]; then echo "gr-bootstrap: $n not accepting connections after 180 s" >&2; exit 1; fi
    sleep 2
  done
done

if [ "$(state gr1)" != "ONLINE" ]; then
  # Drop the GTIDs the image's initialization wrote, so incremental recovery
  # finds no joiner carrying transactions the group lacks.
  for n in gr1 gr2 gr3; do reset_gtids "$n"; done
  mysqlsh --no-wizard --js --uri root:root@gr1:3306 -e "
    for (const h of ['gr1', 'gr2', 'gr3']) {
      dba.configureInstance('root:root@' + h + ':3306', {restart: false});
    }
    var c = dba.createCluster('dbsgomysql', {gtidSetIsComplete: true});
    c.addInstance('root:root@gr2:3306', {recoveryMethod: 'incremental'});
    c.addInstance('root:root@gr3:3306', {recoveryMethod: 'incremental'});
  "

  # gr-ext: an empty GTID history and a replication account written outside
  # the binary log, so a channel from it carries only what a test writes.
  reset_gtids gr-ext
  q gr-ext "SET SQL_LOG_BIN = 0;
            CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED BY 'repl';
            GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';
            SET SQL_LOG_BIN = 1"
fi

for n in gr1 gr2 gr3; do
  s=$(state "$n")
  if [ "$s" != "ONLINE" ]; then
    echo "gr-bootstrap: $n is '${s:-not a member}', not ONLINE; tear down with 'down -v' and start again" >&2
    exit 1
  fi
done

echo "cluster formed: gr1, gr2, gr3 ONLINE; gr-ext prepared"
exec sleep infinity
