#!/usr/bin/env bash
# The environment matrix (M7-8, M7c-2): the integration suites under one changed MySQL
# environment at a time, each row judged against its expected outcome (rows.tsv).
#
#   tests/envmatrix/run-matrix.sh [row ...]      # default: every row of rows.tsv
#
# MX_PAIRS throwaway source/destination pairs work through a shared queue. A dynamic row
# snapshots the globals in VARS on both servers of a long-lived MX_IMAGE pair, applies its
# SET GLOBAL statements, runs the suites, restores the snapshot and compares it; a pair
# whose restore differs is recreated. Init and version rows run on a fresh pair, removed
# after the row. A group row runs the unfinished-work guard tests with dbsgomysql's Group
# Replication cluster (group/) as the source and a fresh server as the destination. TestMySQLLiveReplicationGate is skipped: it changes the estate's replica
# db3, which every pair shares and no row mutates; it runs on the estate only.
#
# Environment: MX_IMAGE (percona/percona-server:8.4), MX_PAIRS (3), MX_TIMEOUT (3m, per
# test binary), MX_PKGS (./internal/... ./cmd/...), MX_TAG (a timestamp), MX_CODE (this
# checkout). Results: tests/results/envmatrix/<tag>/<row>/ and <tag>/SUMMARY.txt. Exit 1
# when any row is not OK.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
CODE="${MX_CODE:-$REPO}"
IMAGE="${MX_IMAGE:-percona/percona-server:8.4}"
PAIRS="${MX_PAIRS:-3}"
TIMEOUT="${MX_TIMEOUT:-3m}"
PKGS="${MX_PKGS:-./internal/... ./cmd/...}"
TAG="${MX_TAG:-$(date +%Y%m%d-%H%M%S)}"
VARS="sql_mode sql_select_limit autocommit transaction_isolation character_set_server collation_server explicit_defaults_for_timestamp max_allowed_packet max_error_count sql_notes time_zone partial_revokes wait_timeout interactive_timeout default_storage_engine sql_require_primary_key sql_generate_invisible_primary_key gtid_mode"

set -a
# shellcheck source=/dev/null
source "$REPO/tests/.env"
set +a
# TestMySQLLiveReplicationGate, TestMySQLStopDuringHold and TestMySQLStopNeverCutsAPass drive
# the estate replica (db3) through the live gate, and the first two stop its applier: they run on
# the estate only.
# MX_SKIP_FILE (the release-verify workflow: envmatrix/gh-skip.txt) adds the tests a GitHub
# matrix job cannot run, one name per line with its reason after '#'.
SKIP="TestMySQLLiveReplicationGate|TestMySQLStopDuringHold|TestMySQLStopNeverCutsAPass"
if [[ -n ${MX_SKIP_FILE:-} ]]; then
    while read -r name _; do
        [[ -z $name || $name == \#* ]] || SKIP+="|$name"
    done <"$MX_SKIP_FILE"
fi

# Each pair's source is loaded with Sakila, which is downloaded, not committed.
[[ -f $REPO/tests/sakila-db/sakila-schema.sql ]] || bash "$REPO/tests/scripts/get_sakila_db.sh" || exit 1
OUT="$REPO/tests/results/envmatrix/$TAG"
WORK="$OUT/.work"
mkdir -p "$WORK/queue" "$WORK/claimed"
ln -sfn "$TAG" "$REPO/tests/results/envmatrix/latest"

sqlc() { # container sql → tab-separated rows
    MYSQL_PWD=$MYSQL_ROOT_PASSWORD docker exec -e MYSQL_PWD "$1" mysql -uroot -N -B -e "$2" </dev/null
}

# rows.tsv without comments, as name<TAB>kind<TAB>setup<TAB>expect.
rows() { grep -v '^#' "$HERE/rows.tsv" | grep -v '^[[:space:]]*$'; }

pair_down() { docker rm -f -v "ga-mx$1-src" "ga-mx$1-dst" >/dev/null 2>&1; }

pair_up() { # pair image [extra-cnf] → containers ga-mx<i>-src/dst on 3375+10i / 3376+10i
    local i=$1 image=$2 extra=${3:-} side port
    pair_down "$i"
    for side in src dst; do
        local mounts=() dir
        for dir in /etc/my.cnf.d /etc/mysql/conf.d; do
            mounts+=(-v "$HERE/cnf/$side.cnf:$dir/goarchive-matrix.cnf:ro")
            [[ -n $extra ]] && mounts+=(-v "$HERE/$extra:$dir/zz-row.cnf:ro")
        done
        port=$((3375 + 10 * i)); [[ $side == dst ]] && port=$((3376 + 10 * i))
        docker run -d --name "ga-mx$i-$side" --cap-add SYS_NICE --memory 1g -p "$port:3306" \
            -e MYSQL_ROOT_PASSWORD "${mounts[@]}" "$image" >/dev/null || return 1
    done
    for port in $((3375 + 10 * i)) $((3376 + 10 * i)); do
        local ok=
        for _ in $(seq 1 150); do
            "$REPO/tests/scripts/mysql-query.sh" "$port" "SELECT 1" >/dev/null 2>&1 && { ok=1; break; }
            docker inspect -f '{{.State.Running}}' "ga-mx$i-src" "ga-mx$i-dst" 2>/dev/null | grep -q false && break
            sleep 2
        done
        if [[ -z $ok ]]; then
            echo "pair $i: port $port did not start"
            for side in src dst; do echo "== ga-mx$i-$side"; docker logs "ga-mx$i-$side" 2>&1 | grep -E 'ERROR|\[Warning\].*(unknown|deprecated)' | tail -5; done
            return 1
        fi
    done
    # The source holds Sakila (internal/key, internal/database); the destination an empty
    # sakila_archive, as on the estate.
    MYSQL_PWD=$MYSQL_ROOT_PASSWORD docker exec -i -e MYSQL_PWD "ga-mx$i-src" mysql -uroot <"$REPO/tests/sakila-db/sakila-schema.sql" &&
        MYSQL_PWD=$MYSQL_ROOT_PASSWORD docker exec -i -e MYSQL_PWD "ga-mx$i-src" mysql -uroot <"$REPO/tests/sakila-db/sakila-data.sql" &&
        sqlc "ga-mx$i-dst" "CREATE DATABASE IF NOT EXISTS sakila_archive"
}

snapshot() { # container → name<TAB>value per variable in VARS
    local q="" v
    for v in $VARS; do q+="SELECT '$v', @@GLOBAL.$v UNION ALL "; done
    sqlc "$1" "${q% UNION ALL }"
}

restore_sql() { # snapshot file → SET GLOBAL statements
    local v val
    while IFS=$'\t' read -r v val; do
        if [[ $val =~ ^[0-9]+$ ]]; then echo "SET GLOBAL $v = $val;"; else echo "SET GLOBAL $v = '$val';"; fi
    done <"$1"
}

run_suite() { # pair dir [run] → go-test.json, go-test.stderr; run limits the tests (go test -run)
    local i=$1 dir=$2 run=${3:-} pkgs only=()
    read -ra pkgs <<<"$PKGS"
    [[ -n $run ]] && only=(-run "$run")
    (cd "$CODE" && TEST_SOURCE_PORT=$((3375 + 10 * i)) TEST_DEST_PORT=$((3376 + 10 * i)) \
        go test -tags=integration -json "${pkgs[@]}" -count=1 -timeout "$TIMEOUT" ${only[@]+"${only[@]}"} \
        -skip "$SKIP") >"$dir/go-test.json" 2>"$dir/go-test.stderr" </dev/null
}

# group_up: dbsgomysql's Group Replication cluster (group/README.md) with gr1 as pair i's
# source, on 3375+10i, and a plain destination of the same image on 3376+10i. Only the
# members, the bootstrap and gr-ext (the bootstrap's dependency) start; the cluster's other
# servers take the next ports of the pair's block of ten.
group_up() { # pair image
    local i=$1 image=$2 b=$((3375 + 10 * i)) ok= dir mounts=()
    group_down "$i"
    GR_IMAGE=$image GR_PORT_1=$b GR_PORT_2=$((b + 2)) GR_PORT_3=$((b + 3)) GR_PORT_INSTALLED=$((b + 4)) \
        GR_PORT_DISABLED=$((b + 5)) GR_PORT_EXT=$((b + 6)) \
        docker compose -p "ga-mxg$i" -f "$HERE/group/compose_group_replication.yaml" up -d --wait --wait-timeout 300 \
        gr1 gr2 gr3 gr-bootstrap || return 1
    for dir in /etc/my.cnf.d /etc/mysql/conf.d; do mounts+=(-v "$HERE/cnf/dst.cnf:$dir/goarchive-matrix.cnf:ro"); done
    docker run -d --name "ga-mx$i-dst" --cap-add SYS_NICE --memory 1g -p "$((b + 1)):3306" \
        -e MYSQL_ROOT_PASSWORD "${mounts[@]}" "$image" >/dev/null || return 1
    for _ in $(seq 1 150); do
        "$REPO/tests/scripts/mysql-query.sh" "$((b + 1))" "SELECT 1" >/dev/null 2>&1 && { ok=1; break; }
        sleep 2
    done
    [[ -n $ok ]] && sqlc "ga-mx$i-dst" "CREATE DATABASE IF NOT EXISTS sakila_archive"
}

group_down() { # pair
    GR_IMAGE=unused docker compose -p "ga-mxg$1" -f "$HERE/group/compose_group_replication.yaml" down -v >/dev/null 2>&1
    pair_down "$1"
}

run_row() { # pair name kind setup expect
    local i=$1 name=$2 kind=$3 setup=$4 expect=$5 dir="$OUT/$2" c t0=$SECONDS note="" rc=-1
    rm -rf "$dir"; mkdir -p "$dir"
    case $kind in
    dynamic)
        for c in src dst; do snapshot "ga-mx$i-$c" >"$dir/before-$c.tsv"; done
        if [[ $setup != "-" ]]; then
            for c in src dst; do
                sqlc "ga-mx$i-$c" "$setup" 2>>"$dir/apply.err" || note+="apply failed on $c; "
            done
        fi
        for c in src dst; do snapshot "ga-mx$i-$c" >"$dir/during-$c.tsv"; done
        run_suite "$i" "$dir"; rc=$?
        for c in src dst; do
            sqlc "ga-mx$i-$c" "$(restore_sql "$dir/before-$c.tsv")" 2>>"$dir/restore.err"
            snapshot "ga-mx$i-$c" >"$dir/after-$c.tsv"
            cmp -s "$dir/before-$c.tsv" "$dir/after-$c.tsv" || { note+="restore differs on $c; "; touch "$WORK/dirty$i"; }
        done
        ;;
    init | version)
        local image=$IMAGE extra=""
        if [[ $kind == init ]]; then extra=$setup; else image=$setup; fi
        if ! pair_up "$i" "$image" "$extra" >"$dir/pair.log" 2>&1; then
            note+="pair did not start (pair.log); "
        else
            run_suite "$i" "$dir"; rc=$?
        fi
        pair_down "$i"; touch "$WORK/dirty$i"
        ;;
    group)
        pair_down "$i"
        if ! group_up "$i" "$setup" >"$dir/group.log" 2>&1; then
            note+="group did not start (group.log); "
        else
            TEST_SOURCE_USER=root TEST_SOURCE_PASSWORD=root run_suite "$i" "$dir" 'TestMySQLGuard|TestMySQLBlockers|TestMySQLConcurrency'; rc=$?
        fi
        group_down "$i"; touch "$WORK/dirty$i"
        ;;
    *) note+="unknown kind $kind; " ;;
    esac
    # go test's exit status is reconciled with its stream (Codex M7c-2 review B2): a killed
    # or crashed run fails the row however its stream ends.
    local verdict judged
    verdict=$("$WORK/judge" -expect "$expect" -json "$dir/go-test.json" -exit "$rc" 2>&1)
    judged=$?
    [[ -n $note ]] && { judged=1; verdict="${verdict/verdict: OK/verdict: FAIL}"$'\n'"  runner: $note"; }
    {
        echo "row: $name  kind: $kind  setup: $setup"
        echo "image: $([[ $kind == version || $kind == group ]] && echo "$setup" || echo "$IMAGE")  pair: $i  code: $(git -C "$CODE" rev-parse --short HEAD)  tree: $TREE  go test exit: $rc  seconds: $((SECONDS - t0))"
        echo "$verdict"
    } >"$dir/verdict.txt"
    [[ $judged -eq 0 ]] || touch "$WORK/failed-$name"
    echo "[$(date +%H:%M:%S)] pair $i: $name → $(sed -n 3p "$dir/verdict.txt")"
}

worker() { # pair index: claims rows from the queue until it is empty
    local i=$1 up="" f name kind setup expect
    for f in "$WORK"/queue/*; do
        [[ -e $f ]] || continue
        mv "$f" "$WORK/claimed/" 2>/dev/null || continue # another worker took it
        IFS=$'\t' read -r name kind setup expect <"$WORK/claimed/$(basename "$f")"
        if [[ $kind == dynamic ]] && { [[ -z $up ]] || [[ -e $WORK/dirty$i ]]; }; then
            rm -f "$WORK/dirty$i"
            if ! pair_up "$i" "$IMAGE" >"$WORK/pair$i.log" 2>&1; then
                echo "pair $i did not start ($WORK/pair$i.log)"; touch "$WORK/failed-$name"; continue
            fi
            up=1
        fi
        run_row "$i" "$name" "$kind" "$setup" "$expect"
    done
    pair_down "$i"
}

go build -o "$WORK/judge" "$REPO/tests/envmatrix/judge" || exit 2
# The tree the suites run from, recorded in every verdict: a result names its code only if
# the tree was clean (Fable M7c-2 review F6).
dirty=$(git -C "$CODE" status --porcelain | wc -l | tr -d ' ')
TREE=clean
[[ $dirty -eq 0 ]] || TREE="dirty ($dirty paths)"
echo "code: $(git -C "$CODE" rev-parse --short HEAD), tree $TREE"
n=0
while IFS= read -r line; do
    name=${line%%$'\t'*}
    if [[ $# -gt 0 ]] && [[ " $* " != *" $name "* ]]; then continue; fi
    printf '%s\n' "$line" >"$WORK/queue/$(printf '%03d' $n)-$name"
    n=$((n + 1))
done < <(rows)
[[ $n -gt 0 ]] || { echo "no row matches: $*"; exit 2; }
for want in "$@"; do rows | cut -f1 | grep -qx "$want" || { echo "unknown row: $want"; exit 2; }; done

# group_down also removes the pair's containers, and tolerates a project that never started. A
# signal that kills the workers mid-row skips their own teardown, so the exit does it (Codex N2).
trap 'for i in $(seq 0 $((PAIRS - 1))); do group_down "$i"; done' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP
echo "environment matrix: $n row(s), $PAIRS pair(s), image $IMAGE, results $OUT"
for i in $(seq 0 $((PAIRS - 1))); do worker "$i" & done
wait

{
    for f in "$WORK"/claimed/*; do
        name=$(cut -f1 "$f")
        printf '%-40s %s\n' "$name" "$(sed -n 3p "$OUT/$name/verdict.txt" 2>/dev/null || echo 'verdict: FAIL (no verdict)')"
    done
} | tee "$OUT/SUMMARY.txt"
ls "$WORK"/failed-* >/dev/null 2>&1 && exit 1
exit 0
