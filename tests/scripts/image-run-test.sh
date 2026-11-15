#!/usr/bin/env bash
# The image's run test (IMG-1, beta.3): the built image runs as UID 65532 by default, starts as
# an arbitrary user, reads the default config path archiver.yaml from its working directory
# /work, and that user can write the working directory, and a run with control enabled and no
# socket_dir is refused, naming control.socket_dir. Run by .github/workflows/docker.yml on a
# PR, and locally against a docker build:
#
#   tests/scripts/image-run-test.sh <image> <version>
#
# Exit 0 when every check passes; 1 naming the first that fails; 2 on bad arguments.
set -uo pipefail
[[ $# -eq 2 ]] || { echo "usage: image-run-test.sh <image> <version>" >&2; exit 2; }
image=$1 version=$2
tmp=${RUNNER_TEMP:-$(mktemp -d)}
fail() { echo "image-run-test: $*"; exit 1; }

# Standard output only: Docker prints a platform warning on standard error when the image's platform is
# not the host's, and that is not the uid. Standard error is kept for the failure message.
uid=$(docker run --rm --entrypoint /bin/sh "$image" -c 'id -u' 2> "$tmp/uid.err")
[[ $uid == 65532 ]] || fail "the default user is '$uid', want 65532 (docker said: $(tr '\n' ' ' < "$tmp/uid.err"))"

out=$(docker run --rm --user 12345:12345 "$image" --version 2>&1)
rc=$?
echo "$out"
[[ $rc -eq 0 ]] || fail "--version as uid 12345 exited $rc"
grep -qF "$version" <<<"$out" || fail "--version does not print $version"

# The default config path, archiver.yaml in the working directory, mounted at /work.
cfg="$tmp/archiver.yaml"
printf 'x: 1\n' > "$cfg" && chmod 0644 "$cfg"
docker run --rm --user 12345:12345 -v "$cfg:/work/archiver.yaml:ro" "$image" validate > "$tmp/run.txt" 2>&1
rc=$?
cat "$tmp/run.txt"
if grep -qi 'permission denied\|no such file' "$tmp/run.txt"; then fail "an access error reading archiver.yaml"; fi
[[ $rc -eq 2 ]] || fail "validate exited $rc, want 2 (usage)"
grep -qF 'x: unknown key (line 1)' "$tmp/run.txt" || fail "archiver.yaml was not read from /work"

docker run --rm --user 12345:12345 --entrypoint /bin/sh "$image" -c 'touch ./write-probe && rm ./write-probe' \
    || fail "uid 12345 cannot write the working directory"
# Control enabled and no socket_dir (TEL-1 C7). The image's user has no home directory, so there is no
# default socket directory: the run is refused before it connects anywhere (no MySQL is needed), with
# the message that names the remedy. The default user (65532) and an arbitrary one both have no entry.
cfg2="$tmp/archiver-control.yaml"
cat > "$cfg2" <<'YAML'
source: {host: src.invalid, user: u, password: p, database: s}
destination: {host: dst.invalid, user: u, password: p, database: d}
control: {enabled: true}
jobs:
  j:
    job_type: archive
    root_table: orders
    primary_key: id
    where: "1=1"
YAML
chmod 0644 "$cfg2"
for user in "" "--user 12345:12345"; do
    # shellcheck disable=SC2086
    docker run --rm $user -v "$cfg2:/work/archiver.yaml:ro" "$image" archive --job j > "$tmp/control.txt" 2>&1
    rc=$?
    cat "$tmp/control.txt"
    [[ $rc -eq 1 ]] || fail "archive with control enabled and no socket_dir (${user:-default user}) exited $rc, want 1"
    grep -qF 'control.socket_dir' "$tmp/control.txt" || fail "the refusal (${user:-default user}) does not name control.socket_dir"
done
echo "image-run-test: OK"
