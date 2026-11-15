# GitHub Actions Workflows

This directory contains CI/CD workflows for the project.

## Workflows

### 1. CI (`ci.yml`)
**Triggers:** pushes and pull requests on `main` and `dev/goarchive-3.1`

Runs on each of those, as a **Check** job on the Go release that `go.mod`'s `toolchain`
line pins:
- **`make check`:** format, vet, lint (golangci-lint pinned in the `Makefile`), consumer policy,
  dead code, unit tests with race detection, and build
- **Test version output:** a separate step that builds the binary and checks its version output

A second job, **Logger on Windows**, runs the logger's unit tests on `windows-latest`: a release
ships a Windows binary, where a log file's mode sets only the read-only attribute. Its one test
step, **Logger and control tests**, also runs the control socket's platform tests
(`internal/control`, `internal/controlpath`): the file lock, socket identity, the path limit, the
home lookup and how a connect and a close behave on Windows. It runs with `-v`, so that what the
probes saw is in the job's log. A second step, **Control client tests**, runs the `goarchive control`
client's tests (`go test ./cmd/goarchive/cmd/ -run TestControl`): its exchange and exit codes against a
real server and test listeners. It is a step of its own because a PowerShell step's exit code is that of
its last command, so a second `go test` in the first step could hide a failure of the first.

Database tests do not run in CI. The full gate is `make gate`, described in `tests/README.md`.

### 2. Release (`release.yml`)
**Triggers:** Push of tags starting with `v` (e.g., `v1.0.0`)

Automatically builds and releases:
- Linux AMD64 binary
- Linux ARM64 binary  
- macOS AMD64 binary
- macOS ARM64 binary
- Windows AMD64 binary
- SHA256 checksums

### 3. Docker (`docker.yml`)
**Triggers:** pushes to `main` and `v*` tags, and pull requests into `main`. A push to
`dev/goarchive-3.1` starts no Docker run, so it publishes no image.

Builds and pushes Docker images to GitHub Container Registry:
- Tags: `main`, `v1.0.0`, `1.0`, `sha-abc123`
- Multi-platform: linux/amd64, linux/arm64
- On a pull request, also built for linux/amd64 and run as an arbitrary user
  (`tests/scripts/image-run-test.sh`): the default user 65532, `--version`, the default config path
  read from `/work`, and a write to `/work` (IMG-1)

### 4. Matrix verify (`matrix-verify.yml`)
**Triggers:** a pull request into `main` (the dev→main PR), on its head SHA, and each new push to
that PR, which cancels the earlier run. For an ad hoc run, a push to a `verify/**` branch or
`workflow_dispatch`. Never a merge or a tag.

The environment matrix on one commit (`tests/README.md` → *The full matrix runs on GitHub*):
- **Matrix:** one job per row of `tests/envmatrix/rows.tsv`, at most 14 at once.
- **Summary:** one table of every row's verdict (`tests/envmatrix/gh-summary.sh`), on the run page
  and as the `summary-matrix` artifact. It is red when any job did not succeed, when any row is
  not OK or is missing, and when the run holds a gate.

```bash
git push origin <sha>:refs/heads/verify/<sha7>     # ad hoc run
git push origin --delete verify/<sha7>             # remove the branch after the run
```

### 5. Gate verify (`gate-verify.yml`)
**Triggers:** a pull request into `main` (the dev→main PR), on its head SHA, and each new push to
that PR, which cancels the earlier run. For an ad hoc run, a push to a `verify-gate/**` branch or
`workflow_dispatch`. Never a merge or a tag.

The gate on every supported image (`tests/README.md` → *The full matrix runs on GitHub*):
- **Integration and E2E:** the gate on an estate built from each of `mysql` 8.0.40, 8.4.11 and
  9.7.1 and `percona/percona-server` 8.0.46-37, 8.4.11-11 and 9.7.1-1, pinned (`GA_ESTATE_IMAGE`),
  as two jobs per image, all twelve at once (RVG-3): `run-gate.sh --part integration` (estate,
  static checks, unit, integration) and `--part e2e` (estate, `e2e`, `e2e-examples`). The e2e job
  installs `netcat-openbsd` and `socat` first: E2E 20 sends the control socket's documented lines
  with `nc -U` and `socat`, and fails when a tool is missing.
- **Summary:** one table of both parts of every gate (`tests/envmatrix/gh-summary.sh`), on the
  run page and as the `summary-gate` artifact. It is red when any job did not succeed, when any
  image lacks a part or a part is not certified (one run record for the target SHA, on a clean
  tree, `PASS` with both exits 0, naming its part, with its completion marker and summary), and
  when the run holds a matrix row.

```bash
git push origin <sha>:refs/heads/verify-gate/<sha7>     # ad hoc run
git push origin --delete verify-gate/<sha7>             # remove the branch after the run
```

**Concurrency.** The runs of a pull request form one group by its number, so each new push cancels
the earlier run of the same workflow. A push or a dispatch forms one group by its SHA, so a newer
run on the same SHA cancels the older one. Matrix verify and Gate verify have separate groups, so
their runs on one SHA do not cancel each other. `verify/**` matches only names under `verify/`, and
`verify-gate/…` is not under it, so each push starts one workflow. A push runs the workflow file of
the pushed commit, so a commit that predates a workflow starts no run of it.

## How to Release a New Version

### Option 1: Using Git Tags (Recommended)

```bash
# 1. Update VERSION in Makefile (optional, for documentation)
# VERSION is extracted from git tag during CI build

# 2. Commit any final changes
git add .
git commit -m "Prepare for v1.0.0 release"

# 3. Create and push a tag
git tag -a v1.0.0 -m "Release version 1.0.0"
git push origin v1.0.0
```

The Release workflow will automatically:
- Build binaries for all platforms
- Create a GitHub Release
- Attach binaries and checksums
- Generate release notes

### Option 2: Manual Build

```bash
# Build locally with specific version
make build VERSION=1.0.0

# Or build release binaries for all platforms
make release VERSION=1.0.0
```

## Required Secrets

No additional secrets required. Workflows use:
- `GITHUB_TOKEN` - Automatically provided by GitHub

## Docker Images

After pushing a tag, Docker images are available at:

```
ghcr.io/dbsmedya/goarchive:v1.0.0
ghcr.io/dbsmedya/goarchive:1.0
```

Run with:
```bash
docker run --rm ghcr.io/dbsmedya/goarchive:v1.0.0 version
```
