# GitHub Actions Workflows

This directory contains CI/CD workflows for the project.

## Workflows

### 1. CI (`ci.yml`)
**Triggers:** Push to main/master, Pull requests

Runs on every PR and push, as a **Check** job on the Go release that `go.mod`'s `toolchain`
line pins:
- **`make check`:** format, vet, lint (golangci-lint pinned in the `Makefile`), consumer policy,
  dead code, unit tests with race detection, and build
- **Test version output:** a separate step that builds the binary and checks its version output

A second job, **Logger on Windows**, runs the logger's unit tests on `windows-latest`: a release
ships a Windows binary, where a log file's mode sets only the read-only attribute.

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
**Triggers:** Push to main/master, PRs, tags

Builds and pushes Docker images to GitHub Container Registry:
- Tags: `main`, `v1.0.0`, `1.0`, `sha-abc123`
- Multi-platform: linux/amd64, linux/arm64

### 4. Matrix verify (`matrix-verify.yml`)
**Triggers:** a push to a `verify/**` branch only, plus `workflow_dispatch` once this file is on
the default branch. Never a pull request, a merge or a tag.

The environment matrix on one commit, on every PR's final SHA
(`tests/README.md` → *The full matrix runs on GitHub*):
- **Matrix:** one job per row of `tests/envmatrix/rows.tsv`, at most 14 at once.
- **Summary:** one table of every row's verdict (`tests/envmatrix/gh-summary.sh`), on the run page
  and as the `summary-matrix` artifact. It is red when any job did not succeed, when any row is
  not OK or is missing, and when the run holds a gate.

```bash
git push origin <sha>:refs/heads/verify/<sha7>     # start a run
git push origin --delete verify/<sha7>             # remove the branch after the run
```

### 5. Gate verify (`gate-verify.yml`)
**Triggers:** a push to a `verify-gate/**` branch only, plus `workflow_dispatch` once this file
is on the default branch. Never a pull request, a merge or a tag.

The gate on every supported image, on a release PR's final SHA, or another SHA when the
operator asks:
- **Gate:** `make gate` on an estate built from each of `mysql` 8.0.40, 8.4.11 and 9.7.1 and
  `percona/percona-server` 8.0.46-37, 8.4.11-11 and 9.7.1-1, pinned (`GA_ESTATE_IMAGE`), all six
  at once.
- **Summary:** one table of every gate (`tests/envmatrix/gh-summary.sh`), on the run page and as
  the `summary-gate` artifact. It is red when any job did not succeed, when any gate is missing or
  not certified (one run record for the target SHA, on a clean tree, `PASS` with both exits 0,
  with its completion marker and summary), and when the run holds a matrix row.

```bash
git push origin <sha>:refs/heads/verify-gate/<sha7>     # start a run
git push origin --delete verify-gate/<sha7>             # remove the branch after the run
```

One run per commit and workflow: a newer run on the same SHA cancels the older one, and a Matrix
verify and a Gate verify run on one SHA do not cancel each other. `verify/**` does not match
`verify-gate/…` (`*` stops at `/`), so each push starts one workflow. A push runs the workflow file
of the pushed commit: on a commit from before these two files, `verify/` starts the old
`release-verify.yml` and `verify-gate/` starts nothing.

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
