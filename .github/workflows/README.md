# GitHub Actions Workflows

This directory contains CI/CD workflows for the project.

## Workflows

### 1. CI (`ci.yml`)
**Triggers:** Push to main/master, Pull requests

Runs on every PR and push:
- **Test Job:** Runs unit tests on Go 1.23 and 1.24
- **Lint Job:** Runs golangci-lint
- **Vet Job:** Runs `go vet` for static analysis
- **Format Job:** Checks code formatting with `gofmt`

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
