# Installation Guide

How to install GoArchive from a release binary, the published image, or source, and how to
build it.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Install a Release Binary](#install-a-release-binary)
- [Run the Published Image](#run-the-published-image)
- [Installation from Source](#installation-from-source)
- [Building with Make](#building-with-make)
- [Docker Build](#docker-build)
- [Running Tests](#running-tests)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)

## Prerequisites

### Required

- **Go** and **MySQL** — supported versions are listed under
  [Environment](docs/README_LIMITATIONS.md#environment), the single source of truth for both
- **Git**: For building from source

### Optional

- **Docker**: For the published image or a local image build
- **Make**: For using the provided Makefile targets
- **govulncheck**: For vulnerability scanning

## Install a Release Binary

Each release on the [Releases page](https://github.com/dbsmedya/goarchive/releases) publishes
binaries named `goarchive-<version>-<os>-<arch>` (linux and darwin for amd64 and arm64, and
windows-amd64 with an `.exe` suffix) and a `checksums.txt`. Its tag is `v<version>`. Replace
`<version>` and the platform below with the release and platform you want:

```bash
# With the GitHub CLI
gh release download v<version> -R dbsmedya/goarchive \
  -p checksums.txt -p 'goarchive-<version>-linux-amd64'

# Or with curl
curl -fsSLO https://github.com/dbsmedya/goarchive/releases/download/v<version>/checksums.txt
curl -fsSLO https://github.com/dbsmedya/goarchive/releases/download/v<version>/goarchive-<version>-linux-amd64

# Verify, then run
shasum -a 256 -c checksums.txt --ignore-missing
chmod +x goarchive-<version>-linux-amd64
./goarchive-<version>-linux-amd64 --version
```

## Run the Published Image

Each release is also published as `ghcr.io/dbsmedya/goarchive:<version>`, tagged with the release
version without the leading `v`. Use a version tag:

```bash
docker run --rm ghcr.io/dbsmedya/goarchive:<version> --version

# With a config file mounted
docker run --rm -v "$(pwd)/archiver.yaml:/root/archiver.yaml" \
  ghcr.io/dbsmedya/goarchive:<version> -c /root/archiver.yaml plan --job archive_old_orders
```

The image runs as root. With the default logging (JSON to stdout), every line goes to `docker logs`
as one JSON object; an error also prints one `Error:` line on stderr. A foreground `docker run`
returns goarchive's exit code (0 done, 1 failed, 2 usage, 3 stopped, 4 another run holds the work);
for a detached container, read the number `docker wait <name>` prints, not `docker wait`'s own
exit status. `docker stop` stops a run gracefully if the batch in flight finishes before Docker's
deadline: set `docker stop --time` (compose: `stop_grace_period`) above one batch's duration. A stop
during a replication hold ends the hold without waiting for the replicas
([Output and exit codes](docs/README_OPERATIONS.md#output-and-exit-codes)).
On a Linux host, a log file written to a mounted directory belongs to the container's root and
is readable by root only ([log files](docs/README_CONFIGURATION.md#logging)). To read it as yourself, create the
file first; GoArchive appends to it and keeps its owner and mode:

```bash
mkdir -p logs && touch logs/goarchive.log && chmod 0640 logs/goarchive.log
# archiver.yaml: logging.output: /logs/goarchive.log
docker run --rm -v "$(pwd)/archiver.yaml:/root/archiver.yaml" -v "$(pwd)/logs:/logs" \
  ghcr.io/dbsmedya/goarchive:<version> -c /root/archiver.yaml archive --job archive_old_orders
```

## Installation from Source

### 1. Clone the Repository

```bash
git clone https://github.com/dbsmedya/goarchive.git
cd goarchive
```

### 2. Install Dependencies

```bash
go mod download
go mod verify
```

### 3. Build the Binary

```bash
go build -o goarchive ./cmd/goarchive

# Optional: put it on your PATH (or move it to $HOME/.local/bin without sudo)
sudo mv goarchive /usr/local/bin/
```

### 4. Verify Installation

```bash
./goarchive --version
```

## Building with Make

The project includes a Makefile with convenient build targets.

### Build Targets

```bash
# Build binary with version info (recommended)
make build

# Quick development build (no version injection)
make dev

# Install to $GOPATH/bin
make install

# Build release binaries for linux and darwin (amd64, arm64)
make release

# Clean build artifacts
make clean
```

### Development Utilities

```bash
# Format Go code
make fmt

# Run golangci-lint (version pinned in the Makefile; no install needed)
make lint

# Check for vulnerabilities (requires govulncheck)
make vulncheck

# Show version info without building
make version

# Show all available targets
make help
```

### Build Output

By default, the binary is built to `bin/goarchive` with version information injected:

- Version: From the git tag, or else the `Makefile`'s `RELEASE_VERSION`
- Commit: Short git commit hash

## Docker Build

To build the image from a checkout instead of using the published one. Without
`--build-arg VERSION`, the binary reports version `dev`:

```bash
docker build --build-arg VERSION=<version> --build-arg COMMIT=$(git rev-parse --short HEAD) \
  -t goarchive:<version> .
docker run --rm goarchive:<version> --version
```

## Running Tests

See [tests/README.md](tests/README.md) for running every test layer.

## Configuration

After installation, create a configuration file to define your archive jobs.

### 1. Copy the Example Configuration

```bash
cp configs/archiver.yaml.example archiver.yaml
```

### 2. Edit the Configuration

Update the `archiver.yaml` file with your database credentials and archive jobs. Every option is
described in [Configuration](docs/README_CONFIGURATION.md).

### 3. Validate Configuration

```bash
./goarchive validate -c archiver.yaml
```

## Troubleshooting

### "go: command not found"

Install Go from https://golang.org/dl/ or use your package manager.

### "cannot find package" during build

Re-run step 2 of [Installation from Source](#2-install-dependencies), or `go mod tidy`.

### Docker build fails

Check that the Docker daemon is running (`docker info`), then retry the build with `--no-cache`.
