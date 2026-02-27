# Installation Guide

This document provides detailed instructions for installing, building, and testing GoArchive.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation from Source](#installation-from-source)
- [Building with Make](#building-with-make)
- [Docker Build](#docker-build)
- [Running Tests](#running-tests)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)

## Prerequisites

### Required

- **Go**: Version 1.21 or later (1.24.0+ recommended)
- **Git**: For cloning the repository
- **MySQL**: Version 8.0+ with InnoDB storage engine (for running the application)

### Optional (for development)

- **Docker & Docker Compose**: For running integration tests
- **Make**: For using the provided Makefile targets
- **golangci-lint**: For code linting
- **govulncheck**: For vulnerability scanning

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
# Simple build
go build -o goarchive ./cmd/goarchive

# Or move to your PATH (optional)
sudo mv goarchive /usr/local/bin/
```

### 4. Verify Installation

```bash
./goarchive version
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

# Build release binaries for all platforms
make release

# Clean build artifacts
make clean
```

### Development Utilities

```bash
# Format Go code
make fmt

# Run linter (requires golangci-lint)
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

- Version: From git tags or `0.1.0-dev`
- Commit: Short git commit hash
- Build Time: UTC timestamp

## Docker Build

### Build Docker Image

```bash
# Build with default version
docker build -t goarchive:latest .

# Build with specific version
docker build --build-arg VERSION=1.0.0 --build-arg COMMIT=abc123 -t goarchive:1.0.0 .
```

### Run with Docker

```bash
# Show help
docker run --rm goarchive:latest

# Run with config file mounted
docker run --rm -v $(pwd)/archiver.yaml:/root/archiver.yaml goarchive:latest -c /root/archiver.yaml plan --job archive_old_orders
```

## Running Tests

### Unit Tests

Unit tests are fast and don't require a database.

```bash
# Run all unit tests
go test -v -short ./...

# Or using Make
make test-unit
```

### Integration Tests

Integration tests require MySQL databases. You can use Docker for local testing.

#### Quick Start with Docker

```bash
# 1. Start test databases
make test-up

# Wait a few seconds for databases to be ready...

# 2. Configure test credentials
export MYSQL_ROOT_PASSWORD=root  # Default password for test containers

# 3. Run integration tests
make test-integration

# 4. Stop test databases when done
make test-down
```

#### Manual Test Database Setup

If you prefer to use your own MySQL instances:

1. **Create the test databases** (source and destination on different ports or hosts)

2. **Configure credentials** by creating `internal/archiver/integration_test.yaml`:

```yaml
databases:
  - name: source
    host: 127.0.0.1
    port: 3306
    user: root
    password: your_password
    database: goarchive_test

  - name: destination
    host: 127.0.0.1
    port: 3307
    user: root
    password: your_password
    database: goarchive_test

force: false  # Set to true to drop/recreate databases
fixture_path: testdata/customer_orders.sql
```

3. **Run integration tests**:

```bash
# Using Make
export MYSQL_ROOT_PASSWORD=your_password
make test-integration

# Or run directly
INTEGRATION_FORCE=true go test -v -run 'TestOrchestrator_.*_Integration' ./internal/archiver/...
```

#### Available Integration Tests

| Test | Description |
|------|-------------|
| `TestOrchestrator_FullArchiveCycle_Integration` | End-to-end archive workflow |
| `TestOrchestrator_CrashRecovery_Integration` | Resume after simulated crash |
| `TestOrchestrator_ReplicationLagPause_Integration` | Lag monitoring behavior |
| `TestOrchestrator_VerificationMismatch_Integration` | Data verification logic |
| `TestOrchestrator_ContextCancellation_Integration` | Graceful shutdown handling |
| `TestOrchestrator_EmptyResultSet_Integration` | Empty result handling |
| `TestOrchestrator_MultiLevelHierarchy_Integration` | 3-level deep relationships |

#### Run Specific Integration Test

```bash
MYSQL_ROOT_PASSWORD=your_password go test -v \
  -run TestOrchestrator_FullArchiveCycle_Integration \
  ./internal/archiver/...
```

#### Force Database Recreation

```bash
INTEGRATION_FORCE=true MYSQL_ROOT_PASSWORD=your_password \
  go test -v -run 'TestOrchestrator_.*_Integration' ./internal/archiver/...
```

### All Tests

```bash
# Run all tests (unit + integration)
make test
```

### End-to-End Tests with Sakila

For comprehensive testing with the Sakila sample database:

```bash
cd tests
./scripts/run-tests.sh --setup --sakila
```

See [tests/README.md](tests/README.md) for detailed testing documentation.

## Configuration

After installation, create a configuration file to define your archive jobs.

### 1. Copy the Example Configuration

```bash
cp configs/archiver.yaml.example archiver.yaml
```

### 2. Edit the Configuration

Update the `archiver.yaml` file with your database credentials and archive jobs. See the [README.md](README.md#configuration) for detailed configuration options.

### 3. Validate Configuration

```bash
./goarchive validate -c archiver.yaml
```

## Troubleshooting

### Common Issues

#### "go: command not found"

Install Go from https://golang.org/dl/ or use your package manager.

#### "cannot find package" during build

```bash
# Ensure dependencies are downloaded
go mod download

# Or tidy up modules
go mod tidy
```

#### Integration tests fail with connection refused

```bash
# Check if test databases are running
make test-status

# Restart test databases
make test-down && make test-up

# Wait 10-15 seconds for MySQL to fully start
```

#### Docker build fails

```bash
# Ensure Docker daemon is running
docker info

# Try building with no cache
docker build --no-cache -t goarchive:latest .
```

#### Permission denied when moving binary

```bash
# Use sudo for system-wide installation
sudo mv goarchive /usr/local/bin/

# Or install to user directory
mv goarchive $HOME/.local/bin/
```

### Getting Help

- Check the [README.md](README.md) for usage instructions
- Review [tests/README.md](tests/README.md) for testing documentation
- Run `./goarchive --help` for command options
