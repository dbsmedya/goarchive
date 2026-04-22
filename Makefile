# MySQL Archiver - Build Configuration
# Edit VERSION below when releasing a new version

# Version configuration - EDIT THIS when releasing
# Or use: make build VERSION=1.2.3
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "0.9.0-community")

# Git commit hash (auto-detected, or 'unknown' if not in git repo)
COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# Build time (auto-detected)
BUILD_TIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

# Binary name
BINARY_NAME := goarchive

# Package path for ldflags
PACKAGE_PATH := github.com/dbsmedya/goarchive/cmd/goarchive/cmd

# Linker flags for version injection
LDFLAGS := -X '$(PACKAGE_PATH).Version=$(VERSION)' \
           -X '$(PACKAGE_PATH).Commit=$(COMMIT)'

# Go build flags
GOFLAGS := -trimpath

# Default target
.PHONY: all
all: build

# Build the binary with version info
.PHONY: build
build:
	@echo "Building $(BINARY_NAME) version $(VERSION) (commit: $(COMMIT))..."
	go build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o bin/$(BINARY_NAME) ./cmd/goarchive
	@echo "Build complete: bin/$(BINARY_NAME)"

# Install to $GOPATH/bin
.PHONY: install
install:
	@echo "Installing $(BINARY_NAME) version $(VERSION)..."
	go install $(GOFLAGS) -ldflags "$(LDFLAGS)" ./cmd/goarchive

# Run all tests (excludes integration tests)
.PHONY: test
test:
	go test -v ./...

# Run unit tests only (fast, excludes integration tests)
.PHONY: test-unit
test-unit:
	go test -v -short ./...

# Run tests with race detection (matches CI)
.PHONY: test-ci
test-ci:
	go test -v -short -race ./...

# Clean build artifacts
.PHONY: clean
clean:
	rm -rf bin/
	go clean ./...

# Show version info without building
.PHONY: version
version:
	@echo "Version: $(VERSION)"
	@echo "Commit:  $(COMMIT)"
	@echo "Package: $(PACKAGE_PATH)"

# New Tagging target
.PHONY: tag
tag:
	@if [ -z "$(V)" ]; then echo "Error: V is not set. Use 'make tag V=1.0.0'"; exit 1; fi
	@if [ -n "$$(git status --short)" ]; then echo "Error: Working directory is dirty. Commit first."; exit 1; fi
	git tag -a v$(V) -m "Release version $(V)"
	@echo "Tagged v$(V). Now run 'make release' to build binaries."

# Build for multiple platforms (release builds)
.PHONY: release
release: clean
	@echo "Building release binaries for version $(VERSION)..."
	# Linux AMD64
	GOOS=linux GOARCH=amd64 go build $(GOFLAGS) -ldflags "$(LDFLAGS) -s -w" \
		-o bin/$(BINARY_NAME)-$(VERSION)-linux-amd64 ./cmd/goarchive
	# Linux ARM64
	GOOS=linux GOARCH=arm64 go build $(GOFLAGS) -ldflags "$(LDFLAGS) -s -w" \
		-o bin/$(BINARY_NAME)-$(VERSION)-linux-arm64 ./cmd/goarchive
	# Darwin AMD64
	GOOS=darwin GOARCH=amd64 go build $(GOFLAGS) -ldflags "$(LDFLAGS) -s -w" \
		-o bin/$(BINARY_NAME)-$(VERSION)-darwin-amd64 ./cmd/goarchive
	# Darwin ARM64
	GOOS=darwin GOARCH=arm64 go build $(GOFLAGS) -ldflags "$(LDFLAGS) -s -w" \
		-o bin/$(BINARY_NAME)-$(VERSION)-darwin-arm64 ./cmd/goarchive
	@echo "Release binaries built in bin/"

# Development build (no version injection, faster)
.PHONY: dev
dev:
	go build -o bin/$(BINARY_NAME) ./cmd/goarchive

# Format code (write changes)
.PHONY: fmt
fmt:
	gofmt -w .

# Check formatting (CI style - fails if unformatted)
.PHONY: fmt-check
fmt-check:
	@if [ -n "$$(gofmt -l .)" ]; then \
		echo "The following files are not formatted:"; \
		gofmt -l .; \
		exit 1; \
	fi; \
	echo "All files are properly formatted"

# Run go vet (CI version - basic checks)
.PHONY: vet
vet:
	go vet ./...

# Run go vet with all checks (stricter)
.PHONY: vet-all
vet-all:
	go vet -all ./...

# Run linter (requires golangci-lint)
.PHONY: lint
lint:
	golangci-lint run ./...

# Run all checks (CI-style)
.PHONY: check
check: fmt-check vet test-ci build
	@echo "All checks passed!"

# Full CI pipeline simulation
.PHONY: github-release
github-release: clean check lint
	@echo "Building release binaries..."
	$(MAKE) release
	@echo ""
	@echo "✅ All CI checks passed and release binaries built!"
	@echo "Binaries available in: bin/"

# Check for vulnerabilities (requires govulncheck)
.PHONY: vulncheck
vulncheck:
	govulncheck ./...

# Integration test configuration
INTEGRATION_CONFIG_DIR := internal/archiver
INTEGRATION_CONFIG_TEMPLATE := $(INTEGRATION_CONFIG_DIR)/integration_test.yaml

# Create integration test configuration interactively
.PHONY: integration-config
integration-config:
	@echo "Setting up integration test configuration..."
	@echo ""
	@if [ -f "$(INTEGRATION_CONFIG_DIR)/integration_test.yaml" ]; then \
		echo "Configuration file already exists: $(INTEGRATION_CONFIG_DIR)/integration_test.yaml"; \
		echo "Edit this file to update your credentials."; \
		echo ""; \
	else \
		cp $(INTEGRATION_CONFIG_TEMPLATE) $(INTEGRATION_CONFIG_DIR)/integration_test.yaml; \
		echo "Created: $(INTEGRATION_CONFIG_DIR)/integration_test.yaml"; \
		echo ""; \
	fi
	@echo "Please edit the configuration file and set your database credentials."
	@echo ""
	@echo "Example configuration:"
	@echo "  Source:      127.0.0.1:3305 (requires Docker: make test-up)"
	@echo "  Destination: 127.0.0.1:3307 (requires Docker: make test-up)"
	@echo ""
	@echo "You can also set credentials via environment variable:"
	@echo "  export MYSQL_ROOT_PASSWORD=your_password"
	@echo ""
	@echo "Then run integration tests:"
	@echo "  INTEGRATION_FORCE=true go test -v -run 'TestOrchestrator_.*_Integration' ./internal/archiver/..."

# Run integration tests
.PHONY: test-integration
test-integration: integration-config
	@echo "Running integration tests..."
	@if [ -z "$(MYSQL_ROOT_PASSWORD)" ]; then \
		echo "WARNING: MYSQL_ROOT_PASSWORD not set. Using value from integration_test.yaml"; \
		echo "Set it with: export MYSQL_ROOT_PASSWORD=your_password"; \
		echo ""; \
	fi
	INTEGRATION_FORCE=true go test -v -run 'TestOrchestrator_.*_Integration' ./internal/archiver/...

# Start test databases (Docker)
.PHONY: test-up
test-up:
	@echo "Starting test databases with Docker..."
	cd tests && docker compose up -d
	@echo ""
	@echo "Test databases starting up. Wait a few seconds for them to be ready."
	@echo "Run 'make test-integration' to run the integration tests."

# Stop test databases
.PHONY: test-down
test-down:
	@echo "Stopping test databases..."
	cd tests && docker compose down

# Show test database status
.PHONY: test-status
test-status:
	@echo "Test database status:"
	@cd tests && docker compose ps

# Run the working Sakila end-to-end suite (tests 06/07/08).
# Assumes Docker test DBs are already up (`make test-up`). Use `make e2e-setup`
# for a fresh-environment run that also resets source + destination from Sakila.
.PHONY: e2e
e2e:
	@bash tests/scripts/run-tests.sh --sakila --skip-docker

# Run the working Sakila suite with full env bootstrap (docker + sakila load +
# schema dump + archive schema). Slower — use for clean-slate verification.
.PHONY: e2e-setup
e2e-setup:
	@bash tests/scripts/run-tests.sh --setup --sakila

# Run the validation-demonstration tests (01-05). These are EXPECTED to fail
# preflight with documented error categories — success here means the failures
# still match the documented expectations.
.PHONY: e2e-examples
e2e-examples:
	@bash tests/scripts/run-tests.sh --sakila-examples --skip-docker

# Help target
.PHONY: help
help:
	@echo "MySQL Archiver - Build Targets"
	@echo ""
	@echo "  make build              - Build binary with version info (bin/goarchive)"
	@echo "  make install            - Install to \$$GOPATH/bin"
	@echo "  make dev                - Quick dev build (no version injection)"
	@echo "  make test               - Run all tests"
	@echo "  make test-unit          - Run unit tests only (fast)"
	@echo "  make test-ci            - Run tests with race detection (CI style)"
	@echo "  make test-integration   - Run integration tests (requires config + databases)"
	@echo "  make check              - Run all CI checks (fmt-check, vet, test-ci, build)"
	@echo "  make github-release     - Full CI pipeline + release build"
	@echo "  make vet                - Run go vet (CI style)"
	@echo "  make vet-all            - Run go vet with all checks (stricter)"
	@echo "  make lint               - Run linter"
	@echo "  make fmt                - Format Go code"
	@echo "  make fmt-check          - Check formatting (CI style)"
	@echo "  make release            - Build binaries for all platforms"
	@echo "  make clean              - Remove build artifacts"
	@echo "  make version            - Show current version settings"
	@echo "  make integration-config - Create/edit integration test configuration"
	@echo "  make test-up            - Start test databases (Docker)"
	@echo "  make test-down          - Stop test databases"
	@echo "  make test-status        - Show test database status"
	@echo "  make e2e                - Sakila E2E (working tests 06/07/08) — assumes DBs up"
	@echo "  make e2e-setup          - Sakila E2E with full env bootstrap"
	@echo "  make e2e-examples       - Sakila validation demonstration tests (01-05)"
	@echo "  make help               - Show this help"
	@echo ""
	@echo "Integration Test Quick Start:"
	@echo "  1. make integration-config  (set your credentials)"
	@echo "  2. make test-up             (start Docker databases)"
	@echo "  3. make test-integration    (run tests)"
	@echo "  4. make test-down           (stop databases when done)"
	@echo ""
	@echo "Current version: $(VERSION) ($(COMMIT))"
