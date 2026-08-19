# MySQL Archiver - Build Configuration
# Edit VERSION below when releasing a new version

# Version configuration - EDIT RELEASE_VERSION below when releasing
# Or override per-build with: make build VERSION=1.2.3
RELEASE_VERSION := 2.1.0-community
# Use the exact git tag when building from a tagged release commit, otherwise
# fall back to the pinned RELEASE_VERSION above.
VERSION := $(shell git describe --tags --exact-match 2>/dev/null || echo "$(RELEASE_VERSION)")

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
check: fmt-check vet consumer-policy test-ci build
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

# Dead-code guard version (golang.org/x/tools/cmd/deadcode)
DEADCODE_VERSION := v0.48.0

# Fail if the production binary carries unreachable functions
.PHONY: deadcode
deadcode: ## Fail if the production binary carries unreachable functions
	@out=$$(go run golang.org/x/tools/cmd/deadcode@$(DEADCODE_VERSION) ./cmd/goarchive) || { \
		echo "deadcode: tool execution failed (exit $$?)"; exit 1; }; \
	if [ -n "$$out" ]; then \
		echo "$$out"; \
		echo "deadcode: production-unreachable functions found (see issue #9)"; \
		exit 1; \
	fi; \
	echo "deadcode: clean"

.PHONY: consumer-policy
consumer-policy: ## Enforce production-query, sqlmock-budget, and unit wire-format boundaries
	@go test ./internal/archiver/ -run '^TestConsumerPolicy' -count=1 \
		&& echo "consumer-policy: clean"

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
	@echo "  INTEGRATION_FORCE=true go test -tags=integration -v -run 'TestOrchestrator_.*_Integration' ./internal/archiver/..."

# Run integration tests
.PHONY: test-integration
test-integration: integration-config
	@echo "Running integration tests..."
	@if [ -z "$(MYSQL_ROOT_PASSWORD)" ]; then \
		echo "WARNING: MYSQL_ROOT_PASSWORD not set. Using value from integration_test.yaml"; \
		echo "Set it with: export MYSQL_ROOT_PASSWORD=your_password"; \
		echo ""; \
	fi
	@echo "NOTE: reseed first so the destination is empty: bash tests/scripts/run-tests.sh --setup"
	INTEGRATION_FORCE=true go test -tags=integration -v -run 'TestOrchestrator_.*_Integration' ./internal/archiver/...

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

# Destroy and rebuild the test databases from scratch. Use when a killed test run
# has left orphaned state that `test-down` cannot clear — `test-down` stops the
# containers but the data lives in a Docker named volume and survives.
#
# The `-v` is the whole point: it removes the db1_data/db2_data/db3_data volumes.
# (Before 2026-07-28 the datadir was a host bind mount and this target used
# `rm -rf tests/docker_files/dbdata`. See tests/compose.yml for why it moved.)
.PHONY: test-reset
test-reset:
	@rm -f tests/.e2e-ready
	@echo "Destroying test database state..."
	cd tests && docker compose down -v
	cd tests && docker compose up -d
	@echo "Containers restarted with empty data directories."
	@echo "Run 'bash tests/scripts/run-tests.sh --setup' to reload Sakila."

# The whole E2E procedure, in the only order that is correct. Use this one.
# Slow by design: it destroys the estate, rebuilds it, then runs.
.PHONY: e2e
e2e:
	@$(MAKE) test-reset
	@$(MAKE) e2e-setup
	@$(MAKE) e2e-tests-must-run-after-setup

# The bare test run. Named for its precondition because the precondition is the
# whole problem: source Sakila is drained by any integration run, so running
# this against a stale estate archives from an empty database and reports a
# meaningless pass. Refuses unless `make e2e-setup` seeded the estate.
.PHONY: e2e-tests-must-run-after-setup
e2e-tests-must-run-after-setup:
	@bash tests/scripts/require-e2e-seed.sh
	@bash tests/scripts/run-tests.sh --sakila --skip-docker

# Bootstrap the estate: docker up, Sakila load, schema dump, archive schema.
# Seeds only — it does not run any test. Writes the marker the E2E targets
# require.
.PHONY: e2e-setup
e2e-setup:
	@rm -f tests/.e2e-ready
	@bash tests/scripts/run-tests.sh --setup
	@touch tests/.e2e-ready

# Run the validation-demonstration tests (01-02). These are EXPECTED to fail
# preflight with documented error categories — success here means the failures
# still match the documented expectations: 01 = COMPOSITE_PK_CHECK,
# 02 = FK_COVERAGE_CHECK.
.PHONY: e2e-examples
e2e-examples:
	@bash tests/scripts/require-e2e-seed.sh
	@bash tests/scripts/run-tests.sh --sakila-examples --skip-docker

# Check the characterization suite against its recorded baseline.
#
# The baseline lives in tests/characterization-baseline.txt, and the script does
# the counting. Nobody counts by hand: the suite nests two levels deep, so the
# obvious `grep -c '^    --- PASS'` misses 98 subtests and manufactures a
# regression that is not there.
.PHONY: characterization
characterization:
	@bash tests/scripts/check-characterization-baseline.sh

# Refuse to proceed against a dead test estate. A killed run leaves the
# containers Exited (137), and every later step then fails with "Can't connect
# to MySQL server" -- indistinguishable from a real failure.
.PHONY: require-estate
require-estate:
	@bash tests/scripts/require-containers-up.sh

# THE FULL VERIFICATION GATE, in the only order that is correct. Use this one.
#
# The ordering, the per-stage exit-code checks and the closing summary all live
# in the script -- see its header for why they cannot safely live in recipe
# lines here (short version: `cmd | tee` returns tee's status, so collecting
# output in make would hide a failing stage).
#
# Requires credentials: set -a; source tests/.env; set +a
.PHONY: gate
gate:
	@bash tests/scripts/run-gate.sh

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
	@echo "  make test-reset         - Destroy and rebuild test DBs (clears orphaned state)"
	@echo "  make e2e                - Full E2E: test-reset, e2e-setup, then the tests. USE THIS"
	@echo "  make e2e-setup          - Step 2 only: bootstrap docker + Sakila and seed the estate"
	@echo "  make e2e-tests-must-run-after-setup - Step 3 only: the tests (refuses unless seeded)"
	@echo "  make e2e-examples       - Sakila validation demos 01-02 (COMPOSITE_PK / FK_COVERAGE)"
	@echo "  make gate               - THE FULL VERIFICATION GATE, in the correct order. USE THIS"
	@echo "  make characterization   - Check the characterization suite against its baseline"
	@echo "  make require-estate     - Fail early if the test databases are unreachable"
	@echo "  make help               - Show this help"
	@echo ""
	@echo "Integration Test Quick Start:"
	@echo "  1. make integration-config  (set your credentials)"
	@echo "  2. make test-up             (start Docker databases)"
	@echo "  3. make test-integration    (run tests)"
	@echo "  4. make test-down           (stop databases when done)"
	@echo ""
	@echo "Current version: $(VERSION) ($(COMMIT))"
