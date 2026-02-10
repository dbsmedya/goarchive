# MySQL Archiver - Build Configuration
# Edit VERSION below when releasing a new version

# Version configuration - EDIT THIS when releasing
# Or use: make build VERSION=1.2.3
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "0.1.0-dev")

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

# Run all tests
.PHONY: test
test:
	go test -v ./...

# Run unit tests only (fast)
.PHONY: test-unit
test-unit:
	go test -v -short ./...

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

# Format code
.PHONY: fmt
fmt:
	go fmt ./...

# Run linter (requires golangci-lint)
.PHONY: lint
lint:
	golangci-lint run ./...

# Check for vulnerabilities (requires govulncheck)
.PHONY: vulncheck
vulncheck:
	govulncheck ./...

# Help target
.PHONY: help
help:
	@echo "MySQL Archiver - Build Targets"
	@echo ""
	@echo "  make build       - Build binary with version info (bin/goarchive)"
	@echo "  make install     - Install to \$$GOPATH/bin"
	@echo "  make dev         - Quick dev build (no version injection)"
	@echo "  make test        - Run all tests"
	@echo "  make test-unit   - Run unit tests only (fast)"
	@echo "  make release     - Build binaries for all platforms"
	@echo "  make clean       - Remove build artifacts"
	@echo "  make version     - Show current version settings"
	@echo "  make fmt         - Format Go code"
	@echo "  make lint        - Run linter"
	@echo "  make help        - Show this help"
	@echo ""
	@echo "Current version: $(VERSION) ($(COMMIT))"
