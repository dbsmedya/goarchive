# GoArchive Test Coverage Report

Generated: 2026-02-05

## Summary

| Package | Coverage | Status |
|---------|----------|--------|
| `internal/logger` | 97.1% | ✅ Excellent |
| `internal/graph` | 91.2% | ✅ Excellent |
| `internal/verifier` | 89.4% | ✅ Good |
| `internal/config` | 84.9% | ✅ Good |
| `internal/lock` | 84.1% | ✅ Good |
| `internal/archiver` | 51.4% | ⚠️ Needs Improvement |
| `internal/database` | 35.9% | ⚠️ Needs Improvement |
| `internal/types` | N/A | ⚠️ No Tests |
| `cmd/goarchive` | 0.0% | ❌ Not Covered |
| `cmd/goarchive/cmd` | 0.0% | ❌ Not Covered |

**Overall Coverage:** ~68% (excluding cmd packages)

---

## Package Details

### ✅ Excellent Coverage (>90%)

#### `internal/logger` - 97.1%
| Test File | Coverage Area |
|-----------|---------------|
| `logger_test.go` | Level parsing, JSON formatting, contextual logging, log rotation config |

**Key Test Cases:**
- Log level parsing (debug, info, warn, error)
- JSON vs text formatting
- Contextual field injection
- Log rotation configuration validation

---

#### `internal/graph` - 91.2%
| Test File | Coverage Area |
|-----------|---------------|
| `builder_test.go` | Dependency graph building from relations |
| `cycle_test.go` | Cycle detection basic scenarios |
| `cycle_comprehensive_test.go` | Complex cycle detection (self-referencing, multi-node, nested) |
| `kahn_test.go` | Topological sort (Kahn's algorithm) |
| `plan_output_test.go` | Plan output formatting and serialization |
| `types_test.go` | Graph type definitions and interfaces |

**Key Test Cases:**
- Graph building from job configurations
- Cycle detection in various scenarios:
  - Self-referencing tables
  - Two-node cycles (A→B→A)
  - Multi-node cycles (A→B→C→A)
  - Nested relation cycles
- Topological sort for dependency ordering
- Plan output generation and verification

---

### ✅ Good Coverage (80-90%)

#### `internal/verifier` - 89.4%
| Test File | Coverage Area |
|-----------|---------------|
| `verifier_test.go` | Row count verification, data integrity checks |

**Key Test Cases:**
- Row count verification across source/destination
- Checksum validation for data integrity
- Batch-level verification
- Error handling for verification failures

---

#### `internal/config` - 84.9%
| Test File | Coverage Area |
|-----------|---------------|
| `config_test.go` | Config struct validation, default values |
| `loader_test.go` | YAML/JSON loading, file parsing |
| `validation_test.go` | Input validation, error messages |

**Key Test Cases:**
- Configuration file loading (YAML/JSON)
- Required field validation (primary_key, tables, etc.)
- Default value application
- Relation validation
- Database connection string validation
- Breaking change: primary_key now required validation

---

#### `internal/lock` - 84.1%
| Test File | Coverage Area |
|-----------|---------------|
| `advisory_test.go` | MySQL advisory lock acquisition/release |
| `job_lock_test.go` | Job-level locking with namespace support |
| `release_test.go` | Lock cleanup and release scenarios |

**Key Test Cases:**
- Advisory lock acquisition
- Lock timeout handling
- Namespace-scoped job locking
- Lock release on completion/error
- Connection cleanup
- Concurrent lock attempts

---

### ⚠️ Needs Improvement (50-80%)

#### `internal/archiver` - 51.4%
| Test File | Coverage Area | Integration? |
|-----------|---------------|--------------|
| `batch_test.go` | Batch processing logic | Partial |
| `delete_test.go` | Record deletion after archive | Partial |
| `discovery_test.go` | Schema discovery, relation detection | Partial |
| `orchestrator_test.go` | End-to-end archive orchestration | ✅ Yes |
| `orchestrator_cycle_test.go` | Cycle detection in orchestrator | ✅ Yes |
| `preflight_test.go` | Pre-flight checks and validation | Partial |

**Covered:**
- Basic batch archive operations
- Delete-after-archive functionality
- Schema discovery from database
- Cycle detection integration
- Pre-flight validation checks

**Gaps:**
- Error recovery scenarios
- Resume from checkpoint logic
- Large dataset handling
- Concurrent archive operations
- Rollback on failure

---

### ⚠️ Low Coverage (<50%)

#### `internal/database` - 35.9%
| Test File | Coverage Area | Integration? |
|-----------|---------------|--------------|
| `database_test.go` | Connection management, query execution | Partial |
| `signal_test.go` | Signal handling for graceful shutdown | Partial |

**Covered:**
- Basic connection establishment
- Query execution
- Signal handling setup

**Gaps:**
- Connection pool management
- Transaction handling
- Error retry logic
- Connection failure recovery
- Replica lag checking
- Complex query building

---

### ❌ No Test Coverage

#### `internal/types` - N/A
Contains type definitions only. Tests would be redundant as types are tested through usage in other packages.

#### `cmd/goarchive` - 0.0%
Main entry point for the CLI application.

**Untested:**
- Main function execution
- CLI argument parsing
- Signal handling integration
- Exit code handling

#### `cmd/goarchive/cmd` - 0.0%
Cobra CLI command definitions.

**Untested:**
- CLI command registration
- Flag parsing and validation
- Subcommand routing
- Help text generation
- Version command

---

## Test Categories

### Unit Tests (with sqlmock)

| Package | Test Files | Description |
|---------|------------|-------------|
| `config` | `*_test.go` | Pure unit tests, no database required |
| `graph` | `*_test.go` | Pure unit tests, graph algorithm verification |
| `logger` | `logger_test.go` | Pure unit tests, no external deps |

### Integration Tests (requires real MySQL)

| Package | Test Files | Database Requirement |
|---------|------------|---------------------|
| `archiver` | `orchestrator_test.go`, `orchestrator_cycle_test.go` | 3 MySQL servers |
| `database` | `database_test.go`, `signal_test.go` | 1 MySQL server |
| `lock` | `*_test.go` | 1 MySQL server |
| `verifier` | `verifier_test.go` | 2 MySQL servers |

---

## Running Tests

### Run All Tests
```bash
./tests/scripts/run-tests.sh
```

### Run Unit Tests Only (fast)
```bash
go test -short ./internal/config ./internal/graph ./internal/logger
```

### Run with Coverage
```bash
go test -cover ./...
```

### Generate HTML Coverage Report
```bash
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html
```

### Run Specific Package
```bash
go test -v ./internal/graph
```

---

## Known Test Limitations

1. **Database State Dependency**: Integration tests require clean database state. Use `reset-source-data.sh` between runs.

2. **Job Name Collision**: Tests use checkpoint table which persists across runs. Use unique job names in tests.

3. **Port Availability**: Integration tests require ports 3305, 3307, 3308 to be available.

4. **Timing Issues**: Lock tests may be flaky under heavy system load due to timing-sensitive nature.

---

## Improving Coverage

### Priority 1: CLI Commands (`cmd/`)
- Add integration tests for CLI argument parsing
- Test subcommand routing
- Test error exit codes

### Priority 2: Database Package (`internal/database`)
- Add unit tests with sqlmock for query building
- Test connection retry logic
- Test transaction handling

### Priority 3: Archiver Package (`internal/archiver`)
- Add unit tests for batch logic with sqlmock
- Test error recovery paths
- Test checkpoint resume logic

---

## Coverage Trends

| Date | Overall Coverage | Notes |
|------|------------------|-------|
| 2026-02-05 | ~68% | Initial report, all existing tests passing |

---

## Appendix: Test Infrastructure

### Test Databases
- **Source**: `127.0.0.1:3305` (sakila database)
- **Archive**: `127.0.0.1:3307` (sakila_archive database)
- **Replica**: `127.0.0.1:3308` (replication testing)

### Test Data
- Sakila sample database (rental, payment tables)
- Located in `tests/sakila-db/`

### Helper Scripts
- `check-servers.sh` - Verify database connectivity
- `setup-test-env.sh` - Full environment setup
- `reset-source-data.sh` - Restore test data
- `run-tests.sh` - Execute test suite

See [README.md](README.md) for detailed setup instructions.
