# Testing Reference

The test layers, how to run each, and what they need.

> 📖 **[`tests/README.md`](../tests/README.md) is the source of truth for
> integration and E2E testing.** It owns the full command matrix, the Sakila E2E
> suite and its expected error categories, single-test targeting, environment
> variables, troubleshooting, and how to add a test. This page is the overview —
> go there for detail rather than expecting it repeated here.

**Related:** [Configuration](README_CONFIGURATION.md) ·
[Validation & Preflight](README_VALIDATION.md) ·
[Operations](README_OPERATIONS.md) · [← Back to README](../README.md)

---

## Contents

- [Test layers](#test-layers)
- [Unit tests](#unit-tests)
- [Integration tests](#integration-tests)
- [End-to-end (Sakila) tests](#end-to-end-sakila-tests)
- [The reseed requirement](#the-reseed-requirement)
- [Make targets](#make-targets)

---

## Test layers

| Layer | Database required | Build tag | Command |
|-------|:-----------------:|-----------|---------|
| Unit | no | — | `go test -short ./...` |
| Integration | real MySQL | `integration` | `make test-integration` |
| End-to-end | real MySQL + Sakila | — | `make e2e` |

---

## Unit tests

Fast, no database, `sqlmock`-backed.

```bash
go test -short ./...          # unit only
go test ./... -count=1        # unit, no cache
go test -v -run TestFunctionName ./internal/graph/
```

---

## Integration tests

Integration tests run against real MySQL — a source and a destination server.
Use Docker locally, or point them at existing instances.

### Start the test databases

```bash
make test-up        # start source/archive/replica containers
make test-status    # confirm they are running
```

Three servers are expected on ports **3305** (source), **3307** (archive), and
**3308** (replica).

### Provide credentials

Credentials live in `tests/.env`. Source it before running anything:

```bash
set -a; source tests/.env; set +a
```

Three alternatives are supported:

**A — environment variable**

```bash
export MYSQL_ROOT_PASSWORD=your_password
INTEGRATION_FORCE=true go test -v -run 'TestOrchestrator_.*_Integration' ./internal/archiver/...
```

**B — Makefile**

```bash
export MYSQL_ROOT_PASSWORD=your_password
make test-integration
```

**C — custom config file**

```bash
cp internal/archiver/integration_test.yaml /path/to/my-config.yaml
# edit credentials
export INTEGRATION_CONFIG=/path/to/my-config.yaml
INTEGRATION_FORCE=true go test -v -run 'TestOrchestrator_.*_Integration' ./internal/archiver/...
```

### Config file format

`internal/archiver/integration_test.yaml`:

```yaml
databases:
  - name: source
    host: 127.0.0.1
    port: 3305
    user: root
    password: your_password_here   # required
    database: goarchive_test

  - name: destination
    host: 127.0.0.1
    port: 3307
    user: root
    password: your_password_here   # required
    database: goarchive_test

force: false                        # true = drop/recreate databases
fixture_path: testdata/customer_orders.sql
```

### Running

```bash
make test-integration

# a single test
MYSQL_ROOT_PASSWORD=your_password go test -v \
  -run TestOrchestrator_FullArchiveCycle_Integration \
  ./internal/archiver/...

# force database recreation (clean slate)
INTEGRATION_FORCE=true MYSQL_ROOT_PASSWORD=your_password \
  go test -v -run 'TestOrchestrator_.*_Integration' ./internal/archiver/...

# via the script, with setup
bash tests/scripts/run-tests.sh --setup --integration-only

make test-down    # stop containers
```

### Orchestrator integration tests

| Test | Covers |
|------|--------|
| `TestOrchestrator_FullArchiveCycle_Integration` | End-to-end archive workflow |
| `TestOrchestrator_CrashRecovery_Integration` | Resume after simulated crash |
| `TestOrchestrator_ReplicationLagPause_Integration` | Lag monitoring behaviour |
| `TestOrchestrator_VerificationMismatch_Integration` | Data verification logic |
| `TestOrchestrator_ContextCancellation_Integration` | Graceful shutdown |
| `TestOrchestrator_EmptyResultSet_Integration` | Empty result handling |
| `TestOrchestrator_MultiLevelHierarchy_Integration` | 3-level deep relationships |

---

## End-to-end (Sakila) tests

E2E tests archive the Sakila sample database through the **real CLI binary**.
They come in two flavours:

- **Working archives** — runs that must complete successfully.
- **Validation demos** — configurations that **must fail preflight** with
  documented error categories. Success means the expected failure occurred.

```bash
make e2e            # working Sakila suite (assumes env already set up)
make e2e-setup      # full bootstrap: docker + Sakila load + schema dump, then run
make e2e-examples   # validation-failure demos
```

Sakila contains DELETE triggers (`del_film`), so Sakila `archive` and `purge`
invocations need `--force-triggers`.

---

## The reseed requirement

> **Integration and E2E runs need a freshly reseeded destination. This is the #1
> source of false failures.**

The real-database tests archive Sakila into `sakila_archive`, and several rely on
it **starting empty**. A prior run leaves rows behind, and the next run aborts
with:

```
destination already contains a row … Duplicate entry
```

That is **leftover state, not a regression.**

The real-DB tests also DELETE from source Sakila, so they are effectively
run-once against a fresh setup.

Reseed with the `--setup` flag:

```bash
bash tests/scripts/run-tests.sh --setup --sakila
# or
make e2e-setup
```

### Writing tests that do not leak state

New orchestrator integration tests should clean `archiver_job` and the per-job
`archiver_job_log_<id>` table for their job names before and after execution, so
heartbeat and lock state cannot leak between tests. Use
`testsupport.CleanupArchiverState`, which resolves the job id and drops the
per-job table — do not delete from a shared log table, which no longer exists.

Destructive CLI tests that intentionally use broken-schema fixtures must pass
`--skip-validate-preflight`, since `archive`, `purge`, and `copy-only` now run
preflight at startup.

---

## Make targets

| Target | Purpose |
|--------|---------|
| `make test-unit` | Unit tests (`go test -v -short ./...`) |
| `make test-ci` | Tests with race detection, matching CI |
| `make test-up` | Start test databases via Docker Compose |
| `make test-status` | Show test database container status |
| `make test-down` | Stop test databases |
| `make integration-config` | Create the integration test config if absent |
| `make test-integration` | Run integration tests |
| `make e2e` | Working Sakila E2E suite (skips docker bootstrap) |
| `make e2e-setup` | Full bootstrap, then the working suite |
| `make e2e-examples` | Validation-failure demo suite |
| `make deadcode` | Dead-code guard — must stay clean |
| `make lint` / `make vet` / `make fmt-check` | Static analysis |

Run `make help` for the complete list.
