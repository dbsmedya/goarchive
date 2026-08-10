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

Fast, and no database.

```bash
go test -short ./...          # unit only
go test ./... -count=1        # unit, no cache
go test -v -run TestFunctionName ./internal/graph/
```

**Two kinds of unit test, and the difference matters when adding one.**

Tests that exercise **GoArchive's own SQL** — tracking tables, batch DML, checkpoints — use
`sqlmock`, and that is correct: the statement under test is one GoArchive wrote.

Tests that exercise a **preflight stage** do not mock SQL at all. Preflight facts come from
`dbsgomysql`, so those tests inject the library's typed values (`[]validations.TableInfo`,
`validations.Grants`, …) directly into the run the stage reads from, and most of them open no
database handle whatsoever. Mocking the library's queries instead would couple the test to
`dbsgomysql`'s SQL wire format — column names, aliases, row order — which nothing verifies, so a
library change would diverge silently while the suite stayed green. Injecting the fact couples
the test to the library's API, which the compiler checks.

A stage that ignores the injected fact and issues its own query fails with
`validations: nil Querier`, so the absence of a handle is itself the assertion.

`internal/archiver/consumer_policy_test.go` holds the guards for this: one fails the build if
non-test code names `information_schema` in a string literal, the other pins how much `sqlmock`
the converted files may still contain. If you need a mock in a pinned file, preload the fact
instead — the guard's failure message names the pattern to copy.

### Retained application-owned MySQL probes

The boundary is ownership, not whether a statement happens to inspect MySQL. Two focused
exceptions remain GoArchive SQL and therefore keep exact `sqlmock` coverage:

- Dry-run reads `SHOW VARIABLES LIKE 'max_allowed_packet'` to decide whether the INSERT
  payload GoArchive itself constructs fits the destination. This application-specific
  safety check stays local; it does not justify a generic dbsgomysql server-variable API.
- Replication lag monitoring owns `SHOW REPLICA STATUS`, its `SHOW SLAVE STATUS` fallback,
  and optional `FOR CHANNEL` clause. These administrative reads retain their current tests
  and have a separate post-2.2 horizon for reconsidering a dbsgomysql port.

Neither exception permits unit tests to reproduce dbsgomysql metadata queries, aliases,
result columns, or fallback choreography. Preflight metadata tests continue to inject the
library's exported typed facts.

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
make e2e            # the whole procedure — use this one
make e2e-examples   # validation-failure demos (needs a seeded estate)
```

`make e2e` runs `test-reset`, then `e2e-setup`, then
`e2e-tests-must-run-after-setup`. The individual steps exist for when you know
why you want one, and step 3 refuses to run unless step 2 has: any integration
run DELETEs from source Sakila, and archiving a drained database reports a
meaningless pass.

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
make e2e-setup
# or, explicitly
bash tests/scripts/run-tests.sh --setup
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
| `make e2e` | **The whole E2E procedure**: test-reset → e2e-setup → the tests |
| `make e2e-setup` | Step 2 alone: bootstrap and seed the estate |
| `make e2e-tests-must-run-after-setup` | Step 3 alone: the tests; refuses unless seeded |
| `make e2e-examples` | Validation-failure demo suite; refuses unless seeded |
| `make deadcode` | Dead-code guard — must stay clean |
| `make lint` / `make vet` / `make fmt-check` | Static analysis |

Run `make help` for the complete list.
