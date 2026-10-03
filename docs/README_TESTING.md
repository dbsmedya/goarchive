# Testing Reference

What each test layer proves, for someone evaluating GoArchive. How to run each layer — commands,
environment, the test estate and troubleshooting — is in [tests/README.md](../tests/README.md).

**Related:** [Configuration](README_CONFIGURATION.md) ·
[Validation & Preflight](README_VALIDATION.md) ·
[Operations](README_OPERATIONS.md) · [← Back to README](../README.md)

---

## Contents

- [Test layers](#test-layers)
- [Unit tests](#unit-tests)
- [Consumer-policy enforcement](#consumer-policy-enforcement)
- [Integration tests](#integration-tests)
- [End-to-end (Sakila) tests](#end-to-end-sakila-tests)

---

## Test layers

| Layer | Database required | Build tag | What it proves |
|-------|:-----------------:|-----------|----------------|
| Unit | no | — | GoArchive's own logic and SQL, and each preflight stage's policy over the library's facts |
| Integration | real MySQL | `integration` | Copy, verification, delete, recovery and replication gating against real source, destination and replica servers |
| End-to-end | real MySQL + Sakila | — | The built CLI binary, end to end, on a realistic schema |

The commands for each layer are in [tests/README.md → Overview](../tests/README.md#overview).

---

## Unit tests

Fast, and no database (`go test -short ./...`). Coverage is measured on demand from the current
checkout, never kept as a snapshot; see
[tests/README.md → Coverage on demand](../tests/README.md#coverage-on-demand).

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

A metadata-policy test should preload an exported fact and assert the semantic result plus
the reader call count:

```go
fake := &fakePreflightInspector{
    tablesResult: []validations.TableInfo{
        {Table: "orders", Type: "BASE TABLE", Engine: "InnoDB"},
    },
}
_, run, _ := newTypedFactRun(t, fake, nil)

first, err := run.sourceTables(context.Background())
// Assert the returned fact and error, call again, then prove memoization.
second, err := run.sourceTables(context.Background())
if fake.tablesCalls != 1 {
    t.Fatalf("Tables calls = %d, want 1", fake.tablesCalls)
}
```

For SQL GoArchive owns, keep an exact expectation and assert both the behavior and mock
completion:

```go
mock.ExpectQuery(regexp.QuoteMeta(maxAllowedPacketQuery)).
    WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
        AddRow("max_allowed_packet", "67108864"))
// Call the GoArchive method, assert its value/error, then ExpectationsWereMet.
```

If you need a mock in a file the [consumer-policy guards](#consumer-policy-enforcement) pin,
preload the fact instead — the guard's failure message names the pattern to copy.

### Retained application-owned MySQL probes

The boundary is ownership, not whether a statement happens to inspect MySQL. The following application probes remain GoArchive SQL and therefore keep exact `sqlmock` coverage:

- Dry-run reads `SHOW VARIABLES LIKE 'max_allowed_packet'` to decide whether the INSERT
  payload GoArchive itself constructs fits the destination. This application-specific
  safety check stays local; it does not justify a generic dbsgomysql server-variable API.
- `database.Manager.Connect` proves, on every pool, that the session honours the UTC
  `time_zone` the DSN pins (by its effective offset) and that its `sql_mode` carries
  `NO_AUTO_VALUE_ON_ZERO` (`AUTO_INCREMENT_ZERO_MODE_CHECK`).
- `database.Manager.Connect` reads `@@GLOBAL.server_uuid`, `DATABASE()`, `@@GLOBAL.hostname`
  and `@@GLOBAL.port` from both pools to refuse a configuration whose source and
  destination are one database (`SRC_DEST_IDENTITY_CHECK`, issue #13). It inspects what the
  session reports about itself, not the metadata catalog, and does not justify a dbsgomysql
  server-identity fact.
- Copy and dry-run own `SHOW COUNT(*) WARNINGS`, `SHOW WARNINGS`, and the pre-operation
  `@@SESSION.max_error_count`, `@@SESSION.sql_notes`, `@@SESSION.sql_mode` read.
  These establish the diagnostics of GoArchive's own INSERT on its transaction.
  `TestReadDiagnosticSession` and `TestCollectInsertDiagnosticsFailures` cover I/O
  refusal; `TestInsertDiagnosticPolicy` covers numeric allowlists and completeness.

## Consumer-policy enforcement

`make consumer-policy` runs every `TestConsumerPolicy*` guard in `internal/archiver`, and
`make check`, which CI runs, includes it. The rule the guards enforce is described in
[dbsgomysql integration](README_dbsgomysql.md); the guards prove that:

- production Go files do not query the metadata catalog owned by dbsgomysql;
- production Go files do not name `SHOW REPLICA STATUS` or `SHOW SLAVE STATUS` in a string
  literal — those reads belong to `dbsgomysql/pkg/replication`;
- the remaining application-owned `sqlmock` budgets match their reviewed inventory;
- non-integration unit tests do not encode dbsgomysql query fragments, aliases, result
  column layouts, or fallback choreography.

Test files carrying the `integration` build tag or an `_integration_test` name are classified
as real-estate tests and are outside the unit wire-format invariant. The detector patterns are stored in
`internal/archiver/testdata/consumer_policy/dbsgomysql_wire_rules.tsv`, so the guard's own
source cannot exempt itself.

---

## Integration tests

Integration tests run against real MySQL — a source, a destination and a live replica.

Focused value-preservation tests use production `BuildDSN` with real sessions:
`TestTemporalPayloadStrictIgnore_Integration`, the collision and valid-key archive/
purge tests, dirty-destination SHA256, driver implicit/explicit close, session
ownership and late-table rollback. These are integration tests at package entry
points. Conversion codes 1264/1265/1366 are reached at the Copy boundary;
1292 is reached at the collector boundary with an explicit CAST expression, not
SQL generated by GoArchive. Normal preflight is retained in orchestrator cases.
Profile-only value-preservation tests skip on the ordinary estate and run against disposable
MySQL profiles ([tests/README.md](../tests/README.md#value-preservation-profile-tests));
`TestValuePreservationCLI_Integration` is an E2E witness
([the E2E guide](../tests/e2e/README.md#focused-value-preservation-cli-witnesses)).

### Orchestrator integration tests

| Test | Covers |
|------|--------|
| `TestOrchestrator_FullArchiveCycle_Integration` | End-to-end archive workflow |
| `TestOrchestrator_CrashRecovery_Integration` | Resume after simulated crash |
| `TestOrchestrator_ReplicationGate_Integration` | Replication gating against a live replica: healthy pass, stopped-applier hold, and lag hold — each proving the job resumes on the same invocation |
| `TestOrchestrator_VerificationMismatch_Integration` | Data verification logic |
| `TestOrchestrator_ContextCancellation_Integration` | Graceful shutdown |
| `TestOrchestrator_EmptyResultSet_Integration` | Empty result handling |
| `TestOrchestrator_MultiLevelHierarchy_Integration` | 3-level deep relationships |
| `TestOrchestrator_BatchArchive_Integration` | Archive with per-job batch overrides: rows copied and deleted, no root left pending |

---

## End-to-end (Sakila) tests

E2E tests archive the Sakila sample database through the **real CLI binary**.
They come in two flavours:

- **Working archives** — runs that must complete successfully.
- **Validation demos** — configurations that **must fail preflight** with
  documented error categories. Success means the expected failure occurred.

The real-database tests delete from source Sakila and expect an empty destination, so every
run starts from a freshly reseeded estate. How to run and reseed:
[tests/README.md → Sakila E2E Test Suite](../tests/README.md#sakila-e2e-test-suite).
