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
- [Environment matrix](#environment-matrix)
- [End-to-end (Sakila) tests](#end-to-end-sakila-tests)

---

## Test layers

| Layer | Database required | Build tag | What it proves |
|-------|:-----------------:|-----------|----------------|
| Unit | no | — | GoArchive's own logic, and each preflight check's policy over the library's facts |
| Integration | real MySQL | `integration` | Preflight on real facts, the engine's copy, verification, deletes and recovery, tracking, connections, and replication gating against a live replica |
| Environment matrix | throwaway MySQL pairs | `integration` | The integration suites again under one changed server variable, init option or MySQL version at a time |
| End-to-end | real MySQL + Sakila | — | The built CLI binary, end to end, on a realistic schema |

The commands for each layer are in [tests/README.md → Overview](../tests/README.md#overview).

---

## Unit tests

Fast, with no database (`go test -short ./...`). Coverage is measured on demand from the current
checkout, never kept as a snapshot
([tests/README.md → Coverage on demand](../tests/README.md#coverage-on-demand)).

**Preflight tests inject facts; they do not mock SQL.** Preflight's facts come from
`dbsgomysql`, so its tests hand the checks the library's typed values (`validations.TableInfo`,
`validations.Grants`, …) and open no database handle. Mocking the library's queries instead would
tie the test to `dbsgomysql`'s SQL wire format, which nothing verifies, so a library change could
diverge while the suite stayed green; injecting the facts ties it to the library's API, which the
compiler checks.

**`sqlmock` covers SQL GoArchive owns** and that a unit test cannot reach otherwise:
connection-time probes in `internal/database` (the session pins, `MAX_ERROR_COUNT_CHECK`,
`AUTO_INCREMENT_ZERO_MODE_CHECK`) and the advisory-lock statements in `internal/lock`. Everything
else GoArchive sends — the engine's statements, the tracking tables, INSERT diagnostics — is
proven by the integration tests on real servers.

### Retained application-owned MySQL probes

The boundary is ownership, not whether a statement inspects MySQL. These read what GoArchive's
own session reports, not the metadata catalog the library owns, so they stay GoArchive SQL:

- **Session pins** — every pool proves its sessions run in UTC (by the effective offset) and carry
  `NO_AUTO_VALUE_ON_ZERO` (`AUTO_INCREMENT_ZERO_MODE_CHECK`).
- **`max_error_count`** — every new source and destination connection reads its session value
  and, at 0, sets 1024 (`MAX_ERROR_COUNT_CHECK`).
- **Server identity** — `@@GLOBAL.server_uuid`, `DATABASE()`, hostname and port of both pools, to
  refuse a source and destination that are one database (`SRC_DEST_IDENTITY_CHECK`).
- **INSERT diagnostics** — `SHOW COUNT(*) WARNINGS`, `SHOW WARNINGS` and the session's
  `max_error_count`, `sql_notes` and `sql_mode`, read on the copy's own transaction.

## Consumer-policy enforcement

`make consumer-policy` runs the `TestConsumerPolicy*` guards in `tests/harness`, and `make check`,
which CI runs, includes it. The rule is in [dbsgomysql integration](README_dbsgomysql.md); the
guards prove that:

- production Go files do not name the metadata catalog (`information_schema`) in a string literal;
- production Go files do not name `SHOW REPLICA STATUS` or `SHOW SLAVE STATUS` — those reads belong
  to `dbsgomysql/pkg/replication`;
- unit tests do not encode dbsgomysql query fragments, aliases, result column layouts or fallback
  choreography; files with the `integration` build tag are outside that rule.

The detector patterns live in `tests/harness/testdata/consumer_policy/`, so the guard's own source
cannot exempt itself.

---

## Integration tests

Real MySQL: a source, a destination in `+03:00` and a live replica. Most tests build their fixtures
in scratch schemas they create and drop; some read the seeded Sakila estate, so the estate is set
up first ([tests/README.md → Integration tests](../tests/README.md#integration-tests)). By
package:

| Package | Proves |
|---------|--------|
| `internal/preflight` | Every check against real facts: structure, keys, relations and pairing, destination compatibility and unique indexes, triggers and their visibility proof, cascades, each permission state (role-held, `partial_revokes`, pools of one), foreign-key visibility |
| `internal/engine` | Archive, purge and copy-only runs: the key matrix (every key type × job type), composite and permuted keys, NULL foreign keys, collations, the DST fold across the UTC pin, INSERT diagnostics, the key-set and range-warning refusals, verification detecting a changed value, recovery after a kill at every crash point, recovery stopping before any delete when the destination lacks a rediscovered row, the refusal of a self-referencing `CASCADE` or `SET NULL`, the sentinel and graceful stop, locking and concurrency, the job binding, `dry-run`'s plans |
| `internal/tracking` | The schema label and the refused revisions, the typed log, the log-key and binding checks, registration and leftover logs |
| `internal/database` | The session pins, `wait_timeout`, `max_error_count`, AUTO_INCREMENT zero, the identity guard, single-statement connections, a partly failing replica fleet |
| `internal/key` | Key SQL shapes on a real server: range access, exact `IN` matching per type, wire sizes, the range-warning check |
| `cmd/goarchive/cmd` | Every command through the CLI layer: flags, banners, output, `validate` over every job, an archive whose rows are larger than the destination's `max_allowed_packet`, an archive under a server's finite `sql_select_limit` |

`TestMySQLLiveReplicationGate` stops the real replica's applier and needs the estate to itself.

---

## Environment matrix

The same suites under one changed environment at a time: a `SET GLOBAL` (strict and non-strict
`sql_mode`, `ANSI_QUOTES`, `autocommit`, `max_error_count`, `max_allowed_packet`, `wait_timeout`,
`sql_select_limit`, character sets and more), an init option (`lower_case_table_names`) or another server (Percona and
Oracle, 8.0.40 to 9.7). Each row states its expected outcome — the run is exact, or refused at
startup by name — and a judge checks it from `go test -json`
([tests/README.md → Environment matrix](../tests/README.md#environment-matrix)). Before a merge or
a release, the full matrix and the gate on every supported image run on GitHub
([The full matrix runs on GitHub](../tests/README.md#the-full-matrix-runs-on-github)).

---

## End-to-end (Sakila) tests

E2E tests archive the Sakila sample database, and a key-matrix fixture of the key types Sakila
lacks, through the **real CLI binary**:

- **Working runs** must complete, and are checked on five independent axes: exact row counts on
  both sides, verification over every copied row, referential integrity, a pacing floor, and the
  tracking tables;
- **Validation demos** must fail preflight with the expected check.

The catalogue and how to run it:
[tests/README.md → Sakila E2E Test Suite](../tests/README.md#sakila-e2e-test-suite).
