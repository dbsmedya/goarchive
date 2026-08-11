# PR-06 dbsgomysql Consumer-Boundary Review

**Date:** 2026-08-11  
**Baseline:** `main` at `0bcb95b`, including dbsgomysql v0.9.0 adoption PR #90  
**Scope:** Phase 4 / PR-06 unit consumer-policy enforcement and durable testing guidance

## Boundary verdict

GoArchive's non-integration unit tests exercise dbsgomysql through exported typed facts.
Exact SQL mocks remain only for statements GoArchive writes or deliberately owns. The six
legacy tracking-schema probe sites are structurally budgeted for deletion by Phase 5 /
PR-07; no other unit-test exemption exists.

## Metadata-mock inventory

| Original file | Starting library-SQL sites | Final disposition |
|---|---:|---|
| `preflight_facts_test.go` | 34 | Converted to typed inspector facts and call-count assertions |
| `root_pk_meta_test.go` | 5 | Converted to typed column/PK policy facts |
| `composite_pk_test.go` | 4 | Converted to typed primary-key facts; zero SQL handles |
| `preflight_test.go` | 4 | Converted to typed facts and explicit stage execution |
| `resume_test.go` | 4 | Temporarily retained for `checkLegacySchema`; Phase 5 deletion owner recorded |
| `copyonly_orchestrator_test.go` | 3 | One non-028 site converted; two startup probe sites temporarily retained for Phase 5 |
| **Total** | **54** | **48 converted; 6 exact Phase-5-owned exemptions remain** |

The AST guard also detects known dbsgomysql query fragments, fallback sources, grant
acquisition statements, SELECT-list signatures, and metadata result-column layouts. Files
with an integration build constraint or an integration-test filename are explicitly
classified as real-estate tests rather than silently ignored.

## Application-owned SQL retained

- `SHOW VARIABLES LIKE 'max_allowed_packet'` remains GoArchive's dry-run payload safety
  query with exact success, query, scan, parse, and caller-wrap coverage.
- `SHOW REPLICA STATUS`, the legacy `SHOW SLAVE STATUS` fallback, and optional channel
  clause remain GoArchive's documented administrative exception with a post-2.2 review
  horizon.
- Tracking tables, advisory locks, checkpoints, batch DML, copy, verification, and deletion
  remain valid exact-SQL unit-test responsibilities.

## Mutation proof

A temporary non-integration unit-test declaration containing a metadata-catalog SELECT was
added and `make consumer-policy` failed non-zero at that file and line with
`literal:information-schema`. The temporary file was deleted, and the same target returned:

```text
ok github.com/dbsmedya/goarchive/internal/archiver
consumer-policy: clean
```

No mutation file remains in the tree.

## Verification evidence

| Check | Result |
|---|---|
| `make consumer-policy` | PASS after the observed red mutation |
| Focused `Payload|MaxAllowedPacket` tests | PASS, including query, scan, parse, and caller-wrap paths |
| `go test -short ./... -count=1` with coverage | PASS; `internal/archiver` 72.7% |
| Generated repository short coverage | 50.5% statements; profile generated with this change |
| `go test -short -race ./... -count=1` | PASS across all packages |
| `go vet ./...` | PASS |
| `make lint` | PASS, 0 issues |
| `make fmt-check` | PASS |
| `go build ./...` | PASS |
| `go mod tidy -diff` | Empty diff |
| Independent Luna `make gate` | PASS; every canonical stage exited 0 |

The independent full gate started the three-server estate and verified reachability on
ports 3305, 3307, and 3308. Its measured results were:

- unit: 783 test passes, 4 skips, 0 failures; nine test-bearing packages passed and two had
  no tests;
- integration: `PASS=1072 FAIL=0 SKIP=1`;
- characterization: `60 / 304 / 364 / 0 / 0`;
- working E2E: 7 passed, 0 failed;
- validation demos: 4 passed, 0 failed, with every expected failure category matched.

Formatting, vet, lint, consumer policy, and dead-code stages also passed. The gate performed
its documented test-estate reset and reseed for E2E, made no source edits, and left the
tracked worktree clean at `a955d6f`.

The generated profile lived outside the repository and is not committed. Percentages in
this review are evidence for this exact change, not a maintained snapshot.

## Deferred scope

PR-06 does not change production behavior, integration fixtures, schemas, release versions,
characterization expectations, or E2E scenarios. The broader real-MySQL dbsgomysql adapter
sweep remains a future integration phase. Phase 5 / PR-07 owns the tracking-schema revision
marker, deletion of `checkLegacySchema` and all six exemptions, three scoped real-MySQL
marker tests, and the final measured integration baseline.

## Architecture review

**Verdict:** PASS  
**Quality:** Excellent  
**Recommendation:** Ready for PR review; no critical, important, or minor findings remain.

| Acceptance criterion | Status | Evidence |
|---|---|---|
| Forbidden unit metadata literal fails policy, then clean tree passes | Met | Observed red/green mutation recorded above |
| No non-integration unit test outside six sites encodes private wire details | Met | Repository-wide AST scan is green; every declared rule has positive detector coverage |
| Every temporary exemption names file, reason, and Phase-5 deletion owner | Met | Five structural entries account for exactly six sites and exact fingerprint counts |
| Complete consumer-policy prefix runs from Make | Met | Production-query, sqlmock-budget, detector, and integration-classification tests pass |
| Durable guidance and final inventory replace stale snapshot | Met | Canonical testing guide updated; February snapshot deleted; 54-site disposition recorded |
| Required short, race, vet, lint, format, build, tidy, and coverage checks pass | Met | Verification matrix above |

The review found no production, API, schema, dependency, or release-version changes. The
hardening follow-up performed during review expanded the detector fixture so every declared
wire fingerprint is exercised and added a negative fixture proving retained application SQL
does not trip the guard.
