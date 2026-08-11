# Architecture review: tracking-schema version gate

**Date:** 2026-08-11  
**Scope:** Phase 5 / PR-07 (`rc-phase-028`)  
**Branch:** `feat/resume-tracking-schema-version-gate`  
**Verdict:** PASS  
**Quality:** Excellent

## Summary

The implementation replaces the obsolete dbsgomysql-backed legacy-shape inspection with
an explicit, fail-closed `goarchive_meta` revision declaration. All Phase-028 acceptance
criteria are met: the six private-wire fixtures and exemptions are gone, compatible schemas
are stamped without checkpoint loss, unknown revisions are refused, and the shipped
operator documentation agrees with runtime behavior.

The review found one important concurrency edge in the first implementation: an
`INSERT IGNORE` collision could return success without validating the winning marker. A
regression test first reproduced that failure; the gate now checks `RowsAffected` and
re-reads and validates a concurrently written declaration before proceeding. No open
critical, important, or minor findings remain.

## Acceptance criteria

| Criterion | Result | Evidence |
|---|---|---|
| No `checkLegacySchema` implementation or test remains | PASS | Repository search is empty; obsolete integration file and test removed |
| No private dbsgomysql wire fixtures remain in unit tests | PASS | `make consumer-policy` is clean with zero exemptions |
| Revision `2.0` is declared and unknown/malformed values fail closed | PASS | Focused absent/current/newer/malformed/read/write/create/probe/concurrency tests |
| Fresh and compatible pre-marker schemas are stamped safely | PASS | Three real-MySQL marker tests; checkpoint `4242` preserved |
| Mutation sensitivity is demonstrated | PASS | Five specified mutants failed their named tests and were reverted; concurrency regression observed red before its fix |
| Documentation and permissions agree | PASS | Schema, configuration, upgrade, permissions, and test guides reconciled |
| Complete quality gates pass | PASS | Static/unit/race green; integration `1077/0/1`; characterization `60/304/364/0/0`; E2E `7/0`; validation demos `4/0` |
| Dependency scope is unchanged | PASS | No `go.mod` or `go.sum` delta; dbsgomysql remains v0.9.0 |

## Verification record

- Formatting, build, vet, integration-tag vet, lint, deadcode, consumer policy, and module
  tidy: PASS.
- Unit and race suites: PASS across all packages.
- Short coverage measured outside the repository: repository `50.6%`,
  `internal/archiver` `72.8%`.
- Integration: `PASS=1077 FAIL=0 SKIP=1`; the skip is
  `TestExecute_CheckpointCallbackError`.
- Characterization: `60 / 304 / 364 / 0 / 0`.
- Working E2E: 7 passed, 0 failed.
- Validation demos: 4 expected failures matched, 0 failed.
- One integration remeasurement encountered the pre-existing timed
  `TestOrchestrator_CrashRecovery_Integration` failure. It passed immediately in isolation;
  the next clean full integration run passed `1077/0/1`.

## Final assessment

Phase 5 is production-ready for PR review. It introduces no reader seam, metadata query,
MySQL error-code dependency, tracking-table reshape, upstream dependency change, tag, or
release action. PR-07 may be opened after the final branch push; merge and release remain
operator-controlled.
