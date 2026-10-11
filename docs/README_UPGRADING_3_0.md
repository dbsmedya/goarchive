# Upgrading to 3.0

What changed from 2.x, for an operator moving a job to 3.0.

**Related:** [Limitations](README_LIMITATIONS.md) · [Configuration](README_CONFIGURATION.md) ·
[Operations](README_OPERATIONS.md) · [← Back to README](../README.md)

---

## Limits removed or changed

| 2.x | 3.0 |
|-----|-----|
| Single-column primary keys only (`COMPOSITE_PK_CHECK`); a foreign key spanning several columns between job tables was refused | Composite primary keys and composite foreign keys are supported at the root and at every child |
| Root keys had to be integers (`ROOT_PK_TYPE_UNSUPPORTED`) | Root keys may be any supported key type: integer, `DECIMAL`, string, binary or temporal. `FLOAT`/`DOUBLE`, `ENUM`/`SET`, `BIT` and prefix key parts are refused (`KEY_TYPE_UNSUPPORTED`) |
| A job continued from a persisted checkpoint, so eligibility had to grow in primary-key order, and a row that became eligible below the checkpoint was never archived | There is no persisted checkpoint: every run starts at the beginning of the `where` range, and a row that becomes eligible later is found by the next run |
| A job name was bound to its command and root table only, not to its source server and schema | A job is also bound to its source server and schema and its relations (purge: also its `where`; copy-only: also its destination database); a change is refused at startup |
| `--force` let `copy-only` take over a held lock with a stale heartbeat, and prompted past a non-empty destination | `--force` is removed. A copy-only rerun uses `resume: true` to skip the roots it copied before |
| `verification.method` chose the INSERT strategy and whether a pre-existing destination row aborted the run | `verification` is one value, `sha256` (the default), `count` or `none`, and chooses only the check. Every copy uses a strict INSERT: a row already in the destination fails the batch |
| `dry-run` inserted a sample into the destination and rolled it back | `dry-run` writes nothing; it reports MySQL's plans for the root fetch and each table's discovery query |
| Community and Enterprise editions; the version carried `-community` | One edition, and the version carries no edition suffix |
