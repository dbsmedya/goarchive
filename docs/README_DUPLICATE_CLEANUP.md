# Cleaning up duplicate rows

GoArchive can remove duplicate rows from a table: keep one row per business key and
archive (or purge) every other row. No helper table, script or stored procedure is
needed — one job with one `where` does it, and every safety of a normal run applies:
preflight, `dry-run` counts, batched auto-committed deletes, verification before
delete, and crash-safe resume.

## The rule: keep the minimum primary key

For each value of the duplicated key, the row with the **smallest primary key** is
the keeper. Every row whose primary key is larger than its key's keeper is a
duplicate. The keeper itself can never match: its primary key is not larger than
itself.

## The job

Given a table `payment` whose rows are duplicated by `base_payment_id`, indexed by
`idx_base_payment_id`:

```yaml
jobs:
  payment_dedup:
    root_table: payment
    primary_key: id
    where: >-
      payment.base_payment_id IS NOT NULL
      AND payment.id > (
        SELECT keeper.id
        FROM payment AS keeper FORCE INDEX (idx_base_payment_id)
        WHERE keeper.base_payment_id = payment.base_payment_id
        ORDER BY keeper.id
        LIMIT 1
      )
```

- **Name the outer table by its table name** (`payment.id`, `payment.base_payment_id`)
  and give the inner one an alias (`keeper`). GoArchive inserts the `where` as written
  into queries on the root table, so the table name is how the subquery refers to the
  row being tested.
- **`IS NOT NULL`** leaves rows without a key alone. `NULL` never equals `NULL`, so such
  rows are never duplicates of each other.
- **Index the key column.** The subquery runs once per candidate row; without an index
  on `base_payment_id` each run scans the table. `FORCE INDEX` pins that index in case
  the optimizer picks a worse plan; the query means the same without it.
- **The `where` is a condition inside one SQL statement.** It may contain subqueries,
  but it must not end the statement or add another one. A `;` inside a quoted value
  (`note = 'a;b'`) is fine.

## Archive or purge

| Command | What happens to the duplicates |
|---|---|
| `archive` | Copied to the destination, verified, then deleted from the source. The duplicates stay recoverable in the destination. |
| `purge` | Deleted from the source without copying. |

Prefer `archive` for a first cleanup: the destination is your record of exactly which
rows were removed. `archive` needs the table to exist in the destination schema with
the same structure (preflight checks it).

## Run it

```bash
goarchive validate -c archiver.yaml -j payment_dedup
goarchive dry-run  -c archiver.yaml -j payment_dedup
goarchive archive  -c archiver.yaml -j payment_dedup
```

Check the `dry-run` count against your own count of duplicates before the real run:

```sql
SELECT COALESCE(SUM(n - 1), 0) FROM (
  SELECT COUNT(*) AS n FROM payment
  WHERE base_payment_id IS NOT NULL
  GROUP BY base_payment_id
) AS g;
```

For a job over the whole table the two numbers must be equal. If they are not, the
`where` does not say what you mean — fix it before running.

## Why batching is safe here

The run fetches duplicates in primary-key order, one batch at a time, and deletes
them while later batches are still to come. That does not move any keeper: deleting
a duplicate never changes which row of its key has the smallest primary key. Each
batch's `where` therefore selects the same rows it would have selected at the start.

## Large tables

On a large table, add a primary-key range to the `where` and move it forward from one
run to the next (`AND payment.id BETWEEN 1 AND 10000000`). Each run stays short, its
`dry-run` count stays cheap, and you can stop between ranges. Keep the key column
indexed; the range does not replace the index.

With a range, the `dry-run` count is that range's **share** of the whole-table
`GROUP BY` figure above, and the shares of all ranges add up to it. Do not compare a
range's count with a `GROUP BY` limited to the same range: the subquery finds each
key's keeper anywhere in the table, so a range whose keys have their keeper in an
earlier range holds more duplicates than a range-limited `GROUP BY` reports.

Move the range **forward** only. A job keeps its checkpoint — the highest primary key it
completed — after it finishes, and never looks below it again. A later range above the
checkpoint is processed; running the same job name over an earlier range silently finds
nothing. To go over earlier rows again, use a new job name or
[retire the job](README_JOBS_SCHEMA.md#retiring-a-job-completely).

Pace the deletes with `batch_delete_size` and `delete_sleep_seconds` if replicas
lag — see [Tuning throughput](README_OPERATIONS.md#tuning-throughput).

## Limits that apply

- **Cold data.** Do not let the application write duplicates, or delete keepers,
  while the job runs. See
  [No DDL and no concurrent writes during a run](README_LIMITATIONS.md#no-ddl-and-no-concurrent-writes-during-a-run-contract).
- **Child tables.** If other tables reference the duplicated table by foreign key,
  declare them as `relations` so their rows move with each duplicate; preflight refuses
  a job whose incoming foreign keys are not covered. See
  [Uncovered incoming foreign keys](README_LIMITATIONS.md#uncovered-incoming-foreign-keys).
- **Primary key.** The root table needs a single-column integer primary key. See
  [Known Limits & Caution](README_LIMITATIONS.md#known-limits--caution).
