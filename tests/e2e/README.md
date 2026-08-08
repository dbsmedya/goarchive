# The E2E suite — structure and the per-test file format

End-to-end tests that run the real `goarchive` binary against the real Sakila
estate.

**This file owns the *mechanics*: the layout, the test-file format, and how to add
a test.** It does not describe what the individual tests prove — `../README.md`
owns the catalogue, the assertion axes, the pacing-floor formula, and the
estate. A second copy of those would be a second copy that disagrees.

The suite exists so someone evaluating GoArchive can read what it proves without
reading Go. If a test's intent is not clear from its own file, that is a defect in
the file.

## Layout

```
tests/e2e/
├── lib/                    the harness: shared, written once
├── validation/    01 02    configs that MUST fail preflight
├── archive/       03 04 05 copy → verify → delete
├── purge/         06       delete without copying
├── copy-only/     07       copy without deleting
└── resume/        08 09    interrupt a live run, then finish it
```

Categories are **by command under test**. That is the axis the suite's own
isolation arguments already use: 04 and 07 share a graph and a slice and differ
only in the command, so a failure in 07 alone points at `copy-only`. Each category
has its own `README.md` explaining what is peculiar to that command.

## A test is a declaration file

`<category>/NN-<slug>.test.sh`. It declares values and nothing else — no logic, no
assertions, no database calls. `lib/engine.sh` does all of that, once, for every
test.

```bash
# Test 04 — rental -> payment, a two-level tree.
test_name="Test04_RentalPayment"
test_desc="Working archive: rental -> payment (2-level tree)"
config_file="test04_rental_payment.yaml"
tables="rental payment"
mode="working"
command="archive"
verify_method="count"
min_duration="1.0"
expected_rows="200 200"
orphan_checks="payment:rental_id:rental:rental_id"
```

| Variable | Meaning |
|---|---|
| `test_name` | identifier for `results/test_N.log` and the report |
| `test_desc` | one line, printed when the test starts |
| `config_file` | a **rendered** config in `../configs/` — the tracked artifact is the `.yaml.template` |
| `tables` | tables to count, in source order |
| `mode` | `working` — runs to completion · `example` — must fail preflight |
| `expected_error` | `example` only: the error category the failure must name |
| `command` | `working` only: `archive` \| `purge` \| `copy-only` |
| `verify_method` | the method the run must report, or `none` to assert none ran |
| `min_duration` | pacing floor in seconds — formula in `../README.md` |
| `expected_rows` | exact rows moved, **one per entry in `tables`, same order** |
| `orphan_checks` | `child:fk:parent:parent_pk`, space-separated |
| `interrupt` | *(resume only)* `graceful` \| `crash` — turns on the two-run arm |
| `interrupt_after_batches` | *(resume only)* which batch to interrupt at |
| `interrupt_expect_dest` | *(resume only)* destination rows required **exactly** at that point |
| `root_pk` | *(resume only)* root table's PK column, for the checkpoint assertion |

Do **not** put `--force-triggers` anywhere. `run_archive_job` applies it per
command, and `copy-only` does not accept the flag at all.

### Four variables fail closed — and the resume group makes eight

A `working` test that omits `verify_method`, `min_duration`, `expected_rows`, or —
once `tables` names more than one table — `orphan_checks` is **refused, not run**.

The four `interrupt*`/`root_pk` variables fail closed as a **group**: declare
`interrupt` and the other three become mandatory, and an unrecognised `interrupt`
value is refused rather than quietly treated as "no interrupt". Both checks run
before the 60-second source reseed, so a misdeclared resume test fails in
**0 seconds**. `interrupt_expect_dest=0` is refused outright — it would assert that
nothing was copied before the interrupt, leaving nothing to resume from.

**`interrupt` is a flag on `mode="working"`, deliberately not a third mode.** Every
guard here is gated on `mode == "working"`; a new mode would have bypassed all of
them at once, and a missed widening fails *open*.

Each is required because its absence was *measured* to let the suite pass on a
broken run; `../README.md` records what each one caught. Two of the refusals fire
before the 60-second source reseed, so a misconfigured test fails in seconds.

Note which check actually catches an omitted `expected_rows`: the engine compares
the number of entries against `tables` up front and reports *"expected_rows has 0
entr(ies) but tables has 2"*. `assert_postcondition`'s own empty-value guard sits
behind it as defence in depth.

## Adding a test

1. **Write `../configs/testNN_<name>.yaml.template`.** The `.template` is the only
   file you author — `configs/*.yaml` is gitignored and re-rendered on every run,
   so edits to the `.yaml` are lost.

   - A test that **copies** into a destination loaded from a DDL-only dump needs
     `safety.disable_foreign_key_checks: true`. A `purge` test does **not** — the
     setting is consumed only on the copy path, so carrying it there is dead
     config.
   - **The job key must be the line immediately after `jobs:`**, with no comment on
     it and none between. `run_archive_job` extracts the job name with
     `grep -A 1 "^jobs:" | tail -1`, so anything else on that line lands inside the
     name.

2. **Add `<category>/NN-<slug>.test.sh`** with the declarations above. Pick the
   category by the command under test; create a new directory with its own
   `README.md` if none fits.

   **Measure the row counts from a real run — do not compute them.** Sakila's PK
   columns are not contiguous, which is why `payment_id <= 2000` yields 1999 and
   `payment_id <= 8024` yields 8022.

3. **Wire `NN` into the ordered dispatch list** in `../scripts/run-tests.sh` →
   `main`:
   - working → `run_e2e_suite "3 4 5 6 7 8 9 NN" "working"`
   - demos → `run_e2e_suite "1 2 10 11 NN" "validation demos"`

4. **Update the catalogue in `../README.md`** and the category's `README.md`.

5. **Verify:** `./scripts/run-tests.sh --sakila -t NN` (or `--sakila-examples -t NN`).
   Both `-t 7` and `-t 07` work.

## Four invariants to know before editing `lib/`

**Numbers are the identity; categories are only where files live.** `-t N`,
`results/test_N.log` and `configs/testNN_*.yaml` all key off the number, so a test
keeps its number if it moves between categories. The category is *derived* from
the parent directory and never declared inside the file — a declared one could
disagree with reality. Two files claiming one number is a hard error, not
last-wins.

**Run order is an explicit list, never the filesystem.** Globbing orders by
category name, which would silently turn `03 04 05 06 07` into `03 04 05 07 06`.

**Per-test values must stay local to `run_e2e_test`.** It declares them `local` and
*then* loads the file, so bash's dynamic scoping lands the assignments on those
locals. Reverse that — or lift the load out of the function — and every test
inherits the previous one's values, every fail-closed guard above stops firing,
and the suite goes on reporting a pass. Nothing else has that symptom, which is
why the suite loop asserts after each test that nothing reached global scope. That
check exercises the real mechanism: `run_e2e_suite` deliberately does not declare
those variables, so a lost `local` surfaces there.

**`ensure_destination_schema` drops and recreates the destination database, and the
tracking tables live inside it.** `destination.job_schema` defaults to the
destination database and no test config overrides it, so that call takes
`archiver_job` and `archiver_job_log_<id>` with it. For a single-run test that is
exactly what you want — no state leaks between tests and job ids restart. For the
**two-run** resume tests it is fatal: call it between the runs and the checkpoint,
the log table and the rows run 1 copied all disappear, so run 2 starts from scratch
and **every end-state assertion still passes**.

It is called once, in STEP 3a, before run 1. Measured: inserting a second call
before run 2 is caught only by `expected_rows` (`archive put 1899 row(s) in the
destination, expected 1999`) — the interrupt preconditions run *before* run 2 and
never see it, and the discriminator, the verify grep and the pacing floor all pass
straight through. If that exact count were ever relaxed to a direction check,
nothing would catch this.
