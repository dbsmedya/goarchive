# The E2E suite — structure and the per-test file format

End-to-end tests that run the real `goarchive` binary against the real Sakila
estate.

**This file owns the *mechanics*: the layout, the test-file format, and how to add
a test.** It does not describe what the individual tests prove — `../README.md`
owns the catalogue, the four assertion axes, the pacing-floor formula, and the
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
└── copy-only/     07       copy without deleting
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

Do **not** put `--force-triggers` anywhere. `run_archive_job` applies it per
command, and `copy-only` does not accept the flag at all.

### Four variables fail closed

A `working` test that omits `verify_method`, `min_duration`, `expected_rows`, or —
once `tables` names more than one table — `orphan_checks` is **refused, not run**.

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
   - working → `run_e2e_suite "3 4 5 6 7 NN" "working"`
   - demos → `run_e2e_suite "1 2 NN" "validation demos"`

4. **Update the catalogue in `../README.md`** and the category's `README.md`.

5. **Verify:** `./scripts/run-tests.sh --sakila -t NN` (or `--sakila-examples -t NN`).
   Both `-t 7` and `-t 07` work.

## Three invariants to know before editing `lib/`

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
