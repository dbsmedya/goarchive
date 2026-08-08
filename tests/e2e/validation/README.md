# validation — configs that must be REJECTED

These are the only tests in the suite that **expect failure**. Each config is
deliberately invalid; the test passes when `validate` exits non-zero *and* names
the documented error category. A config here that started passing preflight would
be a product regression, and the test says so.

They run separately from the working suite — `make e2e-examples`, or
`run-tests.sh --sakila-examples` — because they assert on preflight and never
touch data.

| Test | Error category | What is wrong with the config |
|---|---|---|
| 01 | `COMPOSITE_PK_CHECK` | includes `film_actor` / `film_category`, which have composite primary keys; goarchive supports single-column root PKs only |
| 02 | `FK_COVERAGE_CHECK` | archives `film` while `inventory`, `film_actor` and `film_category` still reference it, so the delete would violate their foreign keys |
| 10 | `TABLE_EXISTENCE_CHECK` | `root_table` is the view `customer_list`; only base tables can be archived — the **non-base-table policy** |
| 11 | `INTERNAL_FK_COVERAGE` | the `customer → rental → payment` GDPR diamond — `payment` has two in-graph parents, so one FK edge is always undeclared |

`mode="example"` selects this arm of the engine. The only other variable that
matters is `expected_error`, a **substring** of the failure output.

## Choosing `expected_error`: the category alone is sometimes vacuous

Prefer the bare category — the wording can then improve without breaking the test.
**But check first that the category has only one cause.** `TABLE_EXISTENCE_CHECK`
has two: a name that is absent, and a name that is present but is not a base table.
Test 10 therefore asserts `TABLE_EXISTENCE_CHECK: Only base tables`.

This is measured, not cautious. Mutating test 10 to the bare category *and* typo-ing
its `root_table` leaves the test **passing** — goarchive says *"Tables not found in
source database"*, the substring matches, and a test whose whole subject is views
proves nothing about views.

> So the older claim here — that matching on the category means "a config that fails
> for a different reason still fails the test" — **is not true in general**, and this
> paragraph replaces it. It holds only where the category maps to a single cause,
> which is the case for 01, 02 and 11 and not for 10.

When a category can fire for more than one reason, extend the substring just far
enough to separate them, and say in the test file why.

## Test 10 is the one demo that asserts an improvement

Every other test here pins behaviour that 1.8.0 already had. Test 10 pins a change:

| Binary | Category |
|---|---|
| `v1.8.0-community` | `PRIMARY_KEY_CHECK` — "fix the primary key", on a view |
| current | `TABLE_EXISTENCE_CHECK` — "only base tables can be archived" |

**So test 10 fails against a 1.8 binary, and that is correct** — an **expected failure**
row in the behavioural baseline, not a regression. It is the only test
in the suite with that property, so a cross-binary run that reports it as a failure
has not found a bug.

See `../README.md` for the file format.
