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
| 02 | `FK_COVERAGE_CHECK` | archives `film` while `inventory`, `film_actor` and `film_category` still reference it, so the delete would violate their foreign keys |
| 10 | `TABLE_EXISTENCE_CHECK` | `root_table` is the view `customer_list`; only base tables can be archived — the **non-base-table policy** |
| 11 | `INTERNAL_FK_COVERAGE` | the `customer → rental → payment` GDPR diamond — `payment` has two parents in the job, so one foreign key is never a relation |
| 12 | `SRC_DEST_IDENTITY_CHECK` | `source` and `destination` are the same server and schema (`127.0.0.1:3305/sakila`); refused at connection time, before preflight — an archive into itself would delete the only copy (issue #13); the root `payment` carries an INSERT trigger, so a binary that checked identity after preflight would be refused by the destination trigger check instead — the demo therefore also pins that the guard runs before preflight |
| 18 | `DELETE_TRIGGER_VISIBILITY_CHECK`, naming `film(unconfirmed)` | nothing in the config: it is Test01's `film` tree, run as `ga3e2e_recipe`, which holds exactly the documented source recipe and no `TRIGGER`, so it cannot see `film`'s DELETE trigger and cannot prove there is none; `--force-triggers`, passed as usual, does not apply |
| 19 | `DELETE_TRIGGER_CHECK`, naming `film(del_film)` | the same tree as an account holding `TRIGGER` too, validated **without** `--force-triggers`: Sakila's `del_film` fires on every deleted film, and without the operator's acceptance the job is refused |

Test 01 was a demo in 2.x (`COMPOSITE_PK_CHECK`). 3.0 archives composite keys, so it
is a working archive now (`../archive/01-film-tree.test.sh`).

`mode="example"` selects this arm of the engine. `expected_error` is a **substring** of
the failure output. Three more declarations, accepted only here and covered by the suite's
leakage check like every other, serve tests 18 and 19:

| Declaration | Values | Effect |
|---|---|---|
| `source_account` | `recipe` \| `recipe-trigger` | Before `validate`, the engine creates `ga3e2e_recipe` on the source as root, with exactly the documented source recipe (`SELECT, DELETE` on the source database, `PROCESS`; `recipe-trigger` adds `TRIGGER`) and the estate's root password; after `validate` it drops the account on every path, and a failed drop fails the demo. The config's `source.user` must be `ga3e2e_recipe`, or the test is refused before it runs. |
| `force_triggers` | `yes` (default) \| `no` | `no` runs `validate` without `--force-triggers`. |
| `expected_detail` | a literal string | A second substring the failure must hold, for a category whose subject still needs pinning (`film(del_film)`). Matched literally, unlike `expected_error`. Quote it in the file when it holds parentheses. |

## Choosing `expected_error`: the category alone is sometimes vacuous

Prefer the bare category — the wording can then improve without breaking the test.
**But check first that the category has only one cause.** `TABLE_EXISTENCE_CHECK`
has two: a name that is absent, and a name that is present but is not a base table.
Test 10 therefore asserts `TABLE_EXISTENCE_CHECK: Only base tables`.

This is measured, not cautious. Mutating test 10 to the bare category *and* typo-ing
its `root_table` leaves the test **passing** — goarchive says the table is not
found, the substring matches, and a test whose whole subject is views proves
nothing about views.

> So the older claim here — that matching on the category means "a config that fails
> for a different reason still fails the test" — **is not true in general**, and this
> paragraph replaces it. It holds only where the category maps to a single cause,
> which is the case for 02, 11, 12, 18 and 19 and not for 10.

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
