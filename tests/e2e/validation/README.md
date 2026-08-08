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

`mode="example"` selects this arm of the engine. The only other variable that
matters is `expected_error`: matching on the *category* rather than the message
means the wording can improve without breaking the test, while a config that fails
for a different reason still fails the test.

See `../README.md` for the file format.
