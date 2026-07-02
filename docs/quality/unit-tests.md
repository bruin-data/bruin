# Unit Tests

Unit tests pin an asset's transformation logic by running its query against controlled mock inputs and checking the output, with no dependence on production data.

They are the complement to [quality checks](/quality/overview): a quality check validates the *real data an asset produces*, after it runs; a unit test validates the *logic itself*, before it runs, against rows you supply. Checks catch bad data; unit tests catch bad SQL. The two are independent and work well together.

A unit test replaces the upstream tables an asset reads with mock rows, runs the query as a single read-only `SELECT` on the asset's connection, and compares the result to what you expect. Nothing is written to the warehouse, so there is nothing to create or clean up.

## Defining a Unit Test

Tests live in the asset's `@bruin` block under `unit_tests:`. Each test names the input tables the query reads, supplies rows for them, and states what the output should be.

```bruin-sql
/* @bruin
name: analytics.revenue_summary
type: sf.sql
connection: snowflake_default
unit_tests:
  - name: refunds_excluded
    inputs:
      - asset: analytics.orders
        rows:
          - {id: 1, status: paid, amount: 100}
          - {id: 2, status: refunded, amount: 999}
          - {id: 3, status: paid, amount: 50}
    expected:
      rows:
        - {status: paid, revenue: 150}
@bruin */
SELECT status, SUM(amount) AS revenue
FROM analytics.orders
WHERE status = 'paid'
GROUP BY status
```

This mocks `analytics.orders` with those three rows, runs the query on the `snowflake_default` connection, and checks that the only output row is `{status: paid, revenue: 150}`.

## Inputs

Each entry under `inputs:` is a table the query reads (`asset:`) and the rows that stand in for it. Rows are sparse: any column you leave out is `NULL`. When the input is itself a pipeline asset with declared `columns:`, Bruin casts the fixture to those types, so an all-null column still gets the right type instead of the database guessing.

You only mock the tables you care about. A table the query reads but you do not list is replaced with an empty table, so a `LEFT JOIN` to a dimension you are ignoring just contributes no rows. This works as long as that table is a pipeline asset with declared `columns:`; if it has no declared columns, the test errors instead of reading real data.

An incremental asset that reads its own prior output (a query referencing `{{ this }}` for a high-water mark) is no different: after rendering, that target is just another table the query reads, so mock it as an input to set the prior state.

## Shared Fixtures

When several tests feed the same rows to an asset, such as a currency table or a set of reference records, define them once at the pipeline level instead of repeating them in every test.

In `pipeline.yml`:

```yaml
fixtures:
  - name: base_currency
    asset: analytics.currency
    rows:
      - {code: USD, rate: 1.0}
      - {code: EUR, rate: 1.1}
```

A test pulls a fixture in by name:

```yaml
unit_tests:
  - name: converts_eur_to_usd
    fixtures: [base_currency]
    inputs:
      - asset: analytics.orders
        rows:
          - {id: 1, amount: 50, code: EUR}
    expected:
      rows:
        - {id: 1, usd_amount: 55}
```

`base_currency` becomes an input for `analytics.currency`, alongside the orders the test declares for itself. If a test lists its own `inputs:` entry for an asset that a referenced fixture also covers, the test's rows take precedence for that asset.

## Expected Output

`expected:` accepts these keys:

| Key | Description |
|-----|-------------|
| `rows` | The rows the query should return. |
| `count` | The number of rows the query should return. |
| `match` | `subset` (default): every listed row must appear, extra rows are fine. `exact`: the row sets must match exactly. |
| `order` | `any` (default) ignores row order. `strict` requires the rows in the order given. |

`count` and `rows` are independent: set either, or both, and both must hold. Rows can be partial, listing only the columns you want to assert; the rest are ignored.

Comparisons are forgiving about representation. Column names match case-insensitively, numbers compare by value (so `150` matches a warehouse's `"150.00"`), and dates compare by instant regardless of string format.

## Asserting Intermediate CTEs

You can pin a named CTE's output directly under `expected.ctes`, not just the final result, so you can check a step in the middle of a multi-CTE asset on its own. Each entry takes the same `rows`/`count`/`match`/`order` keys as the top-level expectation.

```bruin-sql
/* @bruin
name: analytics.revenue_by_status
type: sf.sql
connection: snowflake_default
unit_tests:
  - name: paid_revenue
    inputs:
      - asset: analytics.orders
        rows:
          - {id: 1, status: paid, amount: 100}
          - {id: 2, status: paid, amount: 50}
          - {id: 3, status: refunded, amount: 999}
    expected:
      ctes:
        paid_orders:
          rows:
            - {id: 1, amount: 100}
            - {id: 2, amount: 50}
      rows:
        - {revenue: 150}
@bruin */
WITH paid_orders AS (
  SELECT id, amount FROM analytics.orders WHERE status = 'paid'
)
SELECT SUM(amount) AS revenue FROM paid_orders
```

Each CTE is checked by running the asset's own `WITH` clause up to that CTE, with the same fixtures injected, as a read-only `SELECT * FROM <cte>`. A test can assert only CTEs, only the final output, or both.

## Freezing Time

If an asset stamps rows with `CURRENT_TIMESTAMP`, `CURRENT_DATE`, or `NOW()`, its output changes on every run and can't be asserted. Set `execution_time` to pin those functions to a fixed value:

```yaml
unit_tests:
  - name: stamps_the_load_time
    execution_time: "2024-01-01 09:00:00"
    expected:
      rows:
        - {loaded_at: "2024-01-01 09:00:00"}
```

`execution_time` also sets the run's execution date for Jinja, so date macros line up with the frozen clock.

## Per-test Variables

Override pipeline variables for a single test with `variables:`:

```yaml
unit_tests:
  - name: high_threshold
    variables:
      min_amount: 1000
    inputs: ...
    expected: ...
```

## Requirements

- The asset is a single SQL statement. A bare `SELECT` works, and so does a `materialization: none` asset that wraps a SELECT in DDL (`CREATE OR REPLACE VIEW … AS SELECT`, `CREATE TABLE … AS SELECT`, `INSERT … SELECT`); the test runs the inner SELECT and never issues the DDL. An asset with no read logic to test (a plain `DELETE`, `UPDATE`, `MERGE`, `TRUNCATE`, or a CTE that writes) is rejected rather than run, so the test stays read-only.
- The pipeline is inside a git repository, the same as `bruin render`.
- The asset's connection is configured in `.bruin.yml`.

## Supported Platforms

Verified on BigQuery, Snowflake, PostgreSQL, and MySQL, with two- and three-part table names. Other SQL connections that can run a query should work too.

## Running

Run unit tests with the [`bruin unit-test`](/commands/unit-test) command. Point it at a single asset, a pipeline, or a whole repo (it defaults to the current directory and tests every pipeline it finds, which is handy in CI). See the command reference for flags and exit behavior.
