# Schema Evolution in SQL Assets

SQL assets (`duckdb.sql` and `bq.sql`) build tables inside your warehouse from a `SELECT` query or `columns` metadata. When `schema_evolution` is enabled on an incremental or `ddl` table asset, Bruin inspects the existing table and the incoming schema before executing the write, applies backwards-compatible `ALTER TABLE` changes, and rewrites positional `INSERT` statements to match columns by name.

## Enabling Schema Evolution

Enable `schema_evolution` on an individual SQL asset in its metadata block:

```bruin-sql
/* @bruin
name: analytics.orders
type: bq.sql
materialization:
  type: table
  strategy: delete+insert
  incremental_key: order_date
schema_evolution:
  enabled: true
@bruin */

SELECT
  order_id,
  order_date,
  customer_id,
  CAST(total_amount AS NUMERIC) AS total_amount,
  coupon_code
FROM staging.orders
WHERE order_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
```

Or enable it across all supported SQL table assets in `pipeline.yml` using [`default`](../pipelines/definition.md#default-pipeline-level-defaults), and opt out on specific assets when needed:

```yaml
# pipeline.yml
name: analytics-daily
default:
  schema_evolution:
    enabled: true
```

```bruin-sql
/* @bruin
name: analytics.strict_ledger
type: bq.sql
materialization:
  type: table
  strategy: append
schema_evolution:
  enabled: false
@bruin */

SELECT id, amount FROM staging.ledger
```

When `schema_evolution` is omitted or `enabled: false`, Bruin renders and executes the exact same SQL as before without extra metadata queries.

## How It Works

When `bruin run` executes a SQL asset with `schema_evolution.enabled: true`:

1. **Zero-scan schema inspection:** Bruin inspects the target table's existing columns and constraints (`DESCRIBE` in DuckDB; table metadata API in BigQuery) and inspects the query's output schema without scanning data (`DESCRIBE <query>` in DuckDB; 0-byte query dry-run in BigQuery). If the target table does not exist yet, Bruin skips schema evolution and creates the table normally.
2. **Compatibility planning:** Bruin normalizes aliases and compares each incoming column against the existing table:
   - **New columns** are validated (rejecting `PRIMARY KEY`, `FOREIGN KEY`, unsupported `NOT NULL`, or untyped `NULL` / `[]` columns) and scheduled as `ADD COLUMN`.
   - **Unchanged or implicitly absorbed types** (for example, an existing `BIGINT` column receiving `INT`, or `FLOAT64` receiving `INT64`) require no DDL.
   - **Lossless widenings** (for example, `INT64` to `NUMERIC`, `INT` to `BIGINT`, or `STRING(10)` to `STRING(50)`) are scheduled as `ALTER COLUMN ... SET DATA TYPE` to the narrowest lossless supertype that fits both existing and incoming values.
   - **Incompatible changes** are collected across all columns at once. If any column is incompatible, Bruin aborts before modifying the table and lists every offending column in a single error.
3. **DDL execution and name-matched writes:** Bruin runs the planned `ALTER TABLE` statement(s) (`ALTER COLUMN` widenings first, then `ADD COLUMN` additions) and aligns `INSERT` statements by column name (`INSERT INTO ... BY NAME` on DuckDB; explicit `INSERT INTO table (col1, col2)` column lists on BigQuery) so omitted historical columns and reordered `SELECT` projections succeed without positional column-count errors.

### Validation with `bruin validate`

Running `bruin validate` on a schema-evolution-enabled asset dry-runs the `SELECT` query and checks the planned diff against the live destination table without altering it.

This solves a common CI failure mode: without schema-evolution-aware validation, dry-running `INSERT INTO table ...` or `MERGE table ...` in a pull request fails because the new or widened columns have not been added to production yet. With `schema_evolution.enabled: true`, `bruin validate` passes when the planned evolution is backwards-compatible and fails early in CI if a query introduces an incompatible type change (such as `STRING` to `INT64`) or an untyped `NULL` new column.

## Behavior by Materialization Strategy

Schema evolution adapts where it reads the incoming schema from based on the asset's `materialization.strategy`:

| Strategy | Schema source | Behavior |
| --- | --- | --- |
| `append`, `delete+insert`, `time_interval`, `truncate+insert` | Query output columns | Adds new query columns, widens compatible types, and targets incoming columns by name so omitted table columns stay intact. |
| `merge`, `scd2_by_column`, `scd2_by_time` | Query output types filtered to `columns` | Only columns declared in the asset's `columns` block are added or widened; extra helper columns in `SELECT` are ignored, and a declared column missing from the query fails the run. |
| `ddl`, `datavault_*` | Asset `columns` metadata | Evolves the table directly from declared `columns` types, defaults, and collations without inspecting a `SELECT` query. |
| `create+replace` (or `--full-refresh`, except `ddl`) | _Skipped_ | The table is rebuilt from scratch, so in-place `ALTER TABLE` evolution is not needed. |

### Untyped `NULL` and Empty Array Literals

SQL engines assign arbitrary placeholder types to bare `NULL` and `[]` in `DESCRIBE` and dry-run metadata (for example, both DuckDB and BigQuery report `INT`/`INT64`). Bruin inspects the `SELECT` AST so placeholder types never trigger bogus `INT64` widenings or creations:

- Selecting `NULL AS existing_col` (or `[] AS existing_array_col`) into an existing compatible column is treated as a no-op and leaves the column's type unchanged.
- Selecting bare `NULL` or `[]` as a **new** column fails with an error asking for an explicit `CAST`, such as `CAST(NULL AS STRING)` on BigQuery or `CAST(NULL AS VARCHAR)` on DuckDB.

## Supported Platforms

### Google BigQuery (`bq.sql`)

BigQuery schema evolution supports native BigQuery tables across `append`, `delete+insert`, `time_interval`, `truncate+insert`, `merge`, `scd2_by_column`, `scd2_by_time`, and `ddl` (BigQuery Iceberg tables are not yet supported).

- **Type aliases:** `INT`, `INTEGER`, `SMALLINT`, `TINYINT`, and `BYTEINT` normalize to `INT64`; `FLOAT` normalizes to `FLOAT64`; `BOOLEAN` normalizes to `BOOL`; `DECIMAL(38,9)` normalizes to `NUMERIC`; `BIGDECIMAL` normalizes to `BIGNUMERIC`.
- **Allowed widenings:**
  - `INT64` -> `NUMERIC` -> `BIGNUMERIC` (preserving existing integer digits; for example, `INT64` widened against `NUMERIC(10,2)` becomes `NUMERIC(21,2)` so 19-digit `INT64` values fit, and `NUMERIC` scale increases beyond 9 upgrade to `BIGNUMERIC`).
  - `NUMERIC(p1, s1)` -> `NUMERIC(p2, s2)` and `BIGNUMERIC(p1, s1)` -> `BIGNUMERIC(p2, s2)`.
  - `STRING(n)` -> larger `STRING(m)` -> unbounded `STRING`, and `BYTES(n)` -> larger `BYTES(m)` -> unbounded `BYTES`.
  - `ARRAY<T>` -> `ARRAY<U>` when `T` -> `U` is an allowed widening.
  - Existing `FLOAT64` columns absorb `INT64`, `NUMERIC`, and `BIGNUMERIC` without altering the table; `STRUCT` columns absorb matching `STRUCT` values whose field types normalize or coerce cleanly.
- **Rejected changes:** `INT64` or `NUMERIC` -> `FLOAT64` (lossy above $2^{53}$), `STRING` <-> `INT64`, conversions between `DATE`, `DATETIME`, and `TIMESTAMP`, altering `PRIMARY KEY` columns, and adding `NOT NULL` (`REQUIRED`) columns via `ALTER TABLE`.
- **DDL batching and defaults:** Multiple `ALTER COLUMN ... SET DATA TYPE` and `ADD COLUMN` clauses are combined into a single `ALTER TABLE` statement before the main query runs. Because GoogleSQL forbids inline `DEFAULT` on `ADD COLUMN`, column defaults on `ddl` assets are applied in a follow-up `ALTER TABLE ... ALTER COLUMN ... SET DEFAULT (...)` statement.
- **SCD2 change detection:** On `scd2_by_column`, enabling `schema_evolution` switches non-key comparisons from `!=` to `IS DISTINCT FROM` for groupable types and `FORMAT('%T', ...)` for `ARRAY`, `STRUCT`, `JSON`, and `GEOGRAPHY` columns, so `NULL`-to-value transitions on newly added columns and array updates are versioned accurately.

### DuckDB (`duckdb.sql`) and DuckLake

DuckDB schema evolution supports `append`, `delete+insert`, `time_interval`, `truncate+insert`, `merge`, `scd2_by_column`, `scd2_by_time`, `datavault_*`, and `ddl` (DuckDB Iceberg lakehouse connections are not yet supported).

- **Allowed widenings:**
  - Signed integers: `TINYINT` -> `SMALLINT` -> `INTEGER` -> `BIGINT` -> `HUGEINT`.
  - Unsigned integers: `UTINYINT` -> `USMALLINT` -> `UINTEGER` -> `UBIGINT` -> `UHUGEINT`, plus lossless unsigned-to-signed widening (such as `UINTEGER` -> `BIGINT`).
  - Floating point: `FLOAT` -> `DOUBLE`.
  - Decimals: `DECIMAL(p, s)` precision/scale expansion up to precision 38, and integer -> `DECIMAL(p, s)` when existing integer digits fit.
  - Temporal: `DATE` -> `TIMESTAMP_S` / `TIMESTAMP_MS` -> `TIMESTAMP` -> `TIMESTAMP WITH TIME ZONE` (widened from naive timestamps in `UTC`).
  - Collections: variable lists (`T[]` -> `U[]`) and same-length fixed-size arrays (`T[N]` -> `U[N]`).
  - Absorbed inserts: unbounded `VARCHAR` columns absorb incoming types on insert-based strategies, and `DATE`/`TIMESTAMP`/`UUID`/`JSON`/`ENUM` columns accept `VARCHAR` literals without altering the table.
- **Rejected changes and catalog guards:** Exact numeric types (`INTEGER`, `BIGINT`, `DECIMAL`) are never widened to `FLOAT`/`DOUBLE`, temporal columns are never widened to `TIMESTAMP_NS` (which overflows outside years 1677 to 2262), and columns backed by indexes, `PRIMARY KEY`, `UNIQUE`, `FOREIGN KEY`, or `CHECK` constraints are not altered.
- **DuckDB SCD2 specifics:** Unlike BigQuery's `MERGE`-based SCD2, DuckDB's `scd2_by_column` and `scd2_by_time` rebuild the table via `CREATE OR REPLACE TABLE ... UNION ALL`. Removing an existing column from `columns` on a DuckDB SCD2 asset therefore fails with an error rather than dropping the column, and `scd2_by_column` uses `IS DISTINCT FROM` across non-key columns when `schema_evolution` is enabled.
- **DuckLake limits:** On DuckLake connections, widening is restricted to DuckLake's supported promotions (`TINYINT`..`BIGINT`, `UTINYINT`..`UBIGINT`, `FLOAT` -> `DOUBLE`), new `NOT NULL` columns are rejected, and `ADD COLUMN` defaults must be constant literals.

## Design Tradeoffs

- **Extra metadata calls vs. upfront safety:** Enabling `schema_evolution` adds lightweight metadata and 0-byte dry-run/`DESCRIBE` checks before each incremental write. In exchange, Bruin catches incompatible changes before mutating the table and reports every failing column in one pass rather than failing mid-load on the first mismatched column.
- **Supertype widening vs. exact query type:** When widening an existing `INT64` column to accommodate `NUMERIC(10,2)`, Bruin widens the column to `NUMERIC(21,2)` rather than `NUMERIC(10,2)`. `NUMERIC(10,2)` only holds 8 integer digits and would fail if existing rows contain larger `INT64` values; `NUMERIC(21,2)` holds all 19 `INT64` digits plus 2 decimal places.
- **Transactional DDL vs. pre-DML DDL:** On DuckDB, `ALTER TABLE` and the write query run inside a single SQL transaction (`BEGIN TRANSACTION ... COMMIT`) on a pinned session. Cloud warehouses such as BigQuery forbid `ALTER TABLE` inside `BEGIN TRANSACTION ... COMMIT TRANSACTION`, so Bruin runs `ALTER TABLE` immediately before the DML query. When the asset has pre-hooks, they run first in a BigQuery session that schema inspection, the `ALTER TABLE`, and the main query all join, so variables and temp tables they create stay visible. Because every change is additive or a lossless widening, a committed `ALTER TABLE` followed by a failed DML query leaves existing data intact and retries cleanly on the next run.
- **Explicit column aliases required on `INSERT` strategies:** Because Bruin matches columns by name rather than position (`BY NAME` or injected column lists), computed expressions in `SELECT` must be aliased to match target column names (`SELECT a + b AS total`), and bare `VALUES (...)` queries without column names are rejected on DuckDB.
