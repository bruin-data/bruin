# StarRocks

Bruin supports StarRocks as a native SQL data platform through its MySQL-compatible FE query port. This lets you build and materialize StarRocks tables, run data quality checks, and use sensors — in addition to using StarRocks as an [ingestion source or destination](/ingestion/starrocks).

StarRocks needs specific DDL to create tables (a table model, a `DISTRIBUTED BY` clause, `PROPERTIES`, and optionally `PARTITION BY`), which the generic MySQL adapter does not emit — the `starrocks.*` asset types below generate it.

## Connection

Add a StarRocks entry under `connections` in `.bruin.yml`. The same connection is used for both native assets and ingestion.

```yaml
connections:
  starrocks:
    - name: "starrocks-default"
      username: "root"
      host: "starrocks.example.com"
      port: 9030                    # optional, defaults to 9030 (FE MySQL protocol port)
      password: "XXXXXXXXXX"        # optional
      database: "analytics"         # optional
      ssl: "true"                   # optional — "true" or "skip-verify"
```

## StarRocks Assets

### `starrocks.sql`

Executes a materialized StarRocks SQL asset.

```bruin-sql
/* @bruin
name: analytics.example
type: starrocks.sql
materialization:
    type: table
@bruin */

SELECT
    id,
    country,
    name
FROM staging.customers
```

Supported table materialization strategies:

- `create+replace`
- `append`
- `delete+insert`
- `merge`
- `truncate+insert`
- `time_interval`
- `ddl`

View materialization is also supported. For local and single-node StarRocks clusters, Bruin creates StarRocks-managed tables with `PROPERTIES ("replication_num" = "1")`. Atomic replacements (`create+replace`, `delete+insert`, `truncate+insert`, seeds) use StarRocks' `ALTER TABLE ... SWAP WITH ...`.

#### Table layout

Optional StarRocks table layout settings can be declared under `starrocks`:

```yaml
starrocks:
  table_model: primary_key         # duplicate_key | unique_key | primary_key
  distributed_by: [account_id]      # defaults to the key columns
  partition_by: [event_date]        # optional expression partitioning
  buckets: 8                        # defaults to 1
  properties:
    replication_num: "1"
```

When any of these are set (or `columns` are declared), Bruin emits a typed `CREATE TABLE` with the key clause, `PARTITION BY`, `DISTRIBUTED BY HASH(...) BUCKETS`, and `PROPERTIES`. Otherwise it falls back to `CREATE TABLE ... AS SELECT`.

#### Merge materialization

StarRocks has no `MERGE INTO` statement. Bruin implements `merge` with a StarRocks **PRIMARY KEY** table: matching rows are replaced and new rows inserted by a plain `INSERT`. Bruin infers `starrocks.table_model: primary_key` for merge assets, creates the table if it does not exist, and upserts on the primary key.

Merge assets must declare typed `columns` and at least one `primary_key` column. Per-column `merge_sql` expressions are not supported (StarRocks upserts whole rows on the primary key) — encode that logic in the asset query instead.

```bruin-sql
/* @bruin
name: analytics.accounts
type: starrocks.sql
materialization:
    type: table
    strategy: merge

columns:
  - name: account_id
    type: BIGINT
    primary_key: true
  - name: status
    type: VARCHAR(32)
    update_on_merge: true
@bruin */

SELECT account_id, status
FROM staging.accounts
```

### `starrocks.seed`

Loads a local CSV file into StarRocks.

```yaml
name: analytics.seed_contacts
type: starrocks.seed

columns:
  - name: name
    type: STRING
  - name: channel
    type: STRING

parameters:
  path: seed.csv
  file_type: csv
```

### `starrocks.sensor.table`

Waits until a StarRocks table exists.

```yaml
name: analytics.wait_for_daily_summary
type: starrocks.sensor.table
parameters:
    table: analytics.daily_summary
    poke_interval: 30
```

### `starrocks.sensor.query`

Waits until a StarRocks query returns at least one row.

```yaml
name: analytics.wait_for_orders
type: starrocks.sensor.query
parameters:
    query: SELECT 1 FROM analytics.orders WHERE order_date = "{{ end_date }}" LIMIT 1
```

### `starrocks.source`

Defines an existing StarRocks table as a source asset for lineage and documentation.

```yaml
name: analytics.raw_orders
type: starrocks.source

columns:
  - name: order_id
    type: BIGINT
```
