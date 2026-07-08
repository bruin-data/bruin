# Apache Doris

Bruin supports Apache Doris as a native SQL data platform through Doris' MySQL-compatible query port.

## Connection

Add a Doris entry under `connections` in `.bruin.yml`.

```yaml
connections:
  doris:
    - name: "doris-default"
      username: "root"
      password: "XXXXXXXXXX"
      host: "doris.example.com"
      port: 9030
      database: "analytics"
      driver: "pymysql"                 # optional, defaults to pymysql for ingestr URIs
      ssl_ca_path: "path/to/ca.pem"     # optional
      ssl_cert_path: "path/to/cert.pem" # optional
      ssl_key_path: "path/to/key.pem"   # optional
```

## Doris Assets

### `doris.sql`

Executes a materialized Doris SQL asset.

```bruin-sql
/* @bruin
name: analytics.example
type: doris.sql
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

View materialization is also supported. For local and single-node Doris clusters, Bruin creates Doris-managed tables with `PROPERTIES ("replication_num" = "1")`.

#### Merge materialization

Doris `merge` uses native `MERGE INTO`, so the target table must be a Doris `UNIQUE KEY` table. Bruin infers `doris.table_model: unique_key` for merge assets, creates that table during full refresh, and then uses `MERGE INTO` for incremental runs.

Merge assets must declare typed `columns` and at least one `primary_key` column. Use `update_on_merge` for direct source updates and `merge_sql` for custom matched-row expressions.

```bruin-sql
/* @bruin
name: analytics.accounts
type: doris.sql
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
  - name: update_count
    type: INT
    merge_sql: target.`update_count` + source.`update_count`
@bruin */

SELECT account_id, status, update_count
FROM staging.accounts
```

Optional Doris table layout settings can be declared under `doris`:

```yaml
doris:
  table_model: unique_key
  distributed_by: [account_id]
  buckets: 8
  properties:
    replication_num: "1"
```

### `doris.seed`

Loads a local CSV file into Doris.

```yaml
name: analytics.seed_contacts
type: doris.seed

columns:
  - name: name
    type: STRING
  - name: channel
    type: STRING

parameters:
  path: seed.csv
  file_type: csv
```

### `doris.sensor.table`

Waits until a Doris table exists.

```yaml
name: analytics.wait_for_daily_summary
type: doris.sensor.table
parameters:
    table: analytics.daily_summary
    poke_interval: 30
```

### `doris.sensor.query`

Waits until a Doris query returns at least one row.

```yaml
name: analytics.wait_for_orders
type: doris.sensor.query
parameters:
    query: SELECT 1 FROM analytics.orders WHERE order_date = "{{ end_date }}" LIMIT 1
```

### `doris.source`

Defines an existing Doris table as a source asset for lineage and documentation.

```yaml
name: analytics.raw_orders
type: doris.source

columns:
  - name: order_id
    type: BIGINT
```
