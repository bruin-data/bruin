<script setup>
const materializationFiles = [
  {
    path: '.bruin.yml',
    language: 'yaml',
    content: `default_environment: default
environments:
  default:
    connections:
      duckdb:
        - name: duckdb-default
          path: shop.db`
  },
  {
    path: 'assets/1_full_rebuilds/customers.sql',
    language: 'sql',
    content: `/* @bruin

name: analytics.customers
type: duckdb.sql
description: Rebuilt from the query on every run.

depends:
  - raw.customers

materialization:
  type: table
  strategy: create+replace # default for tables

@bruin */

SELECT customer_id, name, email, country
FROM raw.customers`
  },
  {
    path: 'assets/1_full_rebuilds/order_events.sql',
    language: 'sql',
    content: `/* @bruin

name: landing.order_events
type: duckdb.sql
description: Empty table that another process loads.

materialization:
  type: table
  strategy: ddl

columns:
  - name: event_id
    type: INTEGER
    primary_key: true
  - name: order_id
    type: INTEGER
  - name: event_type
    type: VARCHAR
  - name: received_at
    type: TIMESTAMP

@bruin */`
  },
  {
    path: 'assets/1_full_rebuilds/shop_snapshot.sql',
    language: 'sql',
    content: `/* @bruin

name: analytics.shop_snapshot
type: duckdb.sql
description: Emptied and refilled; the table is kept.

depends:
  - raw.customers
  - raw.orders

materialization:
  type: table
  strategy: truncate+insert

@bruin */

SELECT
    CURRENT_DATE AS snapshot_date,
    (SELECT COUNT(*) FROM raw.customers) AS customers,
    (SELECT COUNT(*) FROM raw.orders) AS orders,
    (SELECT SUM(amount) FROM raw.orders) AS revenue`
  },
  {
    path: 'assets/2_incremental/customer_order_stats.sql',
    language: 'sql',
    content: `/* @bruin

name: analytics.customer_order_stats
type: duckdb.sql
description: Replaces rows for customers who ordered.

depends:
  - raw.orders

materialization:
  type: table
  strategy: delete+insert
  incremental_key: customer_id

@bruin */

SELECT
    customer_id,
    COUNT(*) AS orders,
    SUM(amount) AS lifetime_revenue
FROM raw.orders
WHERE customer_id IN (
    SELECT customer_id
    FROM raw.orders
    WHERE order_date
        BETWEEN '{{ start_date }}' AND '{{ end_date }}'
)
GROUP BY customer_id`
  },
  {
    path: 'assets/2_incremental/customer_profiles.sql',
    language: 'sql',
    content: `/* @bruin

name: analytics.customer_profiles
type: duckdb.sql
description: Upserts customers by primary key.

depends:
  - raw.customers

materialization:
  type: table
  strategy: merge

columns:
  - name: customer_id
    type: INTEGER
    primary_key: true
  - name: email
    type: VARCHAR
    update_on_merge: true
  - name: country
    type: VARCHAR
    update_on_merge: true
  - name: last_updated_at
    type: TIMESTAMP
    merge_sql: >-
      GREATEST(target.last_updated_at,
               source.last_updated_at)

@bruin */

SELECT
    customer_id,
    email,
    country,
    updated_at AS last_updated_at
FROM raw.customers`
  },
  {
    path: 'assets/2_incremental/daily_revenue.sql',
    language: 'sql',
    content: `/* @bruin

name: analytics.daily_revenue
type: duckdb.sql
description: Replaces the days in the run's window.

depends:
  - raw.orders

materialization:
  type: table
  strategy: time_interval
  incremental_key: order_date
  time_granularity: date

@bruin */

SELECT
    order_date,
    COUNT(*) AS orders,
    SUM(amount) AS revenue
FROM raw.orders
WHERE order_date
    BETWEEN '{{ start_date }}' AND '{{ end_date }}'
GROUP BY order_date`
  },
  {
    path: 'assets/2_incremental/page_views.sql',
    language: 'sql',
    content: `/* @bruin

name: analytics.page_views
type: duckdb.sql
description: Appends the events from the run's window.

depends:
  - raw.page_views

materialization:
  type: table
  strategy: append

@bruin */

SELECT event_id, customer_id, page, viewed_at
FROM raw.page_views
WHERE viewed_at
    BETWEEN '{{ start_timestamp }}' AND '{{ end_timestamp }}'`
  },
  {
    path: 'assets/3_history/customers_history.sql',
    language: 'sql',
    content: `/* @bruin

name: dim.customers_history
type: duckdb.sql
description: New version when updated_at moves forward.

depends:
  - raw.customers

materialization:
  type: table
  strategy: scd2_by_time
  incremental_key: updated_at

columns:
  - name: customer_id
    type: INTEGER
    primary_key: true
  - name: email
    type: VARCHAR
  - name: country
    type: VARCHAR
  - name: updated_at
    type: TIMESTAMP

@bruin */

SELECT customer_id, email, country, updated_at
FROM raw.customers`
  },
  {
    path: 'assets/3_history/products_history.sql',
    language: 'sql',
    content: `/* @bruin

name: dim.products_history
type: duckdb.sql
description: New version when any column changes.

depends:
  - raw.products

materialization:
  type: table
  strategy: scd2_by_column

columns:
  - name: product_id
    type: INTEGER
    primary_key: true
  - name: product_name
    type: VARCHAR
  - name: price
    type: DOUBLE
  - name: stock
    type: INTEGER

@bruin */

SELECT product_id, product_name, price, stock
FROM raw.products`
  },
  {
    path: 'assets/4_data_vault/hub_customer.sql',
    language: 'sql',
    content: `/* @bruin

name: rdv.hub_customer
type: duckdb.sql
description: One row per customer business key.

depends:
  - raw.customers

materialization:
  type: table
  strategy: datavault_hub

columns:
  - name: customer_hk
    type: VARCHAR
    meta:
      datavault_role: hash_key
  - name: customer_id
    type: VARCHAR
    meta:
      datavault_role: business_key
  - name: load_dts
    type: TIMESTAMP
    meta:
      datavault_role: load_datetime
  - name: record_source
    type: VARCHAR
    meta:
      datavault_role: record_source

@bruin */

SELECT
    md5(customer_id::VARCHAR) AS customer_hk,
    customer_id::VARCHAR AS customer_id,
    updated_at AS load_dts,
    'raw.customers' AS record_source
FROM raw.customers`
  },
  {
    path: 'assets/4_data_vault/link_customer_order.sql',
    language: 'sql',
    content: `/* @bruin

name: rdv.link_customer_order
type: duckdb.sql
description: One row per customer-order pair.

depends:
  - raw.orders

materialization:
  type: table
  strategy: datavault_link

columns:
  - name: customer_order_hk
    type: VARCHAR
    meta:
      datavault_role: link_hash_key
  - name: customer_hk
    type: VARCHAR
    meta:
      datavault_role: hub_hash_key
  - name: order_hk
    type: VARCHAR
    meta:
      datavault_role: hub_hash_key
  - name: load_dts
    type: TIMESTAMP
    meta:
      datavault_role: load_datetime
  - name: record_source
    type: VARCHAR
    meta:
      datavault_role: record_source

@bruin */

SELECT
    md5(concat_ws('|', customer_id, order_id))
        AS customer_order_hk,
    md5(customer_id::VARCHAR) AS customer_hk,
    md5(order_id::VARCHAR) AS order_hk,
    CAST(order_date AS TIMESTAMP) AS load_dts,
    'raw.orders' AS record_source
FROM raw.orders`
  },
  {
    path: 'assets/4_data_vault/sat_customer_details.sql',
    language: 'sql',
    content: `/* @bruin

name: rdv.sat_customer_details
type: duckdb.sql
description: History of each customer's attributes.

depends:
  - raw.customers

materialization:
  type: table
  strategy: datavault_satellite

columns:
  - name: customer_hk
    type: VARCHAR
    meta:
      datavault_role: parent_hash_key
  - name: hashdiff
    type: VARCHAR
    meta:
      datavault_role: hashdiff
  - name: load_dts
    type: TIMESTAMP
    meta:
      datavault_role: load_datetime
  - name: record_source
    type: VARCHAR
    meta:
      datavault_role: record_source
  - name: customer_name
    type: VARCHAR
  - name: email
    type: VARCHAR
  - name: country
    type: VARCHAR

@bruin */

SELECT
    md5(customer_id::VARCHAR) AS customer_hk,
    md5(concat_ws('|', name, email, country)) AS hashdiff,
    updated_at AS load_dts,
    'raw.customers' AS record_source,
    name AS customer_name,
    email,
    country
FROM raw.customers`
  },
  {
    path: 'assets/5_views/customer_orders.sql',
    language: 'sql',
    content: `/* @bruin

name: analytics.customer_orders
type: duckdb.sql
description: A view; no data is stored.

depends:
  - analytics.customers
  - raw.orders

materialization:
  type: view

@bruin */

SELECT
    c.customer_id,
    c.name,
    o.order_id,
    o.order_date,
    o.amount
FROM analytics.customers AS c
JOIN raw.orders AS o USING (customer_id)`
  },
  {
    path: 'assets/raw/customers.asset.yml',
    language: 'yaml',
    content: `name: raw.customers
type: duckdb.seed

parameters:
  path: customers.csv

columns:
  - name: customer_id
    type: integer
  - name: name
    type: varchar
  - name: email
    type: varchar
  - name: country
    type: varchar
  - name: updated_at
    type: timestamp`
  },
  {
    path: 'assets/raw/customers.csv',
    language: 'text',
    content: `customer_id,name,email,country,updated_at
1,Ada Lovelace,ada@example.com,UK,2024-06-01 09:00:00
2,Alan Turing,alan@example.com,UK,2024-06-01 10:30:00
3,Grace Hopper,grace@example.com,US,2024-06-02 08:15:00
4,Edsger Dijkstra,edsger@example.com,NL,2024-06-03 14:45:00`
  },
  {
    path: 'assets/raw/orders.asset.yml',
    language: 'yaml',
    content: `name: raw.orders
type: duckdb.seed

parameters:
  path: orders.csv

columns:
  - name: order_id
    type: integer
  - name: customer_id
    type: integer
  - name: order_date
    type: date
  - name: amount
    type: double
  - name: status
    type: varchar`
  },
  {
    path: 'assets/raw/orders.csv',
    language: 'text',
    content: `order_id,customer_id,order_date,amount,status
101,1,2024-06-01,120.00,shipped
102,2,2024-06-01,75.50,shipped
103,1,2024-06-02,42.00,pending
104,3,2024-06-02,310.25,shipped
105,4,2024-06-03,18.99,cancelled
106,3,2024-06-03,99.00,pending`
  },
  {
    path: 'assets/raw/page_views.asset.yml',
    language: 'yaml',
    content: `name: raw.page_views
type: duckdb.seed

parameters:
  path: page_views.csv

columns:
  - name: event_id
    type: integer
  - name: customer_id
    type: integer
  - name: page
    type: varchar
  - name: viewed_at
    type: timestamp`
  },
  {
    path: 'assets/raw/page_views.csv',
    language: 'text',
    content: `event_id,customer_id,page,viewed_at
1,1,/home,2024-06-01 09:01:00
2,1,/products/42,2024-06-01 09:03:10
3,2,/home,2024-06-02 11:20:00
4,3,/checkout,2024-06-02 18:45:30
5,4,/home,2024-06-03 07:12:00`
  },
  {
    path: 'assets/raw/products.asset.yml',
    language: 'yaml',
    content: `name: raw.products
type: duckdb.seed

parameters:
  path: products.csv

columns:
  - name: product_id
    type: integer
  - name: product_name
    type: varchar
  - name: price
    type: double
  - name: stock
    type: integer
  - name: updated_at
    type: date`
  },
  {
    path: 'assets/raw/products.csv',
    language: 'text',
    content: `product_id,product_name,price,stock,updated_at
1,Wireless Mouse,29.99,120,2024-06-01
2,USB Cable,12.99,300,2024-06-01
3,Keyboard,89.99,45,2024-06-02`
  },
  {
    path: 'pipeline.yml',
    language: 'yaml',
    content: `name: materialization_examples
schedule: daily
start_date: "2024-06-01"

default_connections:
  duckdb: duckdb-default`
  }
]
</script>

# Materialization

Materialization turns a plain `SELECT` query into a table or view. You write the query, and Bruin generates the `CREATE`, `INSERT`, `DELETE`, or `MERGE` statements needed to store its result in the destination.

```bruin-sql
/* @bruin

name: analytics.daily_revenue
type: bq.sql

materialization:
  type: table # [!code focus]

@bruin */

SELECT order_date, SUM(amount) AS revenue
FROM raw.orders
GROUP BY order_date
```

Each run rebuilds `analytics.daily_revenue` from the query result. Set a [`strategy`](#strategies) to load the table incrementally or keep its history, or use `type: view` to store the query as a view instead.

> [!TIP]
> Run `bruin render path/to/asset.sql` to print the exact SQL Bruin will execute for an asset, and add `--full-refresh` to see the full-refresh version.

## Overview

### Materialization types

| `type` | What Bruin builds | Strategies |
| --- | --- | --- |
| *(not set)* | Nothing. Bruin runs the SQL as written, so the asset must create or write to its own table. | None |
| `view` | A view over the query. Bruin runs `CREATE OR REPLACE VIEW` on most platforms, `CREATE OR ALTER VIEW` on MSSQL and Fabric, and drops and recreates the view on Synapse, Doris, and StarRocks. | None. Setting `strategy`, `incremental_key`, `incremental_predicate`, `partition_by`, or `cluster_by` on a view is a validation error. |
| `table` | A physical table loaded with the selected `strategy`. | All strategies below. Defaults to `create+replace`. |

### Strategies at a glance

| Strategy | What each run does | Requires |
| --- | --- | --- |
| **Full rebuilds** | | |
| [`create+replace`](#create-replace) *(default)* | Rebuilds the table from the query result. | — |
| [`truncate+insert`](#truncate-insert) | Empties the existing table, then inserts the query result. | — |
| [`ddl`](#ddl) | Creates an empty table from the declared `columns`. There is no query. | `columns` |
| **Incremental loads** | | |
| [`append`](#append) | Inserts the query result. Never updates or deletes. | — |
| [`delete+insert`](#delete-insert) | Deletes rows whose `incremental_key` value appears in the new result, then inserts the result. | `incremental_key` |
| [`merge`](#merge) | Updates rows that match on the primary key and inserts the rest. | `primary_key` |
| [`time_interval`](#time-interval) | Deletes rows inside the run's date range, then inserts the result. | `incremental_key`, `time_granularity` |
| **History tracking** | | |
| [`scd2_by_column`](#scd2-by-column) | Adds a new version when any non-key column changes. | `primary_key` |
| [`scd2_by_time`](#scd2-by-time) | Adds a new version when the `incremental_key` timestamp moves forward. | `primary_key`, `incremental_key` |
| **Data Vault** | | |
| [`datavault_hub`](#datavault-hub) | Inserts business keys that are not yet in the hub. | Column roles |
| [`datavault_link`](#datavault-link) | Inserts relationships that are not yet in the link. | Column roles |
| [`datavault_satellite`](#datavault-satellite) | Inserts changed descriptive rows. | Column roles |

Not every platform supports every strategy. Check the [platform support matrix](#platform-support) before you choose one.

### Choosing a strategy

| If you need to… | Use |
| --- | --- |
| Rebuild the table from scratch on every run | `create+replace` |
| Rebuild from scratch but keep the existing table object and its permissions | `truncate+insert` |
| Add new rows that never change | `append` |
| Reprocess a date range, including backfills | `time_interval` |
| Replace every row belonging to a batch key, such as `load_id` or `country` | `delete+insert` |
| Insert new records and update existing ones by primary key | `merge` |
| Keep every historical version of a record | `scd2_by_column` or `scd2_by_time` |
| Create an empty table that something else fills | `ddl` |
| Expose the query without storing data | `type: view` |

### Explore an example project

The project below has one asset for each strategy, built on DuckDB around a small shop dataset. Click through the files to compare their `materialization` blocks.

<CodeViewer :files="materializationFiles" title="materialization-examples/" default-file="assets/1_full_rebuilds/customers.sql" :collapsed-folders="['assets/raw']" />

| Folder | What it shows |
| --- | --- |
| `assets/raw/` | CSV [seed](./seed.md) assets that load the source tables. |
| `assets/1_full_rebuilds/` | `create+replace`, `truncate+insert`, and `ddl`. |
| `assets/2_incremental/` | `append`, `delete+insert`, `merge`, and `time_interval`. |
| `assets/3_history/` | `scd2_by_column` and `scd2_by_time`. |
| `assets/4_data_vault/` | A hub, a link, and a satellite. |
| `assets/5_views/` | A `type: view` asset. |

To run it, copy the files into a new folder, run `git init` there, and load the first two days with a full refresh. Then run the third day incrementally:

```bash
# Days 1 and 2: create every table
bruin run --full-refresh \
  --start-date 2024-06-01 --end-date "2024-06-02 23:59:59.999999" .

# Day 3: load incrementally
bruin run --start-date 2024-06-03 \
  --end-date "2024-06-03 23:59:59.999999" .
```

The second run adds June 3 to the date-windowed tables, such as `analytics.page_views` and `analytics.daily_revenue`, without touching earlier days. To watch the SCD2 tables add a new version, change a price in `products.csv`, or a country together with its `updated_at` in `customers.csv`, and run again.

## Configuration reference

All materialization settings live under the top-level `materialization` key of the asset definition:

```yaml
materialization:
  type: table
  strategy: delete+insert
  incremental_key: dt
  partition_by: dt
  cluster_by:
    - dt
    - user_id
```

| Key | Type | Description |
| --- | --- | --- |
| `type` | `table` \| `view` | What to build. When omitted, Bruin runs the query as-is. |
| `strategy` | String | How Bruin loads a table. One of the [strategies](#strategies). **Default:** `create+replace`. |
| `incremental_key` | String | Column used to decide which rows to replace or how to version them. Required by `delete+insert`, `time_interval`, and `scd2_by_time`; optional for `scd2_by_column`. Setting it with any other explicit `strategy` is a validation error. |
| `time_granularity` | `date` \| `timestamp` | Whether the `time_interval` window is applied to a `DATE` or a `TIMESTAMP` column. Required by `time_interval`. |
| `incremental_predicate` | String | Extra SQL condition that limits which destination rows a `merge` considers. Only valid with `merge`. See [`incremental_predicate`](#incremental-predicate). |
| `partition_by` | String | Column or expression used to partition the table. See [Partitioning and clustering](#partitioning-and-clustering). |
| `cluster_by` | String[] | Columns used to cluster or sort the table. See [Partitioning and clustering](#partitioning-and-clustering). |

Some strategies also read column-level settings from the asset's [`columns`](./columns.md):

| Column key | Used by | Effect |
| --- | --- | --- |
| `primary_key: true` | `merge`, SCD2, Data Vault, `ddl` | Identifies a row. Required by `merge` and both SCD2 strategies. |
| `update_on_merge: true` | `merge` | When a row matches, overwrite this column with the new value. |
| `merge_sql: <expression>` | `merge` | When a row matches, set this column to a custom expression such as `GREATEST(target.score, source.score)`. Takes precedence over `update_on_merge`. |
| `meta.datavault_role` | Data Vault | Marks a column's [Data Vault role](#data-vault-column-roles). |

### Pipeline defaults

To share a materialization across many assets, set it once under `default.materialization` in `pipeline.yml`:

```yaml
# pipeline.yml
default:
  materialization:
    type: table
    strategy: create+replace
```

Defaults fill in every field an asset leaves empty, and an asset can override a default but cannot remove it. They also apply to every asset in the pipeline, not only SQL tables: with the defaults above, `type: view` assets fail validation because they inherit `strategy`, and seed assets fail because they inherit `type`. Python assets also inherit `type: table`, which turns on [Python materialization](./python.md#materialization), so each one needs a `connection`. Only set `default.materialization` when it fits every asset in the pipeline.

### Partitioning and clustering

`partition_by` and `cluster_by` are applied when Bruin creates the table: during `create+replace`, `ddl`, full refreshes, and the first run of strategies that create a missing table. Most incremental runs write into the existing table and keep its layout. On Doris and StarRocks, `delete+insert` and `truncate+insert` swap in a newly created table that does not keep your partitioning or distribution.

| Platform | `partition_by` | `cluster_by` |
| --- | --- | --- |
| BigQuery | `PARTITION BY` | `CLUSTER BY` |
| Snowflake | — | `CLUSTER BY` |
| Databricks | `ddl` only | `ddl` only |
| Spark | `PARTITIONED BY` | `WRITE ORDERED BY` |
| Athena | Iceberg partitioning | — |
| Trino | `partitioning` property | — |
| ClickHouse | `PARTITION BY` | Sorting key |
| StarRocks | `PARTITION BY` | `DISTRIBUTED BY HASH` |
| MSSQL, Synapse, Vertica | — | Error on `create+replace` |

— means the key is ignored. Both keys are also ignored on PostgreSQL, Redshift, DuckDB, MySQL, Oracle, Dremio, Sail, Fabric, and Doris.

- **Databricks:** both keys only take effect with `ddl`. `create+replace` fails if `cluster_by` is set and ignores `partition_by`.
- **MSSQL, Synapse, Vertica:** `cluster_by` fails with `create+replace` and full refreshes, and is ignored by other strategies.
- **ClickHouse:** `partition_by` accepts an expression such as `toYYYYMM(created_at)`, and `cluster_by` becomes the `ORDER BY` sorting key. See [ClickHouse table options](#clickhouse-table-options).
- **Doris:** set the layout in the `doris` block, for example `doris.distributed_by`.

BigQuery-specific options such as `require_partition_filter` live in a separate `bigquery` block. See [BigQuery table options](../platforms/bigquery.md#bigquery-table-options).

## Strategies

Strategies fall into four groups:

- **Full rebuilds** (`create+replace`, `truncate+insert`, `ddl`) recreate or reset the table on each run.
- **Incremental loads** (`append`, `delete+insert`, `merge`, `time_interval`) change only part of an existing table.
- **History tracking** (`scd2_by_column`, `scd2_by_time`) keep every version of each record.
- **Data Vault** (`datavault_hub`, `datavault_link`, `datavault_satellite`) load a Raw Data Vault.

> [!IMPORTANT]
> Incremental strategies write into an existing table, and on most platforms they do not create it. For a new asset, do the first run with `--full-refresh` so Bruin creates the table with `create+replace`, then let later runs load it incrementally. The Data Vault strategies, ClickHouse SCD2, and StarRocks `merge` create the target automatically.

### Full rebuilds

#### `create+replace` {#create-replace}

Rebuilds the table from the query result on every run. This is the default for `type: table`.

**Use it when** the table is cheap enough to recompute every time and you want the simplest, always-correct result.

```bruin-sql
/* @bruin

name: analytics.customers
type: bq.sql

materialization:
  type: table
  strategy: create+replace # optional, this is the default

@bruin */

SELECT customer_id, email, country
FROM raw.customers
```

**How it works:** Bruin runs `CREATE OR REPLACE TABLE analytics.customers AS <query>`, or the platform's equivalent drop-and-create.

**Good to know:**

- Cost and run time grow with the size of the full result, because nothing is reused between runs.
- Recreating the table can reset grants and other table-level settings on some platforms. Use `truncate+insert` if you need to keep them.

#### `truncate+insert` {#truncate-insert}

Removes every row from the existing table, then inserts the query result. On most platforms the table object itself, with its schema, permissions, and indexes, is kept.

**Use it when** you want a full refresh but other users or tools depend on the table object staying in place.

```bruin-sql
/* @bruin

name: analytics.daily_snapshot
type: bq.sql

materialization:
  type: table
  strategy: truncate+insert

@bruin */

SELECT
    CURRENT_DATE() AS snapshot_date,
    COUNT(*) AS total_users,
    SUM(revenue) AS total_revenue
FROM raw.users
```

**How it works:** `TRUNCATE TABLE analytics.daily_snapshot`, then `INSERT INTO analytics.daily_snapshot <query>`. Athena and Trino use `DELETE FROM` instead of `TRUNCATE`.

**Good to know:**

- `TRUNCATE` is usually faster than `DELETE` for removing every row, and no `incremental_key` is needed.
- The table must already exist. Create it on the first run with `--full-refresh`.
- Bruin wraps both statements in a transaction where the platform allows it. On BigQuery, a failed insert rolls the truncate back, and readers never see the table empty. On engines where `TRUNCATE` commits implicitly, such as MySQL, Redshift, and Oracle, a failed insert can still leave the table empty.
- Doris and StarRocks do not truncate. They build a new table from the query and swap it in, so the original table object and its layout are not kept.

#### `ddl`

Creates an empty table from the `columns` defined in the asset. A `ddl` asset has no query.

**Use it when** you need a table with an exact schema that another process, such as an ingestion job or a Python asset, loads.

```bruin-sql
/* @bruin

name: raw.products
type: bq.sql

materialization:
  type: table
  strategy: ddl
  partition_by: product_category

columns:
  - name: product_id
    type: INTEGER
    description: "Unique identifier for the product"
    primary_key: true
  - name: product_category
    type: VARCHAR
    description: "Category of the product"
  - name: product_name
    type: VARCHAR
    description: "Name of the product"
  - name: price
    type: FLOAT
    description: "Price of the product in USD"

@bruin */
```

**How it works:** Bruin runs `CREATE TABLE IF NOT EXISTS raw.products (...)` using the column names, types, primary key, and descriptions. `partition_by` and `cluster_by` are applied where the platform supports them.

**Good to know:**

- Adding a query after the `@bruin` block is a validation error.
- The table is created once and never dropped, even with `--full-refresh`.
- Changing `columns` later does not alter an existing table. Migrate it yourself.

### Incremental loads

#### `append`

Inserts the query result into the table. Existing rows are never updated or deleted.

**Use it when** each run produces rows that are new by definition, such as events or audit logs.

```bruin-sql
/* @bruin

name: analytics.page_views
type: bq.sql

materialization:
  type: table
  strategy: append

@bruin */

SELECT event_id, user_id, page, viewed_at
FROM raw.page_views
WHERE viewed_at >= '{{ start_timestamp }}'
  AND viewed_at <= '{{ end_timestamp }}'
```

**How it works:** `INSERT INTO analytics.page_views <query>`.

**Good to know:**

- Bruin does not deduplicate. Filter the query to only new rows, typically with the [run's date variables](../variables/built-in.md). Running the same window twice inserts the rows twice.
- The example includes both ends of the window, so successive runs must not share a boundary instant. Bruin's default daily windows end at `23:59:59.999999` and do not overlap. For backfills, end each range just before the next one starts, for example `--end-date "2024-03-30 23:59:59.999999"` followed by `--start-date "2024-03-31"`.
- If you need reruns to be safe, use `time_interval` or `delete+insert` instead.

#### `delete+insert` {#delete-insert}

Replaces every row that shares an `incremental_key` value with the new query result.

**Use it when** data arrives in batches identified by a key, such as a date, a load ID, or a tenant, and a new batch should fully replace the old one.

```bruin-sql
/* @bruin

name: analytics.daily_orders
type: bq.sql

materialization:
  type: table
  strategy: delete+insert
  incremental_key: order_date

@bruin */

SELECT order_date, customer_id, COUNT(*) AS orders
FROM raw.orders
WHERE order_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
GROUP BY order_date, customer_id
```

**How it works:** on most platforms, Bruin:

1. Runs the query and stores the result in a temporary table.
2. Finds the distinct `incremental_key` values in that result.
3. Deletes every target row with one of those values.
4. Inserts the temporary table into the target.

Some platforms do this differently. See [platform notes](#platform-notes).

**Good to know:**

- Only keys present in the new result are touched. If the result contains no rows for `2024-03-01`, the existing rows for that date stay.
- Because the batch is deleted and reinserted, rows that disappeared from the source within a batch are removed from the target. `merge` cannot do that.
- On Fabric, `delete+insert` matches on the primary key columns. `incremental_key` is still required by validation but is not used.

#### `merge`

Upserts the query result into the table by primary key. Matching rows are updated and new rows are inserted.

**Use it when** records are identified by a stable key and their attributes change over time, such as customers, orders, or subscriptions.

Mark the key with `primary_key: true` and tell Bruin which columns to update when a row matches:

- `update_on_merge: true` overwrites the column with the new value.
- `merge_sql: <expression>` sets the column to a custom expression. Use `target.<column>` for the existing value and `source.<column>` for the new one.

```bruin-sql
/* @bruin

name: analytics.players
type: bq.sql

materialization:
  type: table
  strategy: merge

columns:
  - name: player_id
    type: integer
    primary_key: true
  - name: username
    type: string
    update_on_merge: true
  - name: high_score
    type: integer
    merge_sql: GREATEST(target.high_score, source.high_score)

@bruin */

SELECT player_id, username, high_score
FROM raw.player_scores
```

**How it works:** on platforms with a native `MERGE`, Bruin generates a statement equivalent to:

```sql
MERGE analytics.players AS target
USING (<query>) AS source
ON source.player_id = target.player_id
WHEN MATCHED THEN UPDATE SET
  target.username = source.username,
  target.high_score = GREATEST(target.high_score, source.high_score)
WHEN NOT MATCHED THEN INSERT (player_id, username, high_score)
  VALUES (source.player_id, source.username, source.high_score);
```

**Good to know:**

- Without any `update_on_merge` or `merge_sql` column, most platforms only insert new keys and leave matched rows untouched. MySQL, ClickHouse, Fabric, and StarRocks behave differently: they replace matched rows entirely, so `update_on_merge` has no effect there. See [platform notes](#platform-notes).
- `merge` never deletes. Rows removed from the source stay in the target. Use `delete+insert` if deletions must propagate.
- `incremental_key` cannot be combined with `merge`. To limit the rows read from the source, filter the query. To limit the destination rows scanned, use `incremental_predicate`.
- `merge_sql` is supported on BigQuery, Snowflake, PostgreSQL, Redshift, MySQL, MSSQL, Oracle, Athena (Iceberg), DuckDB, Spark, Doris, and Vertica. Databricks, Synapse, ClickHouse, and Fabric ignore it, and StarRocks rejects it.
- Spark `merge` needs a catalog and table format that implement row-level `MERGE INTO`, such as Iceberg with the Spark SQL extensions enabled.

##### `incremental_predicate` {#incremental-predicate}

`incremental_predicate` adds a boolean SQL expression to the merge's match condition. It limits the destination rows considered for a match, which can greatly reduce the partitions a warehouse such as BigQuery has to scan.

The expression can reference two aliases:

| Alias | Refers to |
| --- | --- |
| `source` | The rows returned by the asset query for the current run. |
| `target` | The existing rows in the destination table. |

Provide only the expression, without a leading `WHERE` or `AND` and without a trailing semicolon.

The following BigQuery asset reads seven days from its source and restricts matching to the same seven days of the partitioned destination:

```bruin-sql
/* @bruin

name: analytics.events
type: bq.sql

materialization:
  type: table
  strategy: merge
  partition_by: event_date
  incremental_predicate: target.event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) # [!code focus]

columns:
  - name: event_id
    type: integer
    primary_key: true
  - name: event_date
    type: date
  - name: payload
    type: string
    update_on_merge: true

@bruin */

SELECT event_id, event_date, payload
FROM raw.events
WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) -- [!code focus]
```

Bruin adds the predicate to the primary-key comparison. The relevant part of the generated merge is equivalent to:

```sql
MERGE analytics.events AS target -- [!code focus]
USING (
  SELECT event_id, event_date, payload
  FROM raw.events
  WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
) AS source -- [!code focus]
ON (
  (source.event_id = target.event_id OR (source.event_id IS NULL AND target.event_id IS NULL))
  AND (target.event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) -- [!code focus]
)
WHEN MATCHED THEN
  UPDATE SET target.payload = source.payload
WHEN NOT MATCHED THEN
  INSERT (event_id, event_date, payload)
  VALUES (source.event_id, source.event_date, source.payload);
```

In one BigQuery benchmark, merging a 5,000-row source into a 1.8-million-row destination processed 1.92 GB without the predicate and 10.5 MB with the seven-day predicate above: 99.45% fewer bytes processed, 98.91% fewer bytes billed, and 93.88% less slot usage. Actual savings depend on the destination size, its partitioning, and the window you choose.

**Good to know:**

- The predicate does not filter the asset query. Filter the query separately.
- The predicate is inserted as-is, without validation. It must cover every destination row that could match the source data. A row outside the window cannot match, so its key is inserted again as a duplicate. Choose a window that covers late-arriving data and the full period in which existing rows can change.
- Supported on BigQuery, Athena, Databricks, Doris, DuckDB, MSSQL, MySQL, Oracle, PostgreSQL, Snowflake, Spark, Synapse, and Vertica.
- On ClickHouse, the predicate is added to the `DELETE` that removes matched rows, so it must reference the target's columns directly, without the `target.` or `source.` aliases.
- Redshift and Fabric reject it. Redshift's `MERGE` only accepts equality predicates in its match condition. StarRocks ignores it, and Python and ingestr assets fail validation.

#### `time_interval` {#time-interval}

Replaces the rows inside the run's time window with the query result.

**Use it when** the table is organized by date or timestamp and you want idempotent daily or hourly loads and easy backfills.

```bruin-sql
/* @bruin

name: analytics.product_stock
type: bq.sql

materialization:
  type: table
  strategy: time_interval
  incremental_key: dt
  time_granularity: date

columns:
  - name: product_id
    type: INTEGER
    primary_key: true
  - name: stock
    type: INTEGER
  - name: dt
    type: DATE

@bruin */

SELECT product_id, stock, dt
FROM raw.inventory
WHERE dt BETWEEN '{{ start_date }}' AND '{{ end_date }}'
```

**How it works:** Bruin runs:

```sql
DELETE FROM analytics.product_stock
WHERE dt BETWEEN '{{ start_date }}' AND '{{ end_date }}';

INSERT INTO analytics.product_stock <query>;
```

With `time_granularity: timestamp`, the delete uses `{{ start_timestamp }}` and `{{ end_timestamp }}` instead.

The window comes from the run's start and end dates. By default, that is yesterday, from `00:00:00.000000` to `23:59:59.999999`. Pass a different range to backfill:

```bash
bruin run --start-date "2024-03-01" --end-date "2024-03-31" path/to/asset.sql
```

A date-only `--end-date` means midnight at the start of that day. That is enough for `time_granularity: date`, but with `timestamp` it leaves out almost all of the last day. For timestamp backfills, pass the end time explicitly, for example `--end-date "2024-03-31 23:59:59.999999"`.

**Good to know:**

- Bruin deletes the window but does not filter your query. Filter the query to the same window with the [date variables](../variables/built-in.md), or rows outside the window are inserted next to the existing ones.
- Use `date` for a `DATE` column and `timestamp` for a `TIMESTAMP` column.
- `BETWEEN` includes both ends of the window.
- [Interval modifiers](./interval-modifiers.md) can widen the window, for example to reprocess late-arriving data. On the CLI they apply only with `--apply-interval-modifiers`; Bruin Cloud applies them automatically.
- BigQuery, Snowflake, PostgreSQL, Redshift, DuckDB, MySQL, MSSQL, Vertica, and Oracle run the delete and insert in one transaction. Other platforms run them as separate statements, so a failed insert can leave the window empty until you rerun.

### History tracking (SCD2)

The SCD2 strategies implement [Slowly Changing Dimension Type 2](https://en.wikipedia.org/wiki/Slowly_changing_dimension). Instead of overwriting a changed record, Bruin closes the old version and inserts a new one, so the table keeps every version.

|  | `scd2_by_column` | `scd2_by_time` |
| --- | --- | --- |
| **Detects a change when** | Any non-key column declared in `columns` differs from the current version | The `incremental_key` value is newer than the current version |
| **`_valid_from` comes from** | `CURRENT_TIMESTAMP()`, or the `incremental_key` if you set one | The `incremental_key` |
| **Requires** | A `primary_key` | A `primary_key` and an `incremental_key` column declared as `DATE` or `TIMESTAMP` |
| **Use it when** | The source has no reliable change timestamp | The source records when each row changed, such as `updated_at` |

Both strategies:

- Add three columns: `_valid_from` and `_valid_until` hold when a version became active and inactive, and `_is_current` is `true` for the current version.
- Set `_valid_until` to `9999-12-31` for current versions: midnight on BigQuery, PostgreSQL, Redshift, Databricks, and Spark, and `23:59:59` elsewhere.
- Close the current version of records that disappeared from the source.
- Reserve the names `_valid_from`, `_valid_until`, and `_is_current`. Using them in `columns` is a validation error.
- Need a first run with `--full-refresh` to create the table, except on ClickHouse.
- Are supported on BigQuery, Snowflake, PostgreSQL (17 or later), Redshift, MySQL, DuckDB, ClickHouse, Databricks, Athena, and Spark. Oracle supports `scd2_by_time` only.

Unless you set `partition_by` or `cluster_by`, Bruin picks a layout for the SCD2 table:

| Platform | Default layout |
| --- | --- |
| BigQuery | Partitioned by `DATE(_valid_from)`, clustered by `_is_current` and the primary key |
| Spark | Partitioned by `days(_valid_from)`, written in order of `_is_current` and the primary key |
| Snowflake | Clustered by `_is_current` and the primary key |
| Athena | Partitioned by `_valid_from` on a full refresh only. Incremental runs keep only a `partition_by` you set. |

**Good to know:**

- Declare every column of the query in `columns`. Only declared columns are compared for changes and carried into new versions.
- On BigQuery, Snowflake, PostgreSQL, Redshift, DuckDB, Athena, and Databricks, a change from or to `NULL` is not detected as a change.
- On Oracle, the columns are named `bruin_valid_from`, `bruin_valid_until`, and `bruin_is_current` (`1` or `0`), because Oracle identifiers cannot start with an underscore.

> [!WARNING]
> A `--full-refresh` rebuilds an SCD2 table from the current source snapshot and discards its history. Set [`full_refresh_restricted: true`](#full-refresh-and-full-refresh-restricted) on SCD2 assets to protect them.

#### `scd2_by_column` {#scd2-by-column}

Creates a new version of a record whenever any non-primary-key column changes.

```bruin-sql
/* @bruin

name: dim.product_catalog
type: bq.sql

materialization:
  type: table
  strategy: scd2_by_column

columns:
  - name: ID
    type: INTEGER
    description: "Unique identifier for Product"
    primary_key: true
  - name: Name
    type: VARCHAR
    description: "Name of the Product"
  - name: Price
    type: FLOAT
    description: "Price of the Product"

@bruin */

SELECT 1 AS ID, 'Wireless Mouse' AS Name, 29.99 AS Price
UNION ALL
SELECT 2 AS ID, 'USB Cable' AS Name, 12.99 AS Price
UNION ALL
SELECT 3 AS ID, 'Keyboard' AS Name, 89.99 AS Price
```

The first run, with `--full-refresh`, creates:

```text
ID | Name           | Price  | _is_current | _valid_from         | _valid_until
1  | Wireless Mouse | 29.99  | true        | 2024-01-01 10:00:00 | 9999-12-31 00:00:00
2  | USB Cable      | 12.99  | true        | 2024-01-01 10:00:00 | 9999-12-31 00:00:00
3  | Keyboard       | 89.99  | true        | 2024-01-01 10:00:00 | 9999-12-31 00:00:00
```

Suppose the next source snapshot raises the Wireless Mouse price to 39.99, drops the Keyboard, and adds a Monitor. After the next run, the table is:

```text
ID | Name           | Price  | _is_current | _valid_from         | _valid_until
1  | Wireless Mouse | 29.99  | false       | 2024-01-01 10:00:00 | 2024-01-02 14:30:00
1  | Wireless Mouse | 39.99  | true        | 2024-01-02 14:30:00 | 9999-12-31 00:00:00
2  | USB Cable      | 12.99  | true        | 2024-01-01 10:00:00 | 9999-12-31 00:00:00
3  | Keyboard       | 89.99  | false       | 2024-01-01 10:00:00 | 2024-01-02 14:30:00
4  | Monitor        | 199.99 | true        | 2024-01-02 14:30:00 | 9999-12-31 00:00:00
```

- The Wireless Mouse now has two versions: the old price, closed, and the new price, current.
- The USB Cable did not change, so its version stays current.
- The Keyboard is closed because it left the source.
- The Monitor is added as a new current version.

**Use business timestamps instead of processing time.** By default, versions are stamped with `CURRENT_TIMESTAMP()`. If the source records when a change happened, set it as the `incremental_key` and declare it in `columns`:

```yaml
materialization:
  type: table
  strategy: scd2_by_column
  incremental_key: updated_at
```

With an `incremental_key`:

- New and changed versions get `_valid_from` from the `incremental_key` column.
- A version closed by a change gets `_valid_until` from the new version's `incremental_key`.
- A version closed because the record left the source still uses `CURRENT_TIMESTAMP()`.
- The `incremental_key` is itself a non-key column, so a change to it alone creates a new version.

#### `scd2_by_time` {#scd2-by-time}

Creates a new version of a record when its `incremental_key` moves forward in time.

```bruin-sql
/* @bruin

name: dim.products
type: bq.sql

materialization:
  type: table
  strategy: scd2_by_time
  incremental_key: dt

columns:
  - name: product_id
    type: INTEGER
    description: "Unique identifier for the product"
    primary_key: true
  - name: product_name
    type: VARCHAR
    description: "Name of the product"
  - name: stock
    type: INTEGER
    description: "Number of units in stock"
  - name: dt
    type: DATE
    description: "Date when the product was last updated"

@bruin */

SELECT product_id, product_name, stock, dt
FROM raw.products
```

The first run, with `--full-refresh`, creates:

```text
product_id | product_name | stock | _is_current | _valid_from         | _valid_until
1          | Laptop       | 100   | true        | 2025-04-02 00:00:00 | 9999-12-31 00:00:00
2          | Smartphone   | 150   | true        | 2025-04-02 00:00:00 | 9999-12-31 00:00:00
3          | Headphones   | 175   | true        | 2025-04-02 00:00:00 | 9999-12-31 00:00:00
4          | Monitor      | 25    | true        | 2025-04-02 00:00:00 | 9999-12-31 00:00:00
```

Suppose the next source snapshot changes the Headphones stock to 900 with `dt = 2025-06-02`, drops the Monitor, and adds a PS5. After the next run, the table is:

```text
product_id | product_name | stock | _is_current | _valid_from         | _valid_until
1          | Laptop       | 100   | true        | 2025-04-02 00:00:00 | 9999-12-31 00:00:00
2          | Smartphone   | 150   | true        | 2025-04-02 00:00:00 | 9999-12-31 00:00:00
3          | Headphones   | 175   | false       | 2025-04-02 00:00:00 | 2025-06-02 00:00:00
3          | Headphones   | 900   | true        | 2025-06-02 00:00:00 | 9999-12-31 00:00:00
4          | Monitor      | 25    | false       | 2025-04-02 00:00:00 | 2025-06-03 08:15:00
5          | PS5          | 25    | true        | 2025-06-02 00:00:00 | 9999-12-31 00:00:00
```

- The Laptop and Smartphone did not change, so their versions stay current.
- The Headphones have a newer `dt`, so the old version is closed and a new one is added.
- The Monitor is closed because it left the source. Records that disappear are closed at the run time (`CURRENT_TIMESTAMP()`), not at an `incremental_key` value.
- The PS5 is added as a new current version.

The accepted `incremental_key` types vary slightly by platform. PostgreSQL and Redshift reject `TIMESTAMPTZ`, Oracle rejects `TIMESTAMP WITH TIME ZONE`, and MySQL also accepts `DATETIME`.

#### SCD2 timestamps and time zones

`_valid_from` and `_valid_until` are timezone-aware, so each value is an unambiguous instant:

| Platform | Column type |
| --- | --- |
| DuckDB, MotherDuck, PostgreSQL, Redshift, Athena, Oracle | `TIMESTAMP WITH TIME ZONE` / `TIMESTAMPTZ` |
| Snowflake | `TIMESTAMP_TZ` |
| BigQuery, Databricks, Spark | `TIMESTAMP`, which is already an absolute instant. On Databricks, a full refresh gives `_valid_from` the type of the `incremental_key`. |
| ClickHouse | `DateTime64(6, 'UTC')` |
| MySQL | `DATETIME`, timezone-naive, because MySQL's `TIMESTAMP` cannot hold the `9999-12-31` sentinel. Values follow the session `time_zone`. |

When the `incremental_key` is a timezone-naive column, BigQuery, Snowflake, PostgreSQL, DuckDB, Athena, and ClickHouse read its values as UTC, so results do not depend on the session's time zone. Oracle, Redshift, Databricks, Spark, and MySQL use the session's time zone.

::: tip Migrating existing tables
SCD2 tables created before these columns became timezone-aware are upgraded on their next **incremental** run on PostgreSQL, Redshift, Snowflake, MySQL, Oracle, DuckDB, and Athena. Existing `_valid_from` and `_valid_until` values are converted to UTC, with no `--full-refresh` and no loss of history. This temporary migration was added in July 2026 and will be removed once existing tables have been upgraded.
:::

#### SCD2 on ClickHouse

- Both strategies create the destination on the first run. `--full-refresh` replaces all history with the current source snapshot.
- The current-version sentinel is `2299-12-31 23:59:59`, for servers that cannot represent year 9999.
- Incremental runs use staged deletes and inserts. They are not atomic and do not support a `cluster` connection setting.
- If set, the `incremental_key` must have a non-nullable date or timestamp type and contain no `NULL` values. A `NULL` `_valid_from` would prevent later changes from being detected.
- Use `MergeTree()`, usually ClickHouse's default engine, or `ReplicatedMergeTree()` to keep every version. Engines such as `ReplacingMergeTree()` can discard versions that share a sorting key.
- Keep synchronous lightweight deletes enabled (`lightweight_deletes_sync`, on servers that expose it) so staging tables stay available until deletes finish.

### Data Vault

Bruin provides three strategies for loading a Raw Data Vault: `datavault_hub`, `datavault_link`, and `datavault_satellite`. They are supported for PostgreSQL and DuckDB SQL assets with `type: table`. On other platforms, including Redshift, an incremental run fails with an unsupported strategy error, which `bruin validate` does not flag.

The asset query must calculate the hash keys and hashdiff and return every column declared in `columns`. Bruin applies the loading rules to those values but does not calculate hashes. Every declared column needs a `name` and a database-compatible `type`.

All three strategies:

- Create the schema and the target table if they do not exist.
- Define every role-bearing column as `NOT NULL`: hash keys, business keys, the satellite hashdiff, the load datetime, and the record source. Source rows with `NULL` in any of these are skipped. A column declared with `nullable: false` is also `NOT NULL`, but it is not part of the skip filter, so a `NULL` there fails the run instead of dropping the row.
- Run table creation and the incremental load in one transaction.
- Drop and recreate the target on a full refresh, then apply the same loading rules to the refreshed data. An asset with `full_refresh_restricted: true` keeps loading incrementally and is not dropped.

On incremental runs, `CREATE TABLE IF NOT EXISTS` does not alter an existing table. If you change the declared columns or their types, migrate the table yourself or run a full refresh.

#### Data Vault column roles

Set a column's role with `meta.datavault_role`. Explicit roles are recommended, especially when an asset has several hash key columns.

| Column purpose | Accepted `datavault_role` values | Fallback when no role is set |
| --- | --- | --- |
| Hub hash key | `hash_key`, `hub_hash_key` | The only column with `primary_key: true`, or the only column ending in `_hk` |
| Hub business key | `business_key` | Columns ending in `_bk`, in addition to explicitly roled columns |
| Link hash key | `link_hash_key`, `hash_key` | The only column with `primary_key: true`, or the only column ending in `_hk` |
| Related hash keys in a link | `hub_hash_key`, `parent_hash_key`, `foreign_hash_key` | Other columns ending in `_hk` |
| Satellite parent hash key | `parent_hash_key`, `hub_hash_key`, `hash_key` | The only column with `primary_key: true`, or the only column ending in `_hk` |
| Satellite hashdiff | `hashdiff`, `hash_diff` | A column named `hashdiff` or `hash_diff` |
| Load datetime | `load_datetime`, `load_dts` | A column named `load_dts`, `load_datetime`, or `loaded_at` |
| Record source | `record_source` | A column named `record_source` |

Role values and naming fallbacks are case-insensitive. If several columns have `primary_key: true` and none has a role, validation fails until you set one. For a link with several `_hk` columns, identify the link hash key with `datavault_role: link_hash_key` or `primary_key: true`; Bruin treats the remaining `_hk` columns as related hash keys.

Suffix fallbacks apply in addition to explicit roles, not instead of them. In a hub, every column ending in `_bk` becomes a business key, and in a link every `_hk` column other than the link hash key becomes a related hash key. A descriptive column with one of those suffixes therefore becomes a required key column. Rename it if it should not be part of the key.

#### `datavault_hub` {#datavault-hub}

Loads one row per hub hash key. Within the current query result, Bruin keeps the row with the earliest load datetime for each hash key, and inserts it only if the hash key is not already in the target. Repeated loads are idempotent.

A hub requires one hub hash key, one or more business keys, one load datetime, and one record source.

```bruin-sql
/* @bruin
name: rdv.hub_customer
type: duckdb.sql

materialization:
  type: table
  strategy: datavault_hub

columns:
  - name: customer_hk
    type: VARCHAR
    meta:
      datavault_role: hash_key
  - name: customer_id
    type: VARCHAR
    meta:
      datavault_role: business_key
  - name: load_dts
    type: TIMESTAMP
    meta:
      datavault_role: load_datetime
  - name: record_source
    type: VARCHAR
    meta:
      datavault_role: record_source
@bruin */

SELECT customer_hk, customer_id, load_dts, record_source
FROM stg.customer_hashed
```

Bruin creates `customer_hk` as the table's primary key.

#### `datavault_link` {#datavault-link}

Loads one row per link hash key. As with hubs, Bruin keeps the earliest row per link hash key from the current query and inserts it only if that key is not already in the target.

A link requires one link hash key, one or more related hub, parent, or foreign hash keys, one load datetime, and one record source.

```bruin-sql
/* @bruin
name: rdv.link_customer_order
type: duckdb.sql

materialization:
  type: table
  strategy: datavault_link

columns:
  - name: customer_order_hk
    type: VARCHAR
    meta:
      datavault_role: link_hash_key
  - name: customer_hk
    type: VARCHAR
    meta:
      datavault_role: hub_hash_key
  - name: order_hk
    type: VARCHAR
    meta:
      datavault_role: hub_hash_key
  - name: load_dts
    type: TIMESTAMP
    meta:
      datavault_role: load_datetime
  - name: record_source
    type: VARCHAR
    meta:
      datavault_role: record_source
@bruin */

SELECT customer_order_hk, customer_hk, order_hk, load_dts, record_source
FROM stg.customer_orders_hashed
```

Bruin creates `customer_order_hk` as the table's primary key.

#### `datavault_satellite` {#datavault-satellite}

Keeps the descriptive history of a parent hash key. Bruin orders incoming rows for each parent by load datetime. The first row is compared with the parent's latest row in the target, and each later row with the row before it in the batch. Only real changes are inserted; consecutive rows with the same hashdiff are skipped.

Bruin loads at most one row per target primary key, which by default is the parent hash key plus the load datetime. Rows that collide on that key within a batch are collapsed into one (the row with the lowest hashdiff is kept), and rows whose key already exists in the target are skipped, not updated. Give each version of a parent a distinct load datetime so that no version is dropped.

A satellite requires one parent hash key, one hashdiff, one load datetime, one record source, and any number of descriptive columns.

```bruin-sql
/* @bruin
name: rdv.sat_customer_details
type: duckdb.sql

materialization:
  type: table
  strategy: datavault_satellite

columns:
  - name: customer_hk
    type: VARCHAR
    meta:
      datavault_role: parent_hash_key
  - name: hashdiff
    type: VARCHAR
    meta:
      datavault_role: hashdiff
  - name: load_dts
    type: TIMESTAMP
    meta:
      datavault_role: load_datetime
  - name: record_source
    type: VARCHAR
    meta:
      datavault_role: record_source
  - name: customer_name
    type: VARCHAR
  - name: email
    type: VARCHAR
@bruin */

SELECT customer_hk, hashdiff, load_dts, record_source, customer_name, email
FROM stg.customer_hashed
```

To use a different primary key, mark its columns with `primary_key: true`. Marking only the parent hash key keeps the default key of parent hash key plus load datetime. With a custom primary key, also set `datavault_role: parent_hash_key` on the parent hash key column. Without it, several `primary_key` columns fail validation, and a single `primary_key` column is used as the parent hash key even if it is not one.

## Full refresh and `full_refresh_restricted` {#full-refresh-and-full-refresh-restricted}

A full refresh rebuilds a table from scratch. Trigger it for a whole run with `bruin run --full-refresh`, or for a single asset on every run with `parameters.full_refresh: true`.

During a full refresh, table assets are rebuilt with `create+replace` instead of their configured strategy:

| Strategy | Full-refresh behavior |
| --- | --- |
| `create+replace`, `truncate+insert`, `append`, `delete+insert`, `merge`, `time_interval` | The table is recreated from the full query result. |
| `ddl` | Unchanged. The table is never dropped or recreated. |
| `scd2_by_column`, `scd2_by_time` | A new SCD2 table is built from the current source snapshot. Existing history is lost. |
| Data Vault | On PostgreSQL and DuckDB, the table is dropped and recreated, then loaded with the Data Vault rules. |
| `type: view` | Unchanged. Views are always replaced. |

Because the full query result is loaded, remember to widen or remove date filters in incremental queries when `full_refresh` is set. The `{{ full_refresh }}` [Jinja variable](../variables/built-in.md) lets the query branch on it:

```sql
SELECT *
FROM raw.events
{% if not full_refresh %}
WHERE event_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
{% endif %}
```

### Protecting tables with `full_refresh_restricted`

Some tables should never be dropped, for example when they:

- have external dependencies,
- take a long time to rebuild, or
- hold history that cannot be recomputed, such as SCD2 tables.

Set `full_refresh_restricted: true` to keep the asset on its normal strategy during a full refresh:

```bruin-sql
/* @bruin

name: dashboard.critical_table
type: bq.sql

materialization:
  type: table
  strategy: merge

full_refresh_restricted: true # [!code focus]

@bruin */

SELECT * FROM important_data
```

| Setting | Behavior on a full refresh |
| --- | --- |
| `full_refresh_restricted: true` | The table is not dropped. The asset runs with its normal strategy, and Bruin prints a warning. |
| `full_refresh_restricted: false` or not set | The table is dropped and recreated (default). |

The older asset-level `refresh_restricted` field is still supported as an alias. The restriction takes precedence over both `--full-refresh` and `parameters.full_refresh`.

To protect every asset in an environment, set it in `.bruin.yml`:

```yaml
environments:
  production:
    config:
      full_refresh_restricted: true
    connections:
      # ...
```

## Platform support

`bruin validate` checks that your materialization configuration is complete, for example that `merge` has a primary key. It does not check whether your platform supports the strategy; that error appears when the asset runs.

Every platform below supports views and the `create+replace`, `truncate+insert`, `ddl`, `append`, and `delete+insert` strategies. Support for the remaining strategies varies:

| Platform | merge | time_interval | SCD2 by column | SCD2 by time | Data Vault |
| --- | :---: | :---: | :---: | :---: | :---: |
| [BigQuery](../platforms/bigquery.md) | ✅ | ✅ | ✅ | ✅ | ❌ |
| [Snowflake](../platforms/snowflake.md) | ✅ | ✅ | ✅ | ✅ | ❌ |
| [PostgreSQL](../platforms/postgres.md) | ✅ | ✅ | ✅ | ✅ | ✅ |
| [Redshift](../platforms/redshift.md) | ✅ | ✅ | ✅ | ✅ | ❌ |
| [DuckDB](../platforms/duckdb.md) / [MotherDuck](../platforms/motherduck.md) | ✅ | ✅ | ✅ | ✅ | ✅ |
| [MySQL](../platforms/mysql.md) | ✅ | ✅ | ✅ | ✅ | ❌ |
| [Databricks](../platforms/databricks.md) | ✅ | ✅ | ✅ | ✅ | ❌ |
| [Athena](../platforms/athena.md) | ✅ | ✅ | ✅ | ✅ | ❌ |
| [Spark](../platforms/spark.md) | ✅ | ✅ | ✅ | ✅ | ❌ |
| [ClickHouse](../platforms/clickhouse.md) | ✅ | ✅ | ✅ | ✅ | ❌ |
| [Oracle](../platforms/oracle.md) | ✅ | ✅ | ❌ | ✅ | ❌ |
| [MSSQL](../platforms/mssql.md) | ✅ | ✅ | ❌ | ❌ | ❌ |
| [Synapse](../platforms/synapse.md) | ✅ | ✅ | ❌ | ❌ | ❌ |
| Vertica | ✅ | ✅ | ❌ | ❌ | ❌ |
| [Fabric](../platforms/fabric.md) | ✅ | ❌ | ❌ | ❌ | ❌ |
| [Doris](../platforms/doris.md) | ✅ | ✅ | ❌ | ❌ | ❌ |
| [StarRocks](../platforms/starrocks.md) | ✅ | ✅ | ❌ | ❌ | ❌ |
| [Trino](../platforms/trino.md) | ❌ | ✅ | ❌ | ❌ | ❌ |
| [Dremio](../platforms/dremio.md) | ❌ | ✅ | ❌ | ❌ | ❌ |
| [Sail](../platforms/sail.md) | ❌ | ✅ | ❌ | ❌ | ❌ |

### Platform notes

For `merge`:

- **MySQL** stages the result, deletes matched target rows, and reinserts every staged row. Matched rows are fully replaced, so `update_on_merge` has no effect; `merge_sql` still applies.
- **ClickHouse** deletes and reinserts rows on the primary key, so matched rows are fully replaced. Statements run one at a time, without a transaction. See [ClickHouse materialization](../platforms/clickhouse.md#materialization-and-incremental-strategies) for cluster restrictions.
- **Fabric** deletes and reinserts rows on the primary key.
- **StarRocks** upserts on the primary key. It needs `starrocks.table_model: primary_key`, which is the default, creates the table if it is missing, and rejects `merge_sql` on non-key columns.
- **Doris** needs `doris.table_model: unique_key`, which is the default.
- **Databricks and Synapse** update only `update_on_merge` columns. They ignore `merge_sql`, so a column with only `merge_sql` is never updated.
- **Redshift** does not support `incremental_predicate`.

For `delete+insert`:

- **ClickHouse** requires exactly one `primary_key` column.
- **Fabric** matches on the primary key columns and does not use `incremental_key`.
- **Doris and StarRocks** build a new table from the remaining and new rows and swap it in. The new table does not keep your partitioning or distribution.
- **Trino, Dremio, Sail, Spark, and Oracle** run the asset query twice instead of staging it in a temporary table. **Databricks** stages it in a temporary view, which is evaluated again by both the delete and the insert.

Other differences:

- **Athena** materializations always create Iceberg tables. See [Athena](../platforms/athena.md).
- **Oracle** builds `scd2_by_column` assets as a plain table on a full refresh, and closes records that left the source only when the source returns at least one row.

### ClickHouse table options

Native `clickhouse.sql` assets configure their engine, sorting key, TTL, and table settings in a top-level `clickhouse` block alongside `materialization`:

```yaml
materialization:
  type: table
  strategy: create+replace
  partition_by: toYYYYMM(created_at)
clickhouse:
  engine: ReplacingMergeTree(version)
  order_by:
    - id
    - created_at
  ttl: created_at + INTERVAL 30 DAY
  settings:
    index_granularity: "8192"
```

| Option | Type | Meaning |
| --- | --- | --- |
| `clickhouse.engine` | String | SQL engine expression, such as `MergeTree()`, `ReplacingMergeTree(version)`, or `SummingMergeTree()`. If omitted, Bruin leaves the engine clause to ClickHouse's default. |
| `clickhouse.order_by` | String[] | SQL expressions forming the sorting key, in order. Use `["tuple()"]` for an explicitly empty sorting key. `materialization.cluster_by` is an alias for this option; `order_by` wins when both are set. |
| `clickhouse.ttl` | String | SQL TTL expression, without the `TTL` keyword. |
| `clickhouse.settings` | Map of strings | Table settings rendered as SQL values. Include SQL quotes inside string values, for example `storage_policy: "'default'"`. |

These options apply when the target table is created: with `create+replace`, the default table strategy, `ddl`, full refresh, and the first SCD2 run. Incremental runs keep the target's definition, and temporary staging tables do not inherit these options. Views and other asset types reject the `clickhouse` options.

If columns have `primary_key: true`, they must be the leading entries of `clickhouse.order_by`, in column declaration order. With neither `engine` nor `order_by`, Bruin keeps the primary-key-only behavior. An explicit `order_by` or engine allows `create+replace` without a primary key; ClickHouse validates the chosen engine's key requirements. Engines such as `Memory()` do not support sorting or primary-key clauses.

Pipeline defaults can set the same options under `default.clickhouse`, which only native `clickhouse.sql` table assets inherit. Non-empty asset fields override defaults, and `settings` merge by name with asset values taking precedence.

These options are separate from ingestr destination parameters (`parameters.engine` and `parameters.engine.<setting>`). See [ClickHouse table definitions](../platforms/clickhouse.md#native-sql-table-definitions) for a complete example.

## Materialization in other asset types

The `materialization` block is shared by other asset types, with a smaller set of strategies:

| Asset type | `type` | Strategies |
| --- | --- | --- |
| [Python](./python.md#materialization) | `table` only, with a `connection` | `create+replace`, `append`, `merge`, `delete+insert` |
| [Ingestr](./ingestr.md) | `table` only | `create+replace`, `append`, `merge`, `delete+insert`, `truncate+insert` |
| [Seed](./seed.md) | Not supported | — |

`incremental_predicate` is not supported for Python or ingestr assets. For Python assets, `partition_by` and `cluster_by` are read from `parameters`, not from `materialization`.
