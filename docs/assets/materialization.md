# Materialization

Materialization is the idea of taking a simple `SELECT` query, and applying the necessary logic to materialize the results into a table or view.

Bruin supports various materialization strategies catered to different use cases.

Here's a sample asset with materialization:
```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

materialization:
    type: table

@bruin */

select 1 as one
union all
select 2 as one
```

## Definition Schema
The top level `materialization` key determines how the asset will be materialized.

Here's an example materialization definition:
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

### `materialization > type`
The type of the materialization, can be one of the following:
- `table`
- `view`

**Default:** none

### `materialization > strategy`
The strategy used for the materialization, can be one of the following:
- `create+replace`: overwrite the existing table with the new version.
- `delete+insert`: incrementally update the table by only refreshing a certain partition.
- `truncate+insert`: truncate the entire table and insert new data (full refresh without DROP/CREATE).
- `append`: only append the new data to the table, never overwrite.
- `merge`: merge the existing records with the new records, requires a primary key to be set.
- `time_interval`: incrementally load time-based data within specific time windows.
- `DDL`: create a new table using a DDL (Data Definition Language) statement.
- `scd2_by_column`: implement SCD2 logic that tracks changes based on column value differences.
- `scd2_by_time`: implement SCD2 logic that tracks changes based on time-based incremental key.

### `materialization > partition_by`
Define the column that will be used for the partitioning of the resulting table. This is used to instruct the data warehouse to set the column for the partition key.

- **Type:** `String`
- **Default:** none


### `materialization > cluster_by`
Define the columns that will be used for the clustering of the resulting table. This is used to instruct the data warehouse to set the columns for the clustering.

- **Type:** `String[]`
- **Default:** `[]`

### `materialization > incremental_key`

This is the column of the table that will be used for incremental updates of the table.
- **Type:** `String`
- **Default:** `[]`

## Strategies
Bruin supports various materialization strategies that take your code and convert it to another structure behind the scenes to materialize the execution results of your assets.

### Default: no materialization

By default, Bruin does not apply any materialization to the assets. This means that the query will be executed every time the asset is run, and you are responsible for storing the results in a table via a `CREATE TABLE` or a similar statement in your SQL asset.

### `create+replace`

This materialization strategy is useful when you want to create a table if it does not exist, and replace the contents of the table with the results of the query. This is useful when you want to ensure that the table is always up-to-date with the query results.

`create+replace` strategy does not do any incremental logic, which means it's a full refresh every time the asset is run. This can be expensive for large tables.

Here's an example of an asset with `create+replace` materialization:
```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

materialization:
    type: table

@bruin */

select 1 as one
union all
select 2 as one
```

The result will be a table `dashboard.hello_bq` with the result of the query.

### `delete+insert`
`delete+insert` strategy is useful for incremental updates. It deletes the rows that are no longer present in the query results and inserts the new rows. This is useful when you have a large table and you want to minimize the amount of data that needs to be written.

This strategy requires an `incremental_key` to be specified. This key is used to determine which rows to delete and which rows to insert.

Bruin implements `delete+insert` strategy in the following way:
- run the asset query, put the results in a temp table
- run a `SELECT DISTINCT` query on the temp table to get the unique values of the `incremental_key`
- run a `DELETE` query on the target table to delete all the rows that match the `incremental_key` values determined above
- run an `INSERT` query to insert the new rows from the temp table

Here's an example of an asset with `delete+insert` materialization:
```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

materialization:
    type: table
    strategy: delete+insert
    incremental_key: UserId

@bruin */

select 1 as UserId, 'Alice' as Name
union all
select 2 as UserId, 'Bob' as Name
```

### `truncate+insert`
`truncate+insert` strategy is useful for full table replacement when you want to clear all existing data and insert fresh data. Unlike `create+replace`, this strategy maintains the existing table structure (schema, permissions, indices, etc.) and only removes the data.

This strategy is more efficient than `delete+insert` for full table refreshes because:
- TRUNCATE is generally faster than DELETE for removing all rows
- It doesn't require an `incremental_key`
- It maintains table metadata and permissions

Here's an example of an asset with `truncate+insert` materialization:
```bruin-sql
/* @bruin

name: dashboard.daily_snapshot
type: bq.sql

materialization:
    type: table
    strategy: truncate+insert

@bruin */

select 
    current_date as snapshot_date,
    count(*) as total_users,
    sum(revenue) as total_revenue
from users
```

### `append`
`append` strategy is useful when you want to add new rows to the table without overwriting the existing rows. This is useful when you have a table that is constantly being updated and you want to keep the history of the data.

Bruin will simply run the query, and insert the results into the destination table.

```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

materialization:
    type: table
    strategy: append

@bruin */

select 1 as one
union all
select 2 as one
```

### `merge`
`merge` strategy is useful when you want to merge the existing rows with the new rows. This is useful when you have a table with a primary key and you want to update the existing rows and insert the new rows, helping you avoid duplication while keeping the most up-to-date version of the data in the table incrementally.

Merge strategy requires columns to be defined and marked with `primary_key` and optionally `update_on_merge` or `merge_sql`:
- `primary_key` determines which rows to update vs insert.
- `update_on_merge` marks columns to update with `source.col` when a row matches.
- `merge_sql` lets you specify a custom expression per column for matches, e.g. `GREATEST(target.col, source.col)` or `target.c + source.c`. When present, `merge_sql` takes precedence over `update_on_merge`.

Supported platforms for `merge_sql`:
- BigQuery, Snowflake, Postgres, mssql: supported
- Athena (Iceberg tables): supported
- Databricks,ClickHouse, Trino, DuckDB: not supported

> [!INFO]
> An important difference between `merge` and `delete+insert` is that `merge` will update the existing rows, while `delete+insert` will delete the existing rows and insert the new rows. This means if your source has deleted rows, `merge` will not delete them from the destination, whereas `delete+insert` will if their `incremental_key` matches.

Here's a sample asset with `merge` materialization:
```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

materialization:
    type: table
    strategy: merge

columns:
  - name: UserId
    type: integer
    primary_key: true
  - name: UserName
    type: string
    update_on_merge: true
  - name: Score
    type: integer
    merge_sql: GREATEST(target.Score, source.Score)

@bruin */

select 1 as UserId, 'Alice' as UserName
union all
select 2 as UserId, 'Bob' as UserName
```

### `time_interval`

The `time_interval` strategy is designed for incrementally loading time-based data. It's useful when you want to process data within specific time windows, ensuring efficient updates of historical data while maintaining data consistency.

This strategy requires the following configuration:
- `incremental_key`: The column used for time-based filtering
- `time_granularity`: Must be either 'date' or 'timestamp'
  - Use 'date' when your incremental_key is a DATE column (e.g., '2024-03-20')
  - Use 'timestamp' when your incremental_key is a TIMESTAMP column (e.g., '2024-03-20 15:30:00')

When running assets with time_interval strategy, you can specify the time window using the start and end date flags:
```bash
bruin run --start-date "2024-03-01" --end-date "2024-03-31" path/to/your/asset
```

By default:
- `start-date`: Beginning of yesterday (00:00:00.000000)
- `end-date`: End of yesterday (23:59:59.999999)

Here's a sample asset with `time_interval` materialization:
```bruin-sql
/* @bruin
name: dashboard.hello_bq
type: bq.sql

materialization:
  type: table
  strategy: time_interval
  time_granularity: date
  incremental_key: dt

columns:
  - name: product_id
    type: INTEGER
    description: "Unique identifier for the product"
    primary_key: true
  - name: product_name
    type: VARCHAR
    description: "Name of the product"
  - name: price
    type: FLOAT
    description: "Price of the product in USD"
  - name: stock
    type: INTEGER
    description: "Number of units in stock"
  - name: dt
    type: DATE
    description: "Date when the product was last updated"
@bruin */


SELECT
    1 AS product_id,
    'Laptop' AS product_name,
    999.99 AS price,
    10 AS stock,
    DATE '2025-03-15' AS dt
UNION ALL
SELECT
    2 AS product_id,
    'Smartphone' AS product_name,
    699.99 AS price,
    50 AS stock,
    DATE '2024-03-16' AS dt;
```

The strategy will:
1. Begin a transaction
2. Delete existing records within the specified time interval
3. Insert new records from the query given in the asset

### `DDL`

The `DDL` (Data Definition Language) strategy is used to create a new table using the information provided in the 
embedded YAML section of the asset. This is useful when you want to create a new table with a specific schema and structure
and ensure that this table is only created once.

The `DDL` strategy defines the table structure via column definitions in the columns field of the asset. 
For this reason, you should not include any query after the embedded YAML section.

Here's an example of an asset with `DDL` materialization:

```bruin-sql
/* @bruin
name: dashboard.products
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
  - name: stock
    type: INTEGER
    description: "Number of units in stock"
@bruin */

```

This strategy will:
- Create a new empty table with the name `dashboard.products`
- Use the provided schema to define the column names, column types as well as optional primary key constraints and descriptions.

The strategy also supports partitioning and clustering for data warehouses that support these features. You can specify
in the materialization definition with the following keys:
- `partition_by`
- `cluster_by`

### `scd2_by_column`

The `scd2_by_column` strategy implements [Slowly Changing Dimension Type 2](https://en.wikipedia.org/wiki/Slowly_changing_dimension) logic, which maintains a full history of data changes over time. This strategy is useful when you want to track changes to records and preserve the historical state of your data.

This strategy automatically detects changes in non-primary key columns and creates new versions of records when changes occur, while marking previous versions as historical.

**Requirements:**
- At least one column must be marked as `primary_key: true`
- The column names `_valid_from`, `_valid_until`, and `_is_current` are reserved and cannot be used in your column definitions

**How it works:**
When changes are detected in non-primary key columns:
1. The existing record is marked as historical (`_is_current: false`) and gets an end timestamp in `_valid_until`
2. A new record is inserted with the updated values (`_is_current: true`) and `_valid_until` set to '9999-12-31'
3. Records that no longer exist in the source are marked as historical

**Automatically added columns:**
- `_valid_from`: TIMESTAMP when the record version became active (set to `CURRENT_TIMESTAMP()`)
- `_valid_until`: TIMESTAMP when the record version became inactive (set to `TIMESTAMP('9999-12-31')` for current records)
- `_is_current`: BOOLEAN indicating if this is the current version of the record

Here's an example of an asset with `scd2_by_column` materialization:

```bruin-sql
/* @bruin
name: test.product_catalog
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

**Example behavior:**

Let's say you want to create a new table to track product catalog with SCD2. If the table doesn't exist yet, you'll need an initial run with the `--full-refresh` flag:

```bash
bruin run --full-refresh path/to/your/product_catalog.sql
```

This initial run creates:
```
ID | Name          | Price | _is_current | _valid_from         | _valid_until
1  | Wireless Mouse| 29.99 | true        | 2024-01-01 10:00:00| 9999-12-31 23:59:59
2  | USB Cable     | 12.99 | true        | 2024-01-01 10:00:00| 9999-12-31 23:59:59
3  | Keyboard      | 89.99 | true        | 2024-01-01 10:00:00| 9999-12-31 23:59:59
```

Now lets say you have new incoming data that updates Wireless Mouse price to 39.99, removes Keyboard from the catalog, and adds a new item Monitor. When you run the asset again:

```bash
bruin run path/to/your/product_catalog.sql
```

The table becomes:
```
ID | Name          | Price | _is_current | _valid_from         | _valid_until
1  | Wireless Mouse| 29.99 | false       | 2024-01-01 10:00:00| 2024-01-02 14:30:00
1  | Wireless Mouse| 39.99 | true        | 2024-01-02 14:30:00| 9999-12-31 23:59:59
2  | USB Cable     | 12.99 | true        | 2024-01-01 10:00:00| 9999-12-31 23:59:59
3  | Keyboard      | 89.99 | false       | 2024-01-01 10:00:00| 2024-01-02 14:30:00
4  | Monitor       | 199.99| true        | 2024-01-02 14:30:00| 9999-12-31 23:59:59
```

Notice how:
- Wireless Mouse (ID=1) now has two records: the old price (marked as historical) and the new price (current)
- USB Cable (ID=2) remains unchanged with its original record still current
- Keyboard (ID=3) is marked as historical since it's no longer in the source data
- Monitor (ID=4) is added as a new current record

### `scd2_by_time`

The `scd2_by_time` strategy implements [Slowly Changing Dimension Type 2](https://en.wikipedia.org/wiki/Slowly_changing_dimension) logic based on a time-based incremental key. This strategy is ideal when your source data includes timestamps or dates that indicate when records were last modified, and you want to maintain historical versions based on these time changes.

**Requirements:**
- At least one column must be marked as `primary_key: true`
- An `incremental_key` must be specified that references a column of type `TIMESTAMP` or `DATE`
- The column names `_valid_from`, `_valid_until`, and `_is_current` are reserved and cannot be used in your column definitions

**How it works:**
The strategy tracks changes based on the time values in the `incremental_key` column:
1. When a record has a newer timestamp than existing records, it creates a new version
2. Previous versions are marked as historical (`_is_current: false`) with their `_valid_until` updated
3. Records no longer present in the source are marked as historical

**Automatically added columns:**
- `_valid_from`: TIMESTAMP when the record version became active (derived from the `incremental_key`)
- `_valid_until`: TIMESTAMP when the record version became inactive (set to `TIMESTAMP('9999-12-31')` for current records)
- `_is_current`: BOOLEAN indicating if this is the current version of the record

Here's an example of an asset with `scd2_by_time` materialization:

```bruin-sql
/* @bruin
name: test.products
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

SELECT
    1 AS product_id,
    'Laptop' AS product_name,
    100 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    2 AS product_id,
    'Smartphone' AS product_name,
    150 AS stock,
    DATE '2025-04-02' AS dt
```

**Example behavior:**

Let's say you want to create a new table to track product inventory with SCD2 based on time. If the table doesn't exist yet, you'll need an initial run with the `--full-refresh` flag:

```bash
bruin run --full-refresh path/to/your/products.sql
```

This initial run creates:
```
product_id | product_name | stock | _is_current | _valid_from         | _valid_until
1          | Laptop       | 100   | true        | 2025-04-02 00:00:00| 9999-12-31 23:59:59
2          | Smartphone   | 150   | true        | 2025-04-02 00:00:00| 9999-12-31 23:59:59
3          | Headphones   | 175   | true        | 2025-04-02 00:00:00| 9999-12-31 23:59:59
4          | Monitor      | 25    | true        | 2025-04-02 00:00:00| 9999-12-31 23:59:59
```

Now lets say you have new incoming data with updates: Headphones stock changed from 175 to 900 with a new date (2025-06-02), Monitor is no longer available, and a new product PS5 is added. When you run the asset again:

```bash
bruin run path/to/your/products.sql
```

The table becomes:
```
product_id | product_name | stock | _is_current | _valid_from         | _valid_until
1          | Laptop       | 100   | true        | 2025-04-02 00:00:00| 9999-12-31 23:59:59
2          | Smartphone   | 150   | true        | 2025-04-02 00:00:00| 9999-12-31 23:59:59
3          | Headphones   | 175   | false       | 2025-04-02 00:00:00| 2025-06-02 00:00:00
3          | Headphones   | 900   | true        | 2025-06-02 00:00:00| 9999-12-31 23:59:59
4          | Monitor      | 25    | false       | 2025-04-02 00:00:00| 2025-06-02 00:00:00
5          | PS5          | 25    | true        | 2025-06-02 00:00:00| 9999-12-31 23:59:59
```

Notice how:
- Laptop (ID=1) and Smartphone (ID=2) remain unchanged with their original records still current
- Headphones (ID=3) now has two records: the old stock level (marked as historical) and the new stock level (current) based on the newer date
- Monitor (ID=4) is marked as historical since it's no longer in the source data
- PS5 (ID=5) is added as a new current record with the latest date

**Key differences between scd2_by_column and scd2_by_time:**

| Aspect | scd2_by_column | scd2_by_time |
|--------|----------------|--------------|
| **Change Detection** | Automatically detects changes in any non-primary key column | Based on time values in the incremental_key column |
| **_valid_from Value** | Set to `CURRENT_TIMESTAMP()` when change is processed | Derived from the incremental_key column value |
| **Use Case** | When you want to track any column changes regardless of when they occurred | When your source data has reliable timestamps indicating when changes happened |
| **Configuration** | Only requires primary_key columns | Requires both primary_key columns and incremental_key |

> [!WARNING]
> SCD2 materializations are currently only supported for BigQuery, Snowflake, Postgres, Amazon Redshift and DuckDB.