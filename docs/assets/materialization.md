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
- `append`: only append the new data to the table, never overwrite.
- `merge`: merge the existing records with the new records, requires a primary key to be set.
- `DDL`: create a new table using a DDL (Data Definition Language) statement.

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

Merge strategy requires columns to be defined and marked with `primary_key` or `update_on_merge`.
- `primary_key` is used to determine which rows to update and which rows to insert.
- `update_on_merge` is used to determine which columns to update when a row already exists. By default, this is considered to be `false`.

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

