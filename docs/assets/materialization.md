# Materialization

Materialization is the idea taking a simple `SELECT` query, and applying the necessary logic to materialize the results into a table or view. This is a common pattern in data engineering, where you have a query that is expensive to run, and you want to store the results in a table for faster access.

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
- **Type:** `String[]`
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