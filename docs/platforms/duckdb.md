# Duck DB

DuckDB is an in-memory database designed to be fast and reliable.

Bruin supports using a local DuckDB database.

## Connection

```yaml
    connections:
      duckdb:
        - name: "connection_name"
          path: "/path/to/your/duckdb/database.db"
```

The field `path` is the only one you need and it can point to an existing database or the full path of the database that you want to create and where your queries would be materialized.

> [!WARNING]
> DuckDB does not allow concurrency between different processes, which means other clients should not be connected to the database while Bruin is running.


## Assets

DuckDB assets should use the type `duckdb.sql` and if you specify a connection it must be of the `duckdb` type.
For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.


### Examples

Create a view with orders per country
```bruin-sql
/* @bruin
name: orders_per_country
type: duckdb.sql
materialization:
    type: view
@bruin */

SELECT COUNT(*) as orders, country
FROM events.orders
WHERE status = "paid"
GROUP BY country
```

Materialize new customers per region and append them to an existing table
```bruin-sql
/* @bruin
name: new_customers_per_region
type: duckdb.sql
materialization:
    type: table
    strategy: append
@bruin */

SELECT COUNT(*) as customers, region 
    WHERE created_at >= {{ start_date }} 
      AND created_at < {{ end_date }}
FROM events.customers
```

### `duckdb.sensor.query`

Checks if a query returns any results in DuckDB, runs every 5 minutes until this query returns any results.

```yaml
name: string
type: string
parameters:
    query: string
```

**Parameters**:
- `query`: Query you expect to return any results

#### Example: Partitioned upstream table

Checks if the data available in upstream table for end date of the run.
```yaml
name: analytics_123456789.events
type: duckdb.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}"
```

#### Example: Streaming upstream table

Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.
```yaml
name: analytics_123456789.events
type: duckdb.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}"
```


### `duckdb.seed`
`duckdb.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your DuckDB database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the DuckDB database.

You can define seed assets in a file ending with `.asset.yml` or `.asset.yaml`:
```yaml
name: dashboard.hello
type: duckdb.seed

parameters:
    path: seed.csv
```

**Parameters**:
- `path`: The path to the CSV file that will be loaded into the data platform. This can be a relative file path (relative to the asset definition file) or an HTTP/HTTPS URL to a publicly accessible CSV file.

> [!WARNING]
> When using a URL path, column validation is skipped during `bruin validate`. Column mismatches will be caught at runtime.


####  Examples: Load csv into a Duckdb database

The examples below show how to load a CSV into a DuckDB database.
```yaml
name: dashboard.hello
type: duckdb.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```


## Lakehouse Support <Badge type="warning" text="beta" />

DuckDB can query [Iceberg](https://duckdb.org/docs/extensions/iceberg) and [DuckLake](https://duckdb.org/docs/extensions/ducklake) tables through its native extensions. DuckLake supports DuckDB or Postgres catalogs with S3-backed storage.

### Connection

Add the `lakehouse` block to your DuckDB connection in `.bruin.yml`:

```yaml
connections:
  duckdb:
    - name: "example-conn"
      path: "./path/to/duckdb.db"
      lakehouse:
        format: <iceberg|ducklake>
        catalog:
          type: <glue|postgres|duckdb>
          auth: { ... } # optional
        storage:
          type: <s3>
          auth: { ... } # optional
```

<br>

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `format` | string | Yes | Table format: `iceberg` or `ducklake` |
| `catalog` | object | Yes | Catalog configuration (Glue for Iceberg, DuckDB/Postgres for DuckLake) |
| `storage` | object | No | Storage configuration (required for DuckLake) |

---
### Supported Lakehouse Formats



#### DuckLake

| Catalog \ Storage | S3 |
|-------------------|----|
| DuckDB | <span class="lh-check" aria-label="supported"></span> |
| SQLite   |  |
| Postgres | <span class="lh-check" aria-label="supported"></span> |
| MySQL    |  |



#### Iceberg

| Catalog \ Storage | S3 |
|-------------------|----|
| Glue | <span class="lh-check" aria-label="supported"></span> |


For background, see DuckDB's [lakehouse format overview](https://duckdb.org/docs/stable/lakehouse_formats).

---
### Catalog Options
For guidance, see DuckLake's [choosing a catalog database](https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database).


#### Glue

```yaml
catalog:
  type: glue
  catalog_id: "123456789012"
  region: "us-east-1"
  auth:
    access_key: "${AWS_ACCESS_KEY_ID}"
    secret_key: "${AWS_SECRET_ACCESS_KEY}"
    session_token: "${AWS_SESSION_TOKEN}" # optional
```

#### Postgres


```yaml
catalog:
  type: postgres
  host: "localhost"
  port: 5432 # optional - default: 5432
  database: "ducklake_catalog"
  auth:
    username: "ducklake_user"
    password: "ducklake_password"
```

#### DuckDB

```yaml
catalog:
  type: duckdb
  path: "metadata.ducklake"
```

`catalog.path` should point to the DuckLake metadata file.

Note that if you are using DuckDB as your catalog database, you're limited to a single client.


---
### Storage Options

#### S3

Bruin currently only supports explicit AWS credentials in the `auth` block.
Session tokens are supported for temporary credentials (AWS STS).

```yaml
storage:
  type: s3
  path: "s3://my-ducklake-warehouse/path" # required for DuckLake, optional for Iceberg
  region: "us-east-1"
  auth:
    access_key: "${AWS_ACCESS_KEY_ID}"
    secret_key: "${AWS_SECRET_ACCESS_KEY}"
    session_token: "${AWS_SESSION_TOKEN}" # optional
```

---
### Usage

Bruin makes the lakehouse catalog active for your session and ensures a default `main` schema is available (cannot create Iceberg on S3 schemas/tables, so they must already exist). You can query tables with or without a schema:

```sql
SELECT * FROM my_table;
```

You can also use the fully qualified path:

```sql
SELECT * FROM iceberg_catalog.main.my_table;
```

> [!NOTE]
> Unqualified table names resolve to the `main` schema of the active catalog. Use `<schema>.<table>` to target non-main schemas.

#### Example Asset

```bruin-sql
/* @bruin
name: lakehouse_example
type: duckdb.sql
connection: example-conn
@bruin */

SELECT SUM(amount) as total_sales
FROM orders;
```
