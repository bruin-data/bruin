# DuckDB <Badge type="warning" text="beta" />

DuckDB can query Iceberg and DuckLake tables through its native extensions, enabling local analytics on lakehouse data. See DuckDB's [lakehouse format overview](https://duckdb.org/docs/stable/lakehouse_formats).

## Connection

Add the `lakehouse` block to your DuckDB connection in `.bruin.yml`:

```yaml
connections:
  duckdb:
    - name: "example-conn"
      path: "./path/to/duckdb.db"
      lakehouse:
        format: <iceberg|ducklake>
        catalog:
          type: <glue|postgres>
          auth: { ... } # optional
        storage:
          type: <s3>
          auth: { ... } # optional
```

<br>

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `format` | string | Yes | Table format: `iceberg` or `ducklake` |
| `catalog` | object | Yes | Catalog configuration (Glue for Iceberg, Postgres for DuckLake) |
| `storage` | object | No | Storage configuration (required for DuckLake) |

## Supported Combinations

### Iceberg

| Catalog \ Storage | S3 | GCS |
|-------------------|----|-----|
| Glue | <span class="lh-check" aria-label="supported"></span> | |


### DuckLake

| Catalog \ Storage | S3 | GCS |
|-------------------|----|-----|
| DuckDB | <span class="lh-check" aria-label="supported"></span> |  |
| SQLite   |  |  |
| Postgres |  |  |
| MySQL    |  |  |



## Catalog Options

### Glue

```yaml
catalog:
  type: glue
  catalog_id: "123456789012"
  region: "us-east-1" # optional
  auth:
    access_key: "${AWS_ACCESS_KEY_ID}"
    secret_key: "${AWS_SECRET_ACCESS_KEY}"
    session_token: "${AWS_SESSION_TOKEN}" # optional
```

### Postgres


```yaml
catalog:
  type: postgres
  host: "localhost"
  port: 5432 # optional
  database: "ducklake_catalog"
  auth:
    username: "ducklake_user"
    password: "ducklake_password"
```

## Storage Options

### S3

Bruin currently only supports explicit AWS credentials in the `auth` block.
Session tokens are supported for temporary credentials (AWS STS).

```yaml
storage:
  type: s3
  path: "s3://my-ducklake-warehouse/path" # required for DuckLake, optional for Iceberg
  region: "us-east-1" # optional
  auth:
    access_key: "${AWS_ACCESS_KEY_ID}"
    secret_key: "${AWS_SECRET_ACCESS_KEY}"
    session_token: "${AWS_SESSION_TOKEN}" # optional
```

## Usage

Bruin makes the lakehouse catalog active for your session and ensures a default `main` schema is available (cannot create Iceberg on S3 schemas/tables, so they must already exist). You can query tables with or without a schema:

```sql
SELECT * FROM users;
```

You can also use the fully qualified path:

```sql
SELECT * FROM iceberg_catalog.demo.users;
```

> [!NOTE]
> Unqualified table names resolve to the `main` schema of the active catalog. Use `<schema>.<table>` to target non-main schemas.

### Example Asset

```bruin-sql
/* @bruin
name: daily_sales
type: duckdb.sql
connection: example-conn
@bruin */

SELECT
    date_trunc('day', order_date) as day,
    SUM(amount) as total_sales
FROM demo.orders
WHERE order_date >= '{{ start_date }}'
GROUP BY 1;
```
