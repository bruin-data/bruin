# DuckDB <Badge type="warning" text="beta" />

DuckDB can query Iceberg and DuckLake tables through its native extensions, enabling local analytics on lakehouse data. See DuckDB's [lakehouse format overview](https://duckdb.org/docs/stable/lakehouse_formats).

## Connection

Add the `lakehouse` block to your DuckDB connection in `.bruin.yml`:

```yaml
connections:
  duckdb:
    - name: "analytics"
      path: "./analytics.db"
      lakehouse:
        format: iceberg | ducklake
        catalog:
          type: glue | postgres
          auth: { ... } # optional
        storage:
          type: s3
          auth: { ... } 
```

## Configuration Reference

### `lakehouse`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `format` | string | Yes | Table format: `iceberg` or `ducklake` |
| `catalog` | object | Yes | Catalog configuration |
| `storage` | object | No | Storage configuration (required for DuckLake) |

## Supported Configurations

| Component | Supported | Notes |
|-----------|-----------|-------|
| **Formats** | Iceberg, DuckLake | Delta planned |
| **Catalogs** | AWS Glue, Postgres | Glue for Iceberg, Postgres for DuckLake |
| **Storage** | S3 | Required for DuckLake, optional for Iceberg |

### Catalogs

#### Glue catalog

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

#### Postgres catalog (DuckLake)

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

### Storage

#### S3 storage

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

Bruin attaches the lakehouse as `iceberg_catalog` or `ducklake_catalog`, creates a `main` schema, and runs `USE <catalog>` to set it active. You can query tables with or without a schema:

```sql
SELECT * FROM users;
```

You can also use the fully qualified path:

```sql
SELECT * FROM iceberg_catalog.demo.users;
```

> [!NOTE]
> Unqualified table names resolve to the `main` schema of the active catalog. Use `schema.table` to target non-main schemas.

### Example Asset

```bruin-sql
/* @bruin
name: daily_sales
type: duckdb.sql
connection: analytics
@bruin */

SELECT
    date_trunc('day', order_date) as day,
    SUM(amount) as total_sales
FROM demo.orders
WHERE order_date >= '{{ start_date }}'
GROUP BY 1;
```

## AWS Credentials

> [!WARNING]
> Avoid hardcoding credentials. Use environment variables or a secrets manager.

Bruin currently only supports explicit AWS credentials in the `auth` block. If `auth` is omitted, Bruin will not create DuckDB secrets; you must configure secrets in DuckDB separately.

Session tokens are supported for temporary credentials (AWS STS).

> [!NOTE]
> DuckLake uses a Postgres catalog; ensure the DuckDB process can reach the Postgres host and credentials.

### Required IAM Permissions

**Glue Catalog:**
```
glue:GetDatabase
glue:GetDatabases
glue:GetTable
glue:GetTables
```

**S3 Storage:**
```
s3:GetObject
s3:ListBucket
```

## Limitations

- One lakehouse per DuckDB connection
- Only Iceberg + Glue + S3 and DuckLake + Postgres + S3 supported right now
- Lakehouse setup (extensions, secrets, attach, use) runs per connection
