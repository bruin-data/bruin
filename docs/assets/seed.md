# Seed Assets
Seeds are CSV-files that contain data that is prepared outside of your pipeline that will be loaded into your data platform. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the destination platform accurately.

You can define seed assets in a file ending with `.asset.yaml`:
```yaml
name: dashboard.hello
type: duckdb.seed

parameters:
    path: seed.csv
```

The `type` key in the configuration defines what platform to run the query against.

You can see the "Data Platforms" on the left sidebar to see supported types.

## Parameters

The `parameters` key in the configuration defines the parameters for the seed asset.

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `path` | Yes | - | Path to the CSV file to load. Can be a relative path (relative to the asset definition file) or a URL pointing to a publicly accessible CSV file. |
| `enforce_schema` | No | `true` | When `true`, enforces column types defined in the `columns` section. Set to `false` to let ingestr infer types from the CSV. |

::: warning Column validation skipped for URLs
When using a URL path, column validation is skipped during `bruin validate`. Column mismatches will be caught at runtime when `bruin run` fetches the data.
:::

::: tip
URL-based seeds work with all supported platforms (DuckDB, BigQuery, Snowflake, PostgreSQL, etc.). The URL must be publicly accessible without authentication.
:::

##  Examples
The examples below show how to load a CSV into a DuckDB & BigQuery database.

### Simplest: Load csv into a Duckdb
```yaml
name: dashboard.hello
type: duckdb.seed

parameters:
    path: hello.csv
```

Example CSV:
```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```

This operation will load the CSV into a table called `seed.raw` in the DuckDB database.

### Adding quality checks
You can attach quality checks to seed assets the same way you do for other assets.

```yaml
name: dashboard.hello
type: duckdb.seed

parameters:
    path: hello.csv

columns:
  - name: name
    type: string
    checks:
      - name: not_null
      - name: unique
```

The example above ensures that the `name` column contains unique and non-null values after the CSV is loaded.

### Loading from a URL
You can load data directly from a public URL:

```yaml
name: taxi_zones.lookup
type: duckdb.seed

parameters:
    path: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

This will download the CSV from the URL and load it into the database at runtime.

### Enforcing column types
By default, seed assets enforce the column types defined in the `columns` section. This ensures your destination table has the correct schema.

```yaml
name: dashboard.contacts
type: bigquery.seed

parameters:
    path: contacts.csv

columns:
  - name: id
    type: integer
    primary_key: true
  - name: name
    type: string
  - name: email
    type: string
  - name: created_at
    type: timestamp
```

When columns are defined, Bruin passes type hints to ingestr, ensuring the destination table uses the specified types rather than inferring them from the CSV content.

### Disabling schema enforcement
If you prefer to let ingestr infer column types from the CSV content, you can disable schema enforcement:

```yaml
name: dashboard.raw_data
type: duckdb.seed

parameters:
    path: data.csv
    enforce_schema: false

columns:
  - name: id
    checks:
      - name: not_null
```

With `enforce_schema: false`, the column types will be inferred from the CSV data. You can still define columns for quality checks and documentation without enforcing specific types.
