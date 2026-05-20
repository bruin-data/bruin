# Seed Assets

Seeds are files that contain data prepared outside of your pipeline and loaded into your data platform. Bruin supports seed assets natively, allowing you to drop a CSV, Parquet, JSON, JSONL/NDJSON or Avro file in your pipeline and load it to the destination accurately.

You can define seed assets in a file ending with `.asset.yml` or `.asset.yaml`:

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
| `path` | Yes | - | Path to the seed file to load. Can be a relative path (relative to the asset definition file) or a URL pointing to a publicly accessible file. |
| `file_type` | No | inferred from file extension | Explicit format override. One of `csv`, `parquet`, `json`, `jsonl`, `ndjson`, `avro`. Useful when the file has a non-standard extension. |
| `enforce_schema` | No | `true` | When `true`, enforces column types defined in the `columns` section. Set to `false` to let the loader infer types from the file. |

## Supported file formats

Bruin detects the format from the file extension and selects the appropriate loader:

| Extension | Format |
| --- | --- |
| `.csv` | CSV |
| `.parquet`, `.pq` | Parquet |
| `.jsonl` | JSONL |
| `.ndjson` | NDJSON |
| `.json` | JSON |
| `.avro` | Avro |

Use `file_type` to override the inferred format, for example when the file has no extension or uses a non-standard one.

::: warning Column validation skipped for non-CSV files and URLs
For Parquet, JSON, JSONL, NDJSON, Avro and URL-based seeds, column validation is skipped during `bruin validate`. Mismatches are caught at runtime instead.
:::

::: tip
URL-based seeds work with all supported platforms (DuckDB, BigQuery, Snowflake, PostgreSQL, etc.). The URL must be publicly accessible without authentication.
:::

## Examples

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

### Loading Parquet, JSON, JSONL or Avro

Drop the file in your pipeline and Bruin will pick the right loader from the extension:

```yaml
name: dashboard.orders
type: duckdb.seed

parameters:
    path: orders.parquet
```

To override the inferred format (for example when a Parquet file is named `data.bin`), set `file_type`:

```yaml
name: dashboard.orders
type: duckdb.seed

parameters:
    path: data.bin
    file_type: parquet
```

### Enforcing column types

By default, seed assets enforce the column types defined in the `columns` section. This ensures your destination table has the correct schema.

```yaml
name: dashboard.contacts
type: bq.seed

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
