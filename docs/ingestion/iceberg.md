# Apache Iceberg

[Apache Iceberg](https://iceberg.apache.org/) is an open table format for large analytic datasets, bringing ACID transactions, schema evolution, and time travel to data stored in object storage.

Bruin supports Iceberg as a **destination** for [Ingestr assets](/assets/ingestr), so you can load data into Iceberg tables managed by a catalog of your choice.

> [!NOTE]
> Iceberg is supported as a **destination only** (writing data in). To query existing Iceberg tables, use the DuckDB [lakehouse support](/platforms/duckdb#lakehouse-support).

## Supported catalogs and storage

| Catalog (`catalog.type`) | Storage (`storage.type`) |
|---|---|
| `glue`, `sqlite`, `postgres`, `rest`, `hive`, `hadoop`, `sql` | `s3` (S3-compatible) |

Iceberg tables are written to **S3-compatible** storage (AWS S3, MinIO, Cloudflare R2, etc.).

## Step 1: Add a connection to .bruin.yml

An Iceberg connection has a `catalog` block (where table metadata lives) and a `storage` block (where the data files live):

```yaml
    connections:
      iceberg:
        - name: "my-iceberg"
          catalog_name: "analytics"       # optional, defaults to "ingestr"
          catalog:
            type: glue
            catalog_id: "123456789012"
            region: "us-east-1"
            auth:
              access_key: "${AWS_ACCESS_KEY_ID}"
              secret_key: "${AWS_SECRET_ACCESS_KEY}"
          storage:
            type: s3
            path: "s3://my-company-lake/warehouse"
            region: "us-east-1"
            auth:
              access_key: "${AWS_ACCESS_KEY_ID}"
              secret_key: "${AWS_SECRET_ACCESS_KEY}"
```

### Catalog options

Each catalog type takes different fields. Use the matching `catalog:` block below (the `storage:` block is the same in every case).

**Glue**
```yaml
          catalog:
            type: glue                        # required
            catalog_id: "123456789012"        # optional
            region: "us-east-1"               # optional
            auth:                             # optional — falls back to the storage credentials
              access_key: "${AWS_ACCESS_KEY_ID}"
              secret_key: "${AWS_SECRET_ACCESS_KEY}"
```

**REST**
```yaml
          catalog:
            type: rest                        # required
            host: "catalog.internal"          # required
            port: 8181                        # optional
            credential: "${ICEBERG_REST_CREDENTIAL}"   # optional — REST auth, if the catalog requires it
            token: "${ICEBERG_REST_TOKEN}"             # optional — bearer token, alternative to credential
```

**Hive**
```yaml
          catalog:
            type: hive                        # required
            host: "metastore.internal"        # required
            port: 9083                        # optional
```

**Postgres**
```yaml
          catalog:
            type: postgres                    # required
            host: "metadata-db.internal"      # required
            port: 5432                        # optional
            database: "iceberg_catalog"       # optional
            auth:                             # optional
              username: "iceberg_user"
              password: "${PG_PASSWORD}"
```

**SQLite**
```yaml
          catalog:
            type: sqlite                      # required
            path: "/path/to/catalog.db"       # required
```

**Hadoop**
```yaml
          catalog:
            type: hadoop                      # required
            path: "/warehouse"                # required — warehouse directory
```

**SQL** (advanced)
```yaml
          catalog:
            type: sql                         # required
            uri: "postgresql://user:pass@host:5432/db"   # required — catalog connection string
```

### Storage options

```yaml
          storage:
            type: s3                                    # required
            path: "s3://my-company-lake/warehouse"      # optional — the Iceberg warehouse location
            region: "us-east-1"                         # optional
            endpoint: "localhost:9000"                  # optional — for S3-compatible stores (MinIO, R2, ...)
            use_ssl: false                              # optional — false for plain-HTTP local storage
            auth:
              access_key: "${AWS_ACCESS_KEY_ID}"        # required
              secret_key: "${AWS_SECRET_ACCESS_KEY}"    # required
              session_token: "${AWS_SESSION_TOKEN}"     # optional
```

The **warehouse location** — the `s3://<bucket>/<prefix>` root under which table data files are written — can be given two ways (they are mutually exclusive):

- as a full URI in `path`, e.g. `path: "s3://my-company-lake/warehouse"`; or
- as a separate `bucket` (and optional `prefix`), e.g. `bucket: "my-company-lake"` with `prefix: "warehouse"`.

The location is **optional**: when all of `path`/`bucket`/`prefix` are omitted, the warehouse is taken from the catalog itself (Glue, REST, and SQL catalogs supply their own warehouse location). The `region`, `endpoint`, `use_ssl`, and `auth` credentials are still used to read and write the S3 data files regardless of where the warehouse location comes from.

### Table options

- `create_namespace`: create the destination namespace if it doesn't exist (defaults to `true`).
- `table_location`: explicit table location; supports `{namespace}`, `{table}`, and `{identifier}` placeholders.
- `table_path`: path under the warehouse, e.g. `{namespace}/{table}`.
- `table_properties`: Iceberg table properties, e.g. `write.format.default: parquet`.
- `properties`: any additional, non-secret catalog options passed through to the Iceberg URI verbatim.

> [!WARNING]
> `properties` values are **not** redacted from run logs. Put credentials in the dedicated fields (`auth`, `credential`, `token`, `uri`), never in `properties`.

## Step 2: Create an asset file

```yaml
name: analytics.events
type: ingestr

parameters:
  source_connection: my-postgres
  source_table: 'public.events'

  destination: iceberg
  destination_connection: my-iceberg
```

Use an Iceberg table identifier (`namespace.table`) as the destination table (the asset `name`). For nested namespaces use dot-separated identifiers, e.g. `lake.analytics.events`.

## Step 3: [Run](/commands/run) the asset

```bash
bruin run assets/events.asset.yml
```

## Supported write strategies

`replace`, `append`, `merge`, `delete+insert`, and `truncate+insert`, configured via the asset's [materialization](/assets/materialization) settings.
