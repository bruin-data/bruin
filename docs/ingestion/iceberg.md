# Apache Iceberg

[Apache Iceberg](https://iceberg.apache.org/) is an open table format for large analytic datasets, bringing ACID transactions, schema evolution, and time travel to data stored in object storage.

Bruin supports Iceberg as a **destination** for [Ingestr assets](/assets/ingestr), so you can load data into Iceberg tables managed by a catalog of your choice.

> [!NOTE]
> Iceberg is supported as a **destination only** (writing data in). To query existing Iceberg tables, use the DuckDB [lakehouse support](/platforms/duckdb#lakehouse-support).

## Supported catalogs and storage

| Catalog (`catalog.type`) | Storage (`storage.type`) |
|---|---|
| `glue`, `sqlite`, `postgres`, `rest`, `hive`, `hadoop`, `sql` | `s3`, `gcs`, `local` |

Table data is written to AWS S3 or any S3-compatible store (MinIO, Cloudflare R2, GCS interop), to Google Cloud Storage natively, or to the local filesystem.

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
            rest_use_ssl: true                # optional — use HTTPS (default is HTTP); set true for hosted catalogs (Polaris, Unity, Lakekeeper, Tabular)
            credential: "${ICEBERG_REST_CREDENTIAL}"   # optional — REST auth, if the catalog requires it
            token: "${ICEBERG_REST_TOKEN}"             # optional — bearer token, alternative to credential
```

> [!TIP]
> A REST catalog is a **running server** you start and configure yourself — it holds its own warehouse location, storage backend, and credentials, and usually writes the table metadata to storage. Your connection's `storage` block still supplies the credentials Bruin uses to write the **data files**.
> OAuth2 options such as `oauth2-server-uri` and `scope` go in the top-level [`properties`](#table-options) block.

**Hive**
```yaml
          catalog:
            type: hive                        # required
            host: "metastore.internal"        # required
            port: 9083                        # optional
```

> [!WARNING]
> The Hive metastore reaches storage **independently** — your connection's `storage` credentials configure only the Bruin client and never reach the metastore. On the metastore side, use an `s3a://` warehouse, put the right connector jar on its classpath (`hadoop-aws` for S3, `gcs-connector` for GCS), and set the matching `fs.s3a.*` / `fs.gs.*` properties in its `core-site.xml`. Without this it fails with `No FileSystem for scheme "s3"`. A `file://` warehouse needs none of this.

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

For the `postgres` catalog, ingestr forwards standard PostgreSQL connection parameters to the catalog database connection, so you can secure or tune it via the top-level [`properties`](#table-options) block — a managed database (Neon, RDS, Cloud SQL) usually needs TLS:

```yaml
          catalog:
            type: postgres
            host: "metadata-db.internal"
            database: "iceberg_catalog"
            auth:
              username: "iceberg_user"
              password: "${PG_PASSWORD}"
          properties:
            sslmode: "require"                # e.g. require, verify-full
            sslrootcert: "/path/to/ca.pem"    # optional
```

Recognized connection parameters: `sslmode`, `sslcert`, `sslkey`, `sslrootcert`, `sslpassword`, `sslcrl`, `sslcrldir`, `sslsni`, `sslcompression`, `requiressl`, `connect_timeout`, `application_name`, `fallback_application_name`, `target_session_attrs`, `tcp_user_timeout`, `options`, `service`, `servicefile`, `passfile`, `krbsrvname`, and `replication`. Any other property is treated as an Iceberg/storage option, not a database connection setting. This forwarding applies only to `type: postgres`; for the generic `sql` catalog, embed these inside the `uri` connection string instead (e.g. `postgresql://…?sslmode=require`).

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

> [!WARNING]
> The Hadoop catalog only commits atomically on a real local or HDFS filesystem. For an object-storage warehouse (`s3://…`) you must add `allow-unsafe-commits: "true"` to `properties`, otherwise the connection fails.

**SQL** (advanced)
```yaml
          catalog:
            type: sql                         # required
            uri: "postgresql://user:pass@host:5432/db"   # required — catalog connection string
            driver: "pgx"                     # required — database/sql driver (e.g. pgx, sqlite)
            dialect: "postgres"               # required — SQL dialect (e.g. postgres, sqlite)
```

> The generic `sql` catalog is only needed for backends other than SQLite/Postgres — for those, use the dedicated `sqlite`/`postgres` catalog types, which set `driver`/`dialect` for you.

### Storage options

`type` (`s3`, `gcs`, or `local`) is **optional** — the backend is normally inferred from the warehouse **scheme** (`s3://` → s3, `gs://` → gcs, `file://` → local). You only need `type` to disambiguate the `bucket`/`prefix` form (which carries no scheme), where it selects `s3://` (the default) vs `gs://`. The **warehouse location** — the root under which table data files are written — can be given two ways (mutually exclusive): a full URI in `path` (`s3://…`, `gs://…`, or a filesystem path), or a `bucket` (+ optional `prefix`). Leave both empty to inherit the catalog's own warehouse (Glue, REST, and SQL catalogs supply one); the `region`/`endpoint`/`use_ssl`/`auth` credentials are still used to write the data files either way.

**AWS S3**

```yaml
          storage:
            type: s3
            path: "s3://my-company-lake/warehouse"      # full warehouse URI
            region: "us-east-1"
            auth:
              access_key: "${AWS_ACCESS_KEY_ID}"
              secret_key: "${AWS_SECRET_ACCESS_KEY}"
              session_token: "${AWS_SESSION_TOKEN}"     # optional — temporary/STS credentials
```

**AWS S3 with `bucket` + `prefix`** (alternative to `path`)

```yaml
          storage:
            type: s3
            bucket: "my-company-lake"                   # builds s3://my-company-lake/warehouse
            prefix: "warehouse"                         # optional
            region: "us-east-1"
            auth:
              access_key: "${AWS_ACCESS_KEY_ID}"
              secret_key: "${AWS_SECRET_ACCESS_KEY}"
```

**S3-compatible (MinIO, Cloudflare R2, …)** — set `endpoint`, and `use_ssl: false` for a plain-HTTP local store:

```yaml
          storage:
            type: s3
            path: "s3://warehouse"
            endpoint: "localhost:9000"                  # the S3-compatible endpoint
            use_ssl: false
            region: "us-east-1"
            auth:
              access_key: "${MINIO_ACCESS_KEY}"
              secret_key: "${MINIO_SECRET_KEY}"
```

> [!TIP]
> Reaching **GCS over its S3 interoperability API** needs S3 compatibility mode, which is not set for you — add `s3.compat-mode: "true"` to the top-level [`properties`](#table-options) block. Without it Google rejects the headers the AWS SDK signs by default and the write fails with `SignatureDoesNotMatch`. MinIO and R2 accept those headers, so they need nothing extra.

**Google Cloud Storage (native)** — `type: gcs` with a `gs://` warehouse and a service-account key (`bucket` + `prefix` works too):

```yaml
          storage:
            type: gcs
            path: "gs://my-company-lake/warehouse"      # or: bucket + prefix
            key_file: "/path/to/service-account.json"   # SA key file (or key_json for inline JSON)
```

Leave `key_file`/`key_json` empty to use Application Default Credentials.

**GCS via the S3 interop endpoint (HMAC keys)** — use `type: s3` with the Google endpoint and HMAC credentials:

```yaml
          storage:
            type: s3
            path: "s3://my-gcs-bucket/warehouse"
            endpoint: "storage.googleapis.com"
            region: "auto"                              # required, but unused with an endpoint set
            auth:
              access_key: "${GCS_HMAC_KEY}"
              secret_key: "${GCS_HMAC_SECRET}"
          properties:
            s3.compat-mode: "true"                      # required — see the tip above
            s3.force-virtual-addressing: "false"
```

**Local filesystem** — `type: local` with a filesystem path (a fully local SQLite/Hadoop setup):

```yaml
          storage:
            type: local
            path: "/tmp/iceberg-warehouse"              # becomes file:///tmp/iceberg-warehouse
```

### Table options

- `create_namespace`: create the destination namespace if it doesn't exist (defaults to `true`).
- `table_location`: explicit table location; supports `{namespace}`, `{table}`, and `{identifier}` placeholders.
- `table_path`: path under the warehouse, e.g. `{namespace}/{table}`.
- `table_properties`: Iceberg table properties, e.g. `write.format.default: parquet`.
- `properties`: any additional, non-secret catalog options passed through to the Iceberg URI verbatim (e.g. `allow-unsafe-commits`, `s3.compat-mode`, `oauth2-server-uri` — see the notes on the relevant catalog/storage above).

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
