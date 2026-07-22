# Ingestr Assets

[Ingestr](https://github.com/bruin-data/ingestr) is a CLI tool that allows you to move data between platforms. Bruin supports `ingestr` natively as an asset type.

Using Ingestr, you can move data from:

* your production databases like:
  * MSSQL
  * MySQL
  * Oracle
* your daily tools like:
  * Notion
  * Google Sheets
  * Airtable
* from other platforms such as:
  * Hubspot
  * Salesforce
  * Google Analytics
  * Facebook Ads
  * Google Ads

to your data warehouses:

* AWS Athena
* Google BigQuery
* ClickHouse
* Databricks
* DuckDB
* Microsoft SQL Server
* Oracle
* Postgres
* StarRocks
* Snowflake
* AWS Redshift
* Azure Synapse
* other ingestr destinations when the asset points at an explicit Bruin connection

> [!INFO]
> See the ingestr [platform catalog](https://getbruin.com/docs/ingestr/supported-sources/platforms.html), [ingest command reference](https://getbruin.com/docs/ingestr/commands/ingest.html), and [incremental loading guide](https://getbruin.com/docs/ingestr/getting-started/incremental-loading.html) for the upstream source, destination, and strategy behavior.

## Asset Structure

```yaml
name: string
type: ingestr
connection: string # optional, by default uses the default connection for destination platform in pipeline.yml
materialization: # optional, preferred for destination write strategy
  type: table
  strategy: create+replace | append | merge | delete+insert | truncate+insert
  incremental_key: string
  partition_by: string
  cluster_by:
    - string
parameters:
  source: string # optional, used when inferring the source from connection is not enough, e.g. GCP connection + GSheets source
  source_connection: string
  source_table: string
  destination: string # logical destination type; required unless connection or destination_connection is set
  destination_connection: string # optional, used instead of destination default connection when connection is not set
  
  # optional
  version: v0 | v1 | vMAJOR.MINOR.PATCH
  incremental_strategy: replace | append | merge | delete+insert | truncate+insert # legacy alternative to materialization.strategy
  incremental_key: string # legacy alternative to materialization.incremental_key
  schema_contract: evolve | freeze | discard_row | discard_value
  schema_naming: auto | default | direct | snake_case
  sql_backend: pyarrow | sqlalchemy
  page_size: integer
  loader_file_format: jsonl | csv | parquet
  loader_file_size: integer
  extract_parallelism: integer
  extract_partition_by: string
  extract_partition_interval: string | integer
  sql_limit: integer
  sql_exclude_columns: string
  no_inference: true|false
  mask: string
  trim_whitespace: true|false
  pipelines_dir: string
  staging_bucket: string
  staging_dataset: string
  stream: true|false
  flush_interval: string
  flush_records: integer
  enforce_schema: true|false # Will ensure that the columns defined in the asset are present in the destination and with the desired types (see https://getbruin.com/docs/bruin/assets/columns.html)
  cdc: "true"|"false"
  cdc_mode: stream | batch # deprecated, use stream instead
  cdc_sql_capture: cdc | change_tracking
  cdc_publication: string
  cdc_slot: string
  cdc_server_id: string
  cdc_tls: string
  cdc_grpc_port: string
  cdc_grpc_host: string
  cdc_grpc_tls: string
  cdc_capture_instance: string
  cdc_poll_interval: string
  cdc_max_await_time: string
  cdc_schema_sample_size: integer
  cdc_dest_schema: string
  cdc_state_id: string
  cdc_stream_metrics_addr: string
  cdc_stream_flush_interval: string
  cdc_stream_flush_records: integer
```

## Parameter reference

| Parameter | Required | Ingestr flag | Description |
| --- | --- | --- | --- |
| `connection` | No | `--dest-uri` | Destination connection; defaults to the pipeline's connection for the asset. |
| `source_connection` | Yes | `--source-uri` | Name of the configured source connection. Bruin resolves it to the URI passed to Ingestr. |
| `source` | No | _n/a_ | Overrides the inferred source type. For example, set `gsheets` when reusing a BigQuery connection for Google Sheets. |
| `source_table` | Yes | `--source-table` | Table, sheet, or resource identifier to pull from the source. |
| `file_type` | No | `--source-table` suffix | Appended to the `source_table` as `table#type` for connectors that need a file format hint (`csv`, `jsonl`, `parquet`). |
| `version` | No | _n/a_ | Selects the version of ingestr to install and use. Valid options are bare family markers such as `v1` or `v0`, or a full version pin such as `v1.0.71`. |
| `materialization` | No | `--incremental-*`, `--partition-by`, `--cluster-by` | Preferred way to define destination write behavior. Supports `type: table` with `create+replace`, `append`, `merge`, `delete+insert`, and `truncate+insert`. |
| `destination` | Unless `connection` or `destination_connection` is set | _n/a_ | Logical destination type used for default connection inference. When `connection` and `destination_connection` are omitted, Bruin uses this value to choose the pipeline default destination connection. |
| `destination_connection` | No | _n/a_ | Named destination connection to use when `connection` is omitted. This overrides default connection inference from `destination`. |
| `incremental_strategy` | No | `--incremental-strategy` | Passes the incremental loading strategy (`replace`, `append`, `merge`, `delete+insert`, or `truncate+insert`) to Ingestr. Prefer `materialization.strategy` for new assets. |
| `incremental_key` | No | `--incremental-key` | Column that determines incremental progress. When the column is defined with type `date`, Bruin also forwards it through the `--columns` option so Ingestr treats it as a date field. |
| `partition_by` | No | `--partition-by` | Comma-separated list of destination columns to partition by. |
| `cluster_by` | No | `--cluster-by` | Comma-separated list of destination clustering keys. |
| `schema_contract` | No | `--schema-contract` | Controls how Ingestr handles schema changes (`evolve`, `freeze`, `discard_row`, `discard_value`). |
| `loader_file_format` | No | `--loader-file-format` | Overrides the loader file format (`jsonl`, `csv`, `parquet`). |
| `loader_file_size` | No | `--loader-file-size` | Sets the maximum loader file size accepted by Ingestr. |
| `sql_backend` | No | `--sql-backend` | Selects the SQL backend Ingestr should use (`pyarrow` or `sqlalchemy`). |
| `schema_naming` | No | `--schema-naming` | Controls how Ingestr normalizes schema names. Accepted values match the [Ingestr CLI.](https://getbruin.com/docs/ingestr/commands/ingest.html#optional-flags) |
| `page_size` | No | `--page-size` | Sets the fetch page size for SQL sources. |
| `extract_parallelism` | No | `--extract-parallelism` | Limits the number of concurrent extraction workers. |
| `extract_partition_by` | No | `--extract-partition-by` | Source date/time or integer column used to split bounded extraction into parallel windows. |
| `extract_partition_interval` | No | `--extract-partition-interval` | Width of each extract partition window as a duration (such as `1h` or `7d`), integer step, or `auto`. Ingestr defaults to `auto` when `extract_partition_by` is set. |
| `sql_reflection_level` | No | `--sql-reflection-level` | Tunes the amount of schema reflection performed against the source. |
| `sql_limit` | No | `--sql-limit` | Applies a `LIMIT` clause when extracting from the source. |
| `sql_exclude_columns` | No | `--sql-exclude-columns` | List of columns to skip during extraction. |
| `no_inference` | No | `--no-inference` | Uses `columns` as the source schema for schema-less sources instead of inferring types. |
| `mask` | No | `--mask` | Adds a column masking rule such as `email:hash`. |
| `pipelines_dir` | No | `--pipelines-dir` | Directory where Ingestr stores pipeline metadata. |
| `staging_bucket` | No | `--staging-bucket` | Overrides the staging bucket that Ingestr uses for intermediate files. |
| `staging_dataset` | No | `--staging-dataset` | Dataset/schema to use for staging tables. |
| `trim_whitespace` | No | `--trim-whitespace` | Trims leading and trailing whitespace from extracted string values when set to `true`. |
| `stream` | No | `--stream` | Enables continuous ingestion. Works for CDC sources (`cdc: true`) and message-broker sources (such as Kafka). A streaming asset never exits on its own and must be launched with `bruin run --stream` (see [Streaming assets](#streaming-assets)). |
| `flush_interval` | No | `--flush-interval` | Flush interval for streaming mode, such as `30s`. CDC assets can set `cdc_stream_flush_interval` instead, which takes precedence. |
| `flush_records` | No | `--flush-records` | Number of buffered records that triggers a flush in streaming mode. CDC assets can set `cdc_stream_flush_records` instead, which takes precedence. |
| `enforce_schema` | No | `--columns` | When set to `true`, enforces the column types defined in the asset's `columns` section. Ingestr will create or update the destination table with the specified schema. |
| `cdc` | No | source URI scheme | Enables Bruin's CDC URI handling for PostgreSQL, MySQL/MariaDB, Vitess, PlanetScale, MongoDB, and SQL Server (log-based CDC and Change Tracking) sources when set to `"true"`. CDC assets must use `merge`; Bruin sets it automatically when omitted and rejects other strategies. |
| `cdc_mode` | No | `--stream` flag | **Deprecated** — use `stream` instead. `cdc_mode: stream` is equivalent to `stream: true`; `cdc_mode: batch` is the default (omit it). |
| `cdc_sql_capture` | No | source URI scheme | SQL Server capture mechanism, either `cdc` (log-based, `mssql+cdc`; default) or `change_tracking` (`mssql+ct`). |
| `cdc_publication` | No | source URI query | PostgreSQL publication name. |
| `cdc_slot` | No | source URI query | PostgreSQL replication slot name. |
| `cdc_server_id` | No | source URI query | MySQL-family binlog replication server ID. |
| `cdc_tls` | No | source URI query | MySQL-family CDC TLS setting. |
| `cdc_grpc_port` | No | source URI query | Vitess VStream gRPC port override. |
| `cdc_grpc_host` | No | source URI query | Vitess VStream gRPC host override. |
| `cdc_grpc_tls` | No | source URI query | Vitess VStream TLS setting. |
| `cdc_capture_instance` | No | source URI query | SQL Server log-based CDC capture instance name. |
| `cdc_poll_interval` | No | source URI query | SQL Server log-based CDC poll interval, such as `10s`. |
| `cdc_max_await_time` | No | source URI query | MongoDB change-stream maximum await time, such as `5s`. |
| `cdc_schema_sample_size` | No | source URI query | MongoDB number of documents sampled to infer the schema. |
| `cdc_dest_schema` | No | source URI query | Destination schema used for multi-table CDC runs. |
| `cdc_state_id` | No | source URI query | Stable identity for this CDC connector's resume state. Set it when multiple otherwise-identical CDC assets write to the same destination so they keep independent offsets. |
| `cdc_stream_metrics_addr` | No | `--metrics-addr` | Address on which a streaming CDC asset serves replication lag and rows-synced metrics at `/debug/vars`, such as `127.0.0.1:6060`. Requires `stream: true`. See [PostgreSQL CDC](../platforms/postgres.md#cdc-change-data-capture). |
| `cdc_stream_flush_interval` | No | `--flush-interval` | Flush interval for a streaming CDC asset. Takes precedence over `flush_interval`. |
| `cdc_stream_flush_records` | No | `--flush-records` | Buffered record count that triggers a flush for a streaming CDC asset. Takes precedence over `flush_records`. |

## Destination connections and strategies

Bruin resolves the destination connection in this order:

1. `connection` on the asset, when set.
2. `parameters.destination_connection`, when set.
3. The pipeline default connection for `parameters.destination`.

When Bruin infers a destination connection from `parameters.destination`, the built-in destination values are `athena`, `bigquery`, `clickhouse`, `databricks`, `doris`, `duckdb`, `dynamodb`, `elasticsearch`, `gsheets`, `motherduck`, `mssql`, `oracle`, `postgres`, `redshift`, `snowflake`, `starrocks`, `synapse`, and `vertica`. Other ingestr destinations can still be used when you set `connection` or `destination_connection` to a compatible Bruin connection.

Bruin forwards these write strategies to ingestr:

| Bruin configuration | Ingestr strategy |
| --- | --- |
| `materialization.strategy: create+replace` | `replace` |
| `materialization.strategy: append` | `append` |
| `materialization.strategy: merge` | `merge` |
| `materialization.strategy: delete+insert` | `delete+insert` |
| `materialization.strategy: truncate+insert` | `truncate+insert` |
| `parameters.incremental_strategy` | Passed through as-is |

Use `materialization.incremental_key` or `parameters.incremental_key` only with `append`, `merge`, and `delete+insert`. `merge` requires primary keys, supplied either by ingestr source metadata/schema or by asset columns marked `primary_key: true`; CDC assets determine keys from the source. `truncate+insert` is accepted by Bruin, but ingestr will fail the run if the selected destination cannot truncate tables. Ingestr's `scd2` strategy is not a supported Bruin ingestr asset materialization strategy.

Destination support depends on ingestr's destination implementation:

| Destination / scheme | Bruin can infer default connection from `destination` | Supported strategies in Bruin ingestr assets |
| --- | --- | --- |
| Athena / `athena` | Yes | `replace`, `append` |
| BigQuery / `bigquery` | Yes | `replace`, `append`, `merge`, `delete+insert`, `truncate+insert` |
| Blob storage / `s3`, `gcs`, `adls`, `abfs`, etc. | No, set `connection` | `replace`, `append` |
| Cassandra / `cassandra` | No, set `connection` | `replace`, `append`, `merge`, `truncate+insert` |
| ClickHouse / `clickhouse` | Yes | `replace`, `append`, `merge`, `delete+insert`, `truncate+insert` |
| CrateDB / `cratedb` | No, set `connection` | `replace`, `append`, `merge`, `truncate+insert` |
| CSV, JSONL, Parquet / `csv`, `jsonl`, `parquet` | No, set `connection` | `replace`, `append` |
| Databricks / `databricks` | Yes | `replace`, `append`, `merge`, `delete+insert`, `truncate+insert` |
| DuckDB, MotherDuck / `duckdb`, `motherduck`, `md` | Yes for `duckdb` and `motherduck` | `replace`, `append`, `merge`, `delete+insert`, `truncate+insert` |
| DynamoDB / `dynamodb` | Yes | `replace`, `append`, `merge` |
| Elasticsearch / `elasticsearch` | Yes | `replace`, `append` |
| Fabric / `fabric` | No, set `connection` | `replace`, `append`, `merge`, `delete+insert`, `truncate+insert` |
| Google Sheets / `gsheets` | Yes | `replace`, `append` |
| Iceberg / `iceberg`, `iceberg+rest`, etc. | No, set `connection` | `replace`, `append`, `merge`, `delete+insert`, `truncate+insert` |
| MaxCompute / `maxcompute`, `odps` | No, set `connection` | `replace`, `append`, `truncate+insert` without primary keys |
| MongoDB / `mongodb`, `mongodb+srv` | No, set `connection` | `replace`, `append`, `merge` |
| MS SQL Server / `mssql` | Yes | `replace`, `append`, `merge`, `delete+insert`, `truncate+insert` |
| MySQL, Vitess, PlanetScale / `mysql`, `vitess`, `ps_mysql` | No, set `connection` | `replace`, `append`, `merge`, `delete+insert`, `truncate+insert` |
| OneLake / `onelake` | No, set `connection` | `replace`, `append`, `merge`, `delete+insert` |
| Oracle / `oracle` | Yes | `replace`, `append`, `merge`, `delete+insert`, `truncate+insert` |
| PostgreSQL / `postgres` | Yes | `replace`, `append`, `merge`, `delete+insert`, `truncate+insert` |
| Redshift / `redshift` | Yes | `replace`, `append`, `merge`, `delete+insert`, `truncate+insert` |
| Snowflake / `snowflake` | Yes | `replace`, `append`, `merge`, `delete+insert`, `truncate+insert` |
| SQLite / `sqlite` | No, set `connection` | `replace`, `append`, `merge`, `delete+insert`, `truncate+insert` |
| StarRocks / `starrocks` | Yes | `replace`, `append`, `merge` |
| Synapse / `synapse` | Yes | `replace`, `append`, `merge`, `delete+insert`, `truncate+insert` |
| Trino / `trino` | No, set `connection` | `replace`, `append`, `merge` |

For `truncate+insert`, ingestr truncates the target table before loading new rows. If primary keys are configured, ingestr also needs destination merge support to deduplicate rows from staging. The Bruin CLI can infer default connections for `doris` and `vertica`, but the current ingestr source does not register `doris://` or `vertica://` destinations; use native Bruin assets for those platforms unless your ingestr version adds destination support.

Each source-specific ingestion page in this documentation lists that source's supported tables with primary keys, incremental keys, and default incremental strategies. The upstream ingestr docs remain the source of truth for destination-specific strategy limits.

### Column metadata

Define columns on the asset to enrich the metadata passed to Ingestr. Columns flagged as `primary_key: true` are translated into repeated `--primary-key` flags, columns with `mask` are translated into repeated `--mask` flags, and date-typed incremental keys automatically surface through `--columns`. See [Column metadata](./columns.md) for the syntax.

### Custom SQL queries

For SQL sources, `source_table` can be a custom query by prefixing the SQL with `query:`:

```yaml
name: raw.recent_orders
type: ingestr
parameters:
  source_connection: postgres_prod
  source_table: "query:select id, customer_id, updated_at from public.orders where updated_at > :interval_start"
  destination: bigquery
  incremental_strategy: merge
  incremental_key: updated_at

columns:
  - name: id
    type: integer
    primary_key: true
  - name: updated_at
    type: timestamp
```

The incremental key must be returned by the query. For incremental runs, include your own timestamp filtering in the query and use Ingestr's `:interval_start` and `:interval_end` variables when needed.

### Run configuration

Pipeline run options propagate to ingestr automatically:

* When a run defines an interval start or end date, Bruin appends `--interval-start` and `--interval-end` with the resolved timestamps (including interval modifiers, when enabled).
* Running with `--full-refresh` adds the `--full-refresh` flag to Ingestr.
* For a streaming asset (`stream: true`), Bruin omits `--interval-end` so the live tail is not truncated, and does not pass `--full-refresh`.

## Streaming assets

Some ingestr sources can ingest **continuously** and never finish on their own:

* CDC sources (PostgreSQL, Vitess, PlanetScale) with `cdc: true` and `stream: true`.
* Message-broker sources (Kafka, Kinesis) with `stream: true`.

A continuous asset does not fit a normal batch `bruin run` (which expects every asset to complete), so Bruin keeps streaming assets out of ordinary runs and gives them a dedicated run mode.

Launch a single streaming asset with `--stream`:

```bash
bruin run --stream assets/my_stream.asset.yml
```

* The run targets exactly one streaming asset and keeps it running in the foreground until you stop it with `Ctrl+C` (`SIGINT`/`SIGTERM`). Ingestr flushes buffered records and advances its offset before exiting, so the next run resumes cleanly.
* A normal `bruin run <pipeline>` skips streaming assets and prints a notice; downstream assets read whatever the stream has landed so far.
* `--stream` runs only the main task — column/custom checks and metadata push do not run for a stream. It cannot be combined with `--downstream`, `--continue`, `--modified`, `--selector`, `--interactive`, `--sensor-mode`, or `--full-refresh`.
* Resume state (replication offset) is managed by ingestr in the destination's staging namespace. For CDC assets it is keyed by `cdc_state_id`. Bruin does not store offsets.
* For CDC assets, merge applies updates and deletes correctly only when the source table has a usable primary key / replica identity. For PostgreSQL, an unconsumed replication slot retains WAL on the source, so monitor replication lag via `cdc_stream_metrics_addr` (`/debug/vars`). See [PostgreSQL CDC](../platforms/postgres.md#cdc-change-data-capture).

## Examples

The examples below show how to use the `ingestr` asset type in your pipeline. Feel free to change them as you wish according to your needs.

### Copy a table from MySQL to BigQuery

```yaml
name: raw.transactions
type: ingestr
parameters:
  source_connection: mysql_prod
  source_table: public.transactions
  destination: bigquery
```

### Copy a table from Microsoft SQL Server to Snowflake incrementally

This example shows how to use `updated_at` column to incrementally load the data from Microsoft SQL Server to Snowflake.

```yaml
name: raw.transactions
type: ingestr
parameters:
  source_connection: mssql_prod
  source_table: dbo.transactions
  destination: snowflake
materialization:
  type: table
  strategy: append
  incremental_key: updated_at
```

### Copy data from Google Sheets to Snowflake

This example shows how to copy data from Google Sheets into your Snowflake database

```yaml
name: raw.manual_orders
type: ingestr
parameters:
  source: gsheets
  source_connection: gcp-default
  source_table: <mysheetid>.<sheetname>
  destination: snowflake
```

### Enforce schema with column types

This example shows how to use `enforce_schema` to ensure the destination table has the correct column types. This is useful when the source system's type inference doesn't match your requirements.

```yaml
name: raw.users
type: ingestr
parameters:
  source_connection: mongodb_prod
  source_table: prod.users
  destination: bigquery
  enforce_schema: true
materialization:
  type: table
  strategy: merge
  incremental_key: updated_at

columns:
  - name: _id
    type: string
    primary_key: true
  - name: name
    type: string
  - name: email
    type: string
    mask: hash
  - name: age
    type: integer
  - name: created_at
    type: timestamp
  - name: updated_at
    type: timestamp
```

When `enforce_schema: true` is set, Bruin passes the column type hints to Ingestr via the `--columns` flag, ensuring the destination table schema matches your definition.

#### Sized string types

You can give a string column an optional length to create a bounded column instead of an unbounded one. Set the length inline in the `type` or with the `length` field (requires `enforce_schema: true`):

```yaml
parameters:
  enforce_schema: "true"

columns:
  - name: name
    type: varchar(100)
  - name: email
    type: string
    length: 255
```

If you set both, the inline length wins. A string type without a length creates an unbounded column.
