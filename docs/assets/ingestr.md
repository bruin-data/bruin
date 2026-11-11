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
  batch_size: integer # MiB
  loader_file_format: jsonl | csv | parquet
  loader_file_size: integer
  extract_parallelism: integer
  destination_parallelism: integer
  extract_partition_by: string
  extract_partition_interval: string | integer
  sql_limit: integer
  sql_exclude_columns: string
  no_inference: true|false
  no_load_timestamp: true|false
  no_run_id: true|false
  postgres_snapshot: string # ordinary PostgreSQL source only
  full_refresh: true|false
  mask: string
  trim_whitespace: true|false
  pipelines_dir: string
  staging_bucket: string
  staging_dataset: string
  stream: true|false
  flush_interval: string
  flush_records: integer
  metrics_addr: string
  enforce_schema: true|false # Will ensure that the columns defined in the asset are present in the destination and with the desired types (see https://getbruin.com/docs/bruin/assets/columns.html)
  cdc: "true"|"false"
  cdc_delete_behavior: soft | hard
  cdc_snapshot_mode: auto | never
  cdc_start_lsn: string
  cdc_snapshot_spool: true|false
  cdc_snapshot_chunked: true|false
  cdc_snapshot_execution: serial | parallel
  cdc_snapshot_chunk_size: integer
  cdc_snapshot_chunk_timeout: string
  cdc_wal_staging: memory | inmemory | destination | disk
  cdc_wal_spool_dir: string
  cdc_wal_buffer_bytes: integer
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
  cdc_table_naming: schema_table | table
  cdc_discover_interval: string
  cdc_binary: true|false
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
| `destination` | Unless `connection` or `destination_connection` is set | _n/a_ | Logical destination type used for default connection inference. When `connection` and `destination_connection` are omitted, Bruin uses this value to choose the pipeline default destination connection. When omitted, Bruin infers it from the type of the destination connection, e.g. `bigquery` for a `google_cloud_platform` connection. A `destination` that is set, including one from the pipeline's `default.parameters`, is used as is. |
| `destination_connection` | No | _n/a_ | Named destination connection to use when `connection` is omitted. This overrides default connection inference from `destination`. |
| `destination_table` | No | `--dest-table` | Destination table identifier passed to Ingestr. Defaults to the asset `name`. Set this when the destination table needs characters that are not allowed in an asset name, such as the `?key=value` parameters a CleverTap destination expects. |
| `incremental_strategy` | No | `--incremental-strategy` | Passes the incremental loading strategy (`replace`, `append`, `merge`, `delete+insert`, or `truncate+insert`) to Ingestr. Prefer `materialization.strategy` for new assets. |
| `incremental_key` | No | `--incremental-key` | Column that determines incremental progress. When the column is defined with type `date`, Bruin also forwards it through the `--columns` option so Ingestr treats it as a date field. |
| `partition_by` | No | `--partition-by` | Comma-separated list of destination columns to partition by. |
| `cluster_by` | No | `--cluster-by` | Comma-separated list of destination clustering keys. |
| `schema_contract` | No | `--schema-contract` | Controls how Ingestr handles schema changes (`evolve`, `freeze`, `discard_row`, `discard_value`). |
| `loader_file_format` | No | `--loader-file-format` | Overrides the loader file format (`jsonl`, `csv`, `parquet`). |
| `loader_file_size` | No | `--loader-file-size` | Sets the maximum loader file size accepted by Ingestr. |
| `sql_backend` | No | `--sql-backend` | Selects the SQL backend Ingestr should use (`pyarrow` or `sqlalchemy`). |
| `schema_naming` | No | `--schema-naming` | Controls how Ingestr normalizes schema names. Accepted values match the [Ingestr CLI.](https://getbruin.com/docs/ingestr/commands/ingest.html#optional-flags) |
| `page_size` | No | `--page-size` | Fetch page size in rows. For chunked PostgreSQL CDC, an explicit value adds a row ceiling to byte-bounded pages. |
| `batch_size` | No | `--batch-size` | Positive maximum source record batch size in **MiB**, default `512`. Requires ingestr v1.1.30 or later. Chunked PostgreSQL CDC uses approximately 16 MiB pages, or this value if smaller. A single row may exceed the byte target. |
| `extract_parallelism` | No | `--extract-parallelism` | Limits concurrent extraction workers. An explicit value also controls destination workers unless `destination_parallelism` overrides it. It is separate from `cdc_snapshot_execution`. |
| `destination_parallelism` | No | `--destination-parallelism` | Positive destination worker count, independent of extraction workers. Omission preserves ingestr's destination-specific defaults or an explicit `extract_parallelism`. Requires ingestr v1.1.85 or later. |
| `extract_partition_by` | No | `--extract-partition-by` | Source date/time or integer column used to split bounded extraction into parallel windows. |
| `extract_partition_interval` | No | `--extract-partition-interval` | Width of each extract partition window as a duration (such as `1h` or `7d`), integer step, or `auto`. Ingestr defaults to `auto` when `extract_partition_by` is set. |
| `sql_reflection_level` | No | `--sql-reflection-level` | Tunes the amount of schema reflection performed against the source. |
| `sql_limit` | No | `--sql-limit` | Applies a `LIMIT` clause when extracting from the source. |
| `sql_exclude_columns` | No | `--sql-exclude-columns` | List of columns to skip during extraction. |
| `no_inference` | No | `--no-inference` | Uses `columns` as the source schema for schema-less sources instead of inferring types. |
| `no_load_timestamp` | No | `--no-load-timestamp` | Disables the `_ingestr_loaded_at` metadata column. Defaults to `false`; requires ingestr v1.0.39 or later. |
| `no_run_id` | No | `--no-run-id` | Disables the `_ingestr_run_id` metadata column. Defaults to `false`; requires ingestr v1.1.31 or later. |
| `postgres_snapshot` | No | source URI `snapshot` | Exported snapshot ID for an ordinary PostgreSQL source load. Requires ingestr v1.1.83 or later and cannot be combined with `cdc: true`. Keep the exporting session open until every load finishes; see [Loading an external baseline](../platforms/postgres.md#loading-an-external-baseline). |
| `full_refresh` | No | `--full-refresh` | Requests a full refresh. For streaming CDC assets, refreshes once before continuing replication. The run-level `--full-refresh` flag still applies to all assets. |
| `mask` | No | `--mask` | Adds a column masking rule such as `email:hash`. |
| `pipelines_dir` | No | `--pipelines-dir` | Directory where Ingestr stores pipeline metadata. |
| `staging_bucket` | No | `--staging-bucket` | Overrides the staging bucket that Ingestr uses for intermediate files. |
| `staging_dataset` | No | `--staging-dataset` | Dataset/schema to use for staging tables. |
| `trim_whitespace` | No | `--trim-whitespace` | Trims leading and trailing whitespace from extracted string values when set to `true`. |
| `stream` | No | `--stream` | Enables continuous ingestion. Works for CDC sources (`cdc: true`) and message-broker sources (such as Kafka). A streaming asset never exits on its own and must be launched with `bruin run --stream` (see [Streaming assets](#streaming-assets)). |
| `flush_interval` | No | `--flush-interval` | Flush interval for streaming mode, such as `30s`. CDC assets can set `cdc_stream_flush_interval` instead, which takes precedence. |
| `flush_records` | No | `--flush-records` | Number of buffered records that triggers a flush in streaming mode. CDC assets can set `cdc_stream_flush_records` instead, which takes precedence. |
| `metrics_addr` | No | `--metrics-addr` | Metrics address, such as `127.0.0.1:6060`. Current versions serve Prometheus at `/metrics`. Requires ingestr v1.0.73+ for streaming (pins before v1.1.10 use the legacy `/debug/vars` endpoint); finite-run metrics require v1.1.87+. Takes precedence over `cdc_stream_metrics_addr`. |
| `enforce_schema` | No | `--columns` | When set to `true`, enforces the column types defined in the asset's `columns` section. Ingestr will create or update the destination table with the specified schema. |
| `cdc` | No | source URI scheme | Enables Bruin's CDC URI handling for PostgreSQL, MySQL/MariaDB, Vitess, PlanetScale, MongoDB, and SQL Server (log-based CDC and Change Tracking) sources when set to `"true"`. CDC assets must use `merge`; Bruin sets it automatically when omitted and rejects other strategies. |
| `cdc_delete_behavior` | No | `--cdc-delete-behavior` | How CDC merges apply source deletes in batch and streaming assets: `soft` (default) retains rows with `_cdc_deleted = true`; `hard` removes them. Requires `cdc: true` and ingestr v1.1.77 or later. Hard deletes require a supported source and destination with destination-managed CDC state; see [PostgreSQL delete behavior](../platforms/postgres.md#delete-behavior) and the [ingestr CDC guide](https://getbruin.com/docs/ingestr/getting-started/cdc.html#deletes). |
| `cdc_snapshot_mode` | No | `--cdc-snapshot-mode` | PostgreSQL CDC snapshot policy: `auto` permits snapshots and backfills; `never` prohibits them and full refresh. Omission inherits the saved policy; new connectors default to `auto`. Requires ingestr v1.1.78 or later. See [CDC without snapshots](../platforms/postgres.md#cdc-without-snapshots). |
| `cdc_start_lsn` | No | `--cdc-start-lsn` | Nonzero PostgreSQL LSN for bootstrapping preloaded destination tables. Requires `cdc_snapshot_mode: never` and ingestr v1.1.78 or later. With v1.1.79 or later, the same boundary may remain configured after bootstrap: subsequent runs use their latest saved checkpoints. A different boundary cannot reset progress. See [CDC without snapshots](../platforms/postgres.md#cdc-without-snapshots). |
| `cdc_snapshot_spool` | No | `--cdc-snapshot-spool` | Captures changes while a normal PostgreSQL CDC snapshot copies, then replays them. Defaults to `false`; requires ingestr v1.1.86 or later. Supports finite runs, streaming, and full refresh. Cannot be combined with chunking or snapshot policy `never`. See [Spooling normal snapshots](../platforms/postgres.md#spooling-normal-snapshots). |
| `cdc_snapshot_chunked` | No | `--cdc-snapshot-chunked` | Enables resumable PostgreSQL CDC snapshots in short transactions. Defaults to `false`; requires `cdc: true` and ingestr v1.1.81 or later. Works for batch and streaming assets. See [Large initial snapshots](../platforms/postgres.md#large-initial-snapshots). |
| `cdc_snapshot_execution` | No | `--cdc-snapshot-execution` | Chunked snapshot execution: `serial` (default) copies all tables before applying retained WAL; `parallel` decodes changes while copying chunks. |
| `cdc_snapshot_chunk_size` | No | `--cdc-snapshot-chunk-size` | Maximum rows per snapshot chunk. Must be positive; defaults to `10000`. |
| `cdc_snapshot_chunk_timeout` | No | `--cdc-snapshot-chunk-timeout` | Positive duration for each chunk, including temporary replication-slot creation. Defaults to `30s`. |
| `cdc_wal_staging` | No | `--cdc-wal-staging` | Pending decoded-event storage: `inmemory`, `memory` (alias), `destination`, or `disk`. Defaults to `destination` with normal snapshot spooling and `inmemory` otherwise. Chunked destination staging requires parallel execution; disk requires normal snapshot spooling. `memory` and `disk` require ingestr v1.1.86 or later. |
| `cdc_wal_spool_dir` | No | `--cdc-wal-spool-dir` | Persistent directory required for `cdc_snapshot_spool: true` with `cdc_wal_staging: disk`. Reuse the same directory across restarts; requires ingestr v1.1.86 or later. |
| `cdc_wal_buffer_bytes` | No | `--cdc-wal-buffer-bytes` | Positive memory buffering threshold for pending decoded events. Defaults to `67108864` (64 MiB); does not bound total process memory or retained PostgreSQL WAL. |
| `cdc_mode` | No | `--stream` flag | **Deprecated** — use `stream` instead. `cdc_mode: stream` is equivalent to `stream: true`; `cdc_mode: batch` is the default (omit it). |
| `cdc_backend` | No | _ignored_ | Obsolete compatibility parameter. Accepted without effect; ingestr selects the CDC backend from the source connection's URI scheme. |
| `cdc_sql_capture` | No | source URI scheme | SQL Server capture mechanism, either `cdc` (log-based, `mssql+cdc`; default) or `change_tracking` (`mssql+ct`). |
| `cdc_publication` | No | source URI query | PostgreSQL publication name. |
| `cdc_slot` | No | source URI query | PostgreSQL replication slot name. |
| `cdc_server_id` | No | source URI query | MySQL-family binlog replication server ID. |
| `cdc_tls` | No | source URI query | MySQL-family CDC TLS setting. |
| `cdc_grpc_port` | No | source URI query | Vitess VStream gRPC port override. |
| `cdc_grpc_host` | No | source URI query | Vitess VStream gRPC host override. |
| `cdc_grpc_tls` | No | source URI query | Vitess VStream TLS setting. |
| `cdc_capture_instance` | No | source URI query | SQL Server log-based CDC capture instance name. |
| `cdc_poll_interval` | No | source URI query | SQL Server poll interval, such as `10s`. Applies to both captures; Change Tracking only reads it while streaming, where it defaults to `1s`. |
| `cdc_max_await_time` | No | source URI query | MongoDB change-stream maximum await time, such as `5s`. |
| `cdc_schema_sample_size` | No | source URI query | MongoDB number of documents sampled to infer the schema. |
| `cdc_dest_schema` | No | source URI query | Destination schema used for multi-table CDC runs. |
| `cdc_table_naming` | No | `--cdc-table-naming` | Multi-table target naming when `cdc_dest_schema` is set: `schema_table` (default, `sales.orders` becomes `target.sales_orders`) or `table` (`target.orders`). Collisions are rejected. Requires ingestr v1.1.49 or later. |
| `cdc_discover_interval` | No | source URI `discover_interval` | PostgreSQL streaming table discovery interval, default `30s`. Accepts nonnegative durations and `0`/`off` to disable. A newly discovered table requests a process restart; fixed table selections do not grow automatically. Requires ingestr v1.0.71 or later. |
| `cdc_binary` | No | source URI `binary` | Enables PostgreSQL binary logical-replication decoding; defaults to `false`. Requires ingestr v1.0.72 or later, PostgreSQL 14+, and column types supported by ingestr's binary decoder. |
| `cdc_state_id` | No | source URI query | Stable identity for this CDC connector's resume state. Set it when multiple otherwise-identical CDC assets write to the same destination so they keep independent offsets. |
| `cdc_stream_metrics_addr` | No | `--metrics-addr` | Compatibility alias for `metrics_addr` on streaming assets and, with ingestr v1.1.87+, finite CDC assets. Ignored for finite non-CDC assets. The canonical `metrics_addr` value wins when both are supplied. |
| `cdc_stream_flush_interval` | No | `--flush-interval` | Flush interval for a streaming CDC asset. Takes precedence over `flush_interval`. |
| `cdc_stream_flush_records` | No | `--flush-records` | Buffered record count that triggers a flush for a streaming CDC asset. Takes precedence over `flush_records`. |

## Destination connections and strategies

Bruin resolves the destination connection in this order:

1. `connection` on the asset, when set.
2. `parameters.destination_connection`, when set.
3. The pipeline default connection for `parameters.destination`.

When Bruin infers a destination connection from `parameters.destination`, the built-in destination values are `athena`, `bigquery`, `clickhouse`, `databricks`, `doris`, `duckdb`, `dynamodb`, `elasticsearch`, `fabric`, `gsheets`, `iceberg`, `motherduck`, `mssql`, `oracle`, `postgres`, `redshift`, `snowflake`, `starrocks`, `synapse`, and `vertica`. Other ingestr destinations can still be used when you set `connection` or `destination_connection` to a compatible Bruin connection.

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
| Fabric / `fabric` | Yes | `replace`, `append`, `merge`, `delete+insert`, `truncate+insert` |
| Google Sheets / `gsheets` | Yes | `replace`, `append` |
| Iceberg / `iceberg`, `iceberg+rest`, etc. | Yes for `iceberg` | `replace`, `append`, `merge`, `delete+insert`, `truncate+insert` |
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

For PostgreSQL or MySQL CDC into StarRocks, see [StarRocks CDC replication](/ingestion/starrocks#cdc-replication) for batch and streaming examples, destination naming, and requirements.

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

* When a run defines an interval start or end date, Bruin appends `--interval-start` and `--interval-end` with the resolved timestamps (including interval modifiers, when enabled). For `cdc_snapshot_chunked: true` or `cdc_snapshot_spool: true`, Bruin omits both bounds because the snapshot copies complete selected tables.
* Running with `--full-refresh`, or setting `parameters.full_refresh: true` on the asset, adds the `--full-refresh` flag to Ingestr.
* For a streaming asset (`stream: true`), Bruin omits `--interval-end` so the live tail is not truncated. A full refresh rebuilds the selected destination tables before continuing with live changes.
* `--refresh-id` supplies a stable identifier for the current run's streaming full-refresh request. It requires both streaming and full refresh to be enabled; `refresh_restricted: true` prevents that refresh. The identifier is supplied only through the command line.

## Streaming assets

Some ingestr sources can ingest **continuously** and never finish on their own:

* CDC sources (such as PostgreSQL, MySQL, Vitess, and PlanetScale) with `cdc: true` and `stream: true`, using a destination that supports the source's CDC requirements.
* Message-broker sources (Kafka, Kinesis) with `stream: true`.

A continuous asset does not fit a normal batch `bruin run` (which expects every asset to complete), so Bruin keeps streaming assets out of ordinary runs and gives them a dedicated run mode.

Launch a single streaming asset with `--stream`:

```bash
bruin run --stream assets/my_stream.asset.yml
```

* The run targets exactly one streaming asset and keeps it running in the foreground until you stop it with `Ctrl+C` (`SIGINT`/`SIGTERM`). Ingestr flushes buffered records and advances its offset before exiting, so the next run resumes cleanly.
* A normal `bruin run <pipeline>` skips streaming assets and prints a notice; downstream assets read whatever the stream has landed so far.
* `--stream` runs only the main task — column/custom checks and metadata push do not run for a stream. It cannot be combined with `--downstream`, `--continue`, `--modified`, `--selector`, `--interactive`, or `--sensor-mode`.
* Resume state (replication offset) is managed by ingestr in the destination's staging namespace. For CDC assets it is keyed by `cdc_state_id`. Bruin does not store offsets.
* For CDC assets, merge applies updates and deletes correctly only when the source table has a usable primary key / replica identity. For PostgreSQL, monitor replication progress and retained WAL through `metrics_addr` (`/metrics`). See [PostgreSQL CDC](../platforms/postgres.md#cdc-change-data-capture).
* PostgreSQL wildcard discovery can stop the stream with a restart request. Bruin returns the ingestr result; rerun the asset to backfill newly discovered tables. Bruin does not automatically restart the process. Chunked snapshots require full refresh when the selected table set changes.

### Refreshing a stream

For CDC connectors and destinations that support streaming full refresh, start a new process with:

```bash
bruin run --stream --full-refresh --refresh-id refresh-2026-10-18 assets/my_stream.asset.yml
```

Use the same identifier when restarting or retrying that request. Ingestr records its progress durably, so a completed request resumes replication without refreshing again. Use a new identifier to request another refresh. Without an identifier, `--full-refresh` requests a new refresh on every invocation.

An asset can enable full refresh with `parameters.full_refresh: true`:

```yaml
parameters:
  source_connection: source_db
  source_table: public.orders
  destination: postgres
  cdc: true
  stream: true
  full_refresh: true
```

Supply the refresh identifier when starting each run:

```bash
bruin run --stream --refresh-id refresh-2026-10-18 assets/my_stream.asset.yml
```

The selected ingestr version must support `--full-refresh --stream` and `--refresh-id`. Ingestr validates connector and destination support. Refresh replaces the tables selected by the asset; it does not remove destination tables from earlier source selections or destination configurations.

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

#### Sized decimal types

You can give a `decimal`/`numeric` column a precision and an optional scale. Set them inline in the `type` or with the `precision` and `scale` fields (requires `enforce_schema: true`):

```yaml
parameters:
  enforce_schema: "true"

columns:
  - name: amount
    type: decimal(10,2)
  - name: rate
    type: decimal
    precision: 18
    scale: 4
```

If you set both, the inline parameters win. `decimal(10)` (or `precision` without `scale`) sets only the precision; a `decimal` without either creates a decimal with the destination's default precision and scale.
