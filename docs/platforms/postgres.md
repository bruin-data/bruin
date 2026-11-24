# PostgreSQL

Bruin supports PostgreSQL as a data platform.

## Connection

In order to set up a PostgreSQL connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.

```yaml
    connections:
      postgres:
        - name: "connection_name"
          username: "pguser"
          password: "XXXXXXXXXX"
          host: "pghost.somedomain.com"
          port: 5432
          database: "dev"
          ssl_mode: "allow" # optional
          schema: "schema_name" # optional
          pool_max_conns: 5 # optional
          read_only: false
```

### Read-only connections

Set `read_only: true` to run SQL queries in read-only transactions (default: `false`). Functions that run SQL through another session, such as `dblink` and `pg_background`, are not covered by the transaction, so Bruin also validates each query before execution and rejects writes and calls to those functions. Other functions, including UDFs and extension functions, are allowed and run under the read-only transaction. Bruin does not inspect view or function definitions, so use a restricted database role for full protection. Ingestr assets and seeds do not support this option.

> [!NOTE]
> `ssl_mode` should be one of the modes describe in the [documentation](https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION).

## PostgreSQL Assets

### `pg.sql`

Runs a materialized Postgres asset or an sql script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.

#### Example: Create a table using table materialization

```bruin-sql
/* @bruin
name: events.install
type: pg.sql
materialization:
    type: table
@bruin */

select user_id, ts, platform, country
from analytics.events
where event_name = "install"
```

#### Example: Run a Postgres script

```bruin-sql
/* @bruin
name: events.install
type: pg.sql
@bruin */

create temp table first_installs as
select distinct on (user_id)
    user_id, 
    ts as install_ts,
    platform,
    country
from analytics.events
where event_name = 'install'
order by user_id, ts;

create table if not exists events.install as
select
    user_id, 
    i.install_ts,
    i.platform, 
    i.country,
    a.channel
from first_installs as i
join marketing.attribution as a
    using(user_id)
```

### `pg.sensor.table`

Sensors are a special type of assets that are used to wait on certain external signals.

Checks if a table exists in Postgres, runs by default every 30 seconds until this table is available.

```yaml
name: string
type: string
parameters:
    table: string
    poke_interval: int (optional)
    timeout: duration (optional)
```

**Parameters**:

- `table`: `schema_id.table_id` or (for default schema `public`) `table_id` format.
- `poke_interval`: The interval between retries in seconds (default 30 seconds).
- `timeout`: How long to wait before the sensor fails. Uses single-unit duration syntax (`s`, `m`, `h`, `d`, `ms`, `ns`), e.g. `1h` or `90m`. Defaults to `24h`. See [Sensor Timeout](/assets/sensor#timeout).

### `pg.sensor.query`

Checks if a query returns any results in Postgres, runs by default every 30 seconds until this query returns any results.

```yaml
name: string
type: string
parameters:
    query: string
    poke_interval: int (optional)
    timeout: duration (optional)
```

**Parameters**:

- `query`: Query you expect to return any results
- `poke_interval`: The interval between retries in seconds (default 30 seconds).
- `timeout`: How long to wait before the sensor fails. Uses single-unit duration syntax (`s`, `m`, `h`, `d`, `ms`, `ns`), e.g. `1h` or `90m`. Defaults to `24h`. See [Sensor Timeout](/assets/sensor#timeout).

#### Example: Partitioned upstream table

Checks if the data available in upstream table for end date of the run.

```yaml
name: analytics_123456789.events
type: pg.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}")
```

#### Example: Streaming upstream table

Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.

```yaml
name: analytics_123456789.events
type: pg.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}")
```

### `pg.seed`

`pg.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your PostgreSQL database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the PostgreSQL database.

You can define seed assets in a file ending with `.asset.yml` or `.asset.yaml`:

```yaml
name: dashboard.hello
type: pg.seed

parameters:
    path: seed.csv
```

**Parameters**:

- `path`: The path to the CSV file that will be loaded into the data platform. This can be a relative file path (relative to the asset definition file) or an HTTP/HTTPS URL to a publicly accessible CSV file.

> [!WARNING]
> When using a URL path, column validation is skipped during `bruin validate`. Column mismatches will be caught at runtime.

#### Examples: Load csv into a Postgres database

The examples below show how to load a CSV into a PostgreSQL database.

```yaml
name: dashboard.hello
type: pg.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```

## CDC (Change Data Capture)

Bruin supports PostgreSQL CDC via the `ingestr` asset type. CDC uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html) to capture row-level changes (inserts, updates, deletes) and replicate them to a destination.

### Prerequisites

Your source PostgreSQL database must be configured for logical replication:

- `wal_level` set to `logical`
- A source user with `REPLICATION`, schema access, and `SELECT` on the complete source tables
- Enough replication slots and WAL sender connections for the consumer
- Tables with usable primary keys or replica identity, and a publication covering inserts, updates, deletes, and truncates

When publication and slot names are omitted, ingestr creates/manages them when the source user has permission. Its managed publication covers eligible logged tables; selecting one table does not narrow that shared publication. Set `cdc_publication` to use a publication you manage. Ingestr never adds tables to a user-managed publication. Snapshot policy `never` requires an existing slot.

PostgreSQL 16+ standbys are supported. Create the publication on the primary; enable `wal_level=logical` on both servers and `hot_standby_feedback=on` on the standby, with a physical replication slot for its upstream connection. Temporary chunk slots can wait for a running-transactions record from the primary; see [ingestr's standby guide](https://getbruin.com/docs/ingestr/supported-sources/postgres.html#reading-from-a-standby).

Refer to the [PostgreSQL logical replication documentation](https://www.postgresql.org/docs/current/logical-replication.html) for detailed setup instructions.

### Parameters

CDC is enabled by setting `cdc: "true"` on an `ingestr` asset with a PostgreSQL source connection.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `cdc` | Yes | Set to `"true"` to enable CDC mode |
| `cdc_delete_behavior` | No | `soft` (default) retains deleted rows with `_cdc_deleted = true`; `hard` removes them from the destination. Applies to batch and streaming CDC. Requires ingestr v1.1.77 or later; see [Delete behavior](#delete-behavior) |
| `cdc_snapshot_mode` | No | `auto` allows snapshots and backfills; `never` prohibits them. Omission inherits the saved policy; new connectors default to `auto`. Requires ingestr v1.1.78 or later; see [CDC without snapshots](#cdc-without-snapshots) |
| `cdc_start_lsn` | No | Nonzero PostgreSQL LSN for bootstrapping preloaded destination tables with `cdc_snapshot_mode: never`. Requires ingestr v1.1.78 or later; v1.1.79+ allows retaining the same boundary across subsequent runs |
| `cdc_snapshot_spool` | No | Captures changes while a normal initial/full-refresh snapshot copies, then applies them. Defaults to `false`; requires ingestr v1.1.86 or later. See [Spooling normal snapshots](#spooling-normal-snapshots) |
| `cdc_snapshot_chunked` | No | Enables resumable snapshots in short transactions; defaults to `false`. Requires ingestr v1.1.81 or later. See [Large initial snapshots](#large-initial-snapshots) |
| `cdc_snapshot_execution` | No | `serial` (default) copies all selected tables before replaying retained WAL; `parallel` decodes changes while copying chunks |
| `cdc_snapshot_chunk_size` | No | Positive maximum row count per chunk; defaults to `10000` |
| `cdc_snapshot_chunk_timeout` | No | Positive duration per chunk, including temporary replication-slot creation; defaults to `30s` |
| `cdc_wal_staging` | No | `inmemory`, `memory` (alias), `destination`, or `disk`. Defaults to `destination` with normal spooling and `inmemory` otherwise. Chunked destination staging requires parallel execution; disk requires normal spooling. `memory` and `disk` require ingestr v1.1.86+ |
| `cdc_wal_spool_dir` | No | Persistent directory required with normal spooling and `cdc_wal_staging: disk`; reuse it across restarts. Requires ingestr v1.1.86+ |
| `cdc_wal_buffer_bytes` | No | Positive decoded-event buffering threshold in bytes; defaults to `67108864` (64 MiB). Does not bound total process memory or retained source WAL |
| `stream` | No | Set to `true` for continuous (real-time) streaming. Omit for batch replication (read up to the current WAL position and exit) |
| `cdc_mode` | No | **Deprecated** — use `stream` instead. `cdc_mode: stream` is equivalent to `stream: true` |
| `cdc_publication` | No | Name of the PostgreSQL publication to use |
| `cdc_slot` | No | Name of the PostgreSQL replication slot to use |
| `cdc_dest_schema` | No | Schema to use when running multi-table CDC |
| `cdc_table_naming` | No | Multi-table naming within `cdc_dest_schema`: `schema_table` (default) includes the source schema in the table name; `table` keeps only the table name. Requires ingestr v1.1.49+ |
| `cdc_state_id` | No | Stable connector identity for destination-managed resume state. Keep it unchanged across restarts |
| `cdc_discover_interval` | No | Streaming discovery interval, default `30s`. Nonnegative duration, or `0`/`off` to disable. Requires ingestr v1.0.71+; see [Table discovery and naming](#table-discovery-and-naming) |
| `cdc_binary` | No | Opt into binary pgoutput decoding, default `false`. Requires PostgreSQL 14+, supported column types, and ingestr v1.0.72+ |
| `metrics_addr` | No | Address serving Prometheus metrics at `/metrics`, such as `127.0.0.1:6060`. Finite runs require ingestr v1.1.87+ |
| `cdc_stream_metrics_addr` | No | Compatibility alias for `metrics_addr`; the canonical value takes precedence. Also works for finite runs with ingestr v1.1.87+ |
| `cdc_stream_flush_interval` | No | How often buffered records are written to the destination, such as `30s`. Takes precedence over `flush_interval` |
| `cdc_stream_flush_records` | No | Number of buffered records that triggers a write to the destination. Takes precedence over `flush_records` |
| `source_table` | Yes | One table, a comma-separated list, or `"*"` for all eligible publication tables |
| `extract_parallelism` | No | Source reader workers. An explicit value also controls destination workers unless overridden; separate from chunk execution order |
| `destination_parallelism` | No | Positive write-worker count, independent of extraction workers. Omission preserves ingestr defaults; requires v1.1.85+ |
| `page_size` | No | Source page row ceiling; chunked snapshots apply it alongside their byte target when explicitly set |
| `batch_size` | No | Positive source batch size in **MiB**, default `512`; requires ingestr v1.1.30+. Chunked pages use approximately 16 MiB or this value if smaller |
| `no_load_timestamp` / `no_run_id` | No | Disable `_ingestr_loaded_at` / `_ingestr_run_id` metadata. Both default to `false`; require ingestr v1.0.39+ / v1.1.31+ respectively |
| `incremental_strategy` | No | Defaults to `"merge"` when CDC is enabled. CDC assets must use `"merge"`; Bruin rejects other strategies. |

> [!NOTE]
> When CDC is enabled, primary key columns do not need to be specified in the asset definition — they are determined automatically from the source table.

### Delete behavior

Set `cdc_delete_behavior` to control how source deletes affect the destination in both batch and streaming CDC assets:

- `soft` (default): Keep the row and set `_cdc_deleted = true`. Filter on `NOT _cdc_deleted` to read only live rows.
- `hard`: Remove the row from the destination. PostgreSQL CDC supports this with PostgreSQL, MySQL/MariaDB, SQL Server, Snowflake, BigQuery, DuckDB, and StarRocks destinations. Ingestr keeps the replication position in destination-managed CDC state so deleting rows does not lose the resume position.

The setting requires ingestr v1.1.77 or later and applies to tables with a usable merge key. PostgreSQL keyless tables with `REPLICA IDENTITY FULL` remain append-only change logs in either mode, including their delete events. See the [ingestr CDC guide](https://getbruin.com/docs/ingestr/getting-started/cdc.html#deletes) for source and destination restrictions.

Switching an existing asset from `soft` to `hard` leaves previously written tombstones in place until their keys change again. To remove them, rebuild the destination with `bruin run --full-refresh`; for a streaming asset, use [streaming full refresh](../assets/ingestr.md#refreshing-a-stream).

Full refresh is unavailable for connectors using `cdc_snapshot_mode: never`; remove existing tombstones explicitly if you change their delete behavior.

### Spooling normal snapshots

With ingestr v1.1.86+, `cdc_snapshot_spool: true` captures changes while the normal initial or full-refresh snapshot copies. Once copying finishes, ingestr replays those changes and either exits or continues streaming. The normal snapshot transaction remains open throughout copying. Use [chunked snapshots](#large-initial-snapshots) when you need shorter source transactions.

```yaml
name: public.orders
type: ingestr
connection: warehouse
parameters:
  source_connection: my_pg
  source_table: public.orders
  cdc: true
  cdc_snapshot_spool: true
  metrics_addr: 127.0.0.1:6060
```

Run this with `bruin run assets/public.orders.asset.yml`. Add `stream: true` and launch with `bruin run --stream` to keep processing changes after the snapshot. Both single-table and multi-table selections are supported.

| `cdc_wal_staging` | Changes captured during copying | Recovery storage |
| --- | --- | --- |
| `destination` (default with spooling) | Acknowledged once durably staged | Managed destination `cdc_snapshot_wal` table |
| `disk` | Acknowledged once changes and metadata are synced | Persistent local spool plus destination checkpoints |
| `memory` or `inmemory` | Source WAL is retained until changes are applied | Source WAL rebuilds volatile buffers after restart |

For local disk storage, add:

```yaml
cdc_wal_staging: disk
cdc_wal_spool_dir: /persistent/ingestr-spool
cdc_wal_buffer_bytes: 67108864
```

Size durable storage for all changes captured during the copy; it does not compact changes to each key's latest value. `cdc_wal_buffer_bytes` is a buffering threshold, not a limit on spool size, process memory, or source WAL. Durable staging allows acknowledgment during copying, but PostgreSQL's `restart_lsn` can still lag behind acknowledged progress.

Keep the same staging namespace, storage mode, persistent disk directory, table selection, and source/destination identities across retries. Do not remove an incomplete spool or disable spooling mid-bootstrap. Ingestr rejects incompatible recovery state; use full refresh to establish a new baseline. Finite snapshot cancellation can terminate the process abruptly, so recovery must retain this durable state. Streaming runs receive a graceful stop request before forced termination if needed.

Spooling requires non-null primary keys and a destination supporting CDC keyed merges, truncation, and managed state. It cannot be combined with chunking, snapshot policy `never`, `cdc_start_lsn`, SQL limits/excluded columns, incremental filters, interval bounds, or extraction partitioning. Bruin omits automatically supplied run date bounds for spooling. Flush options remain streaming-only even for a finite spooled snapshot. Later schema-change and newly discovered-table backfills use the ordinary snapshot behavior.

### Large initial snapshots

With ingestr v1.1.81 or later, set `cdc_snapshot_chunked: true` to copy large PostgreSQL tables in key-range chunks, each in a separate transaction. The source transaction closes before waiting for destination writes. Completed chunks have durable checkpoints, so an interrupted first bootstrap can resume without copying them again. Chunking is off by default; omitting the parameter keeps the existing snapshot behavior.

Choose how changes are processed during the initial copy:

| `cdc_snapshot_execution` | Initial copy and catch-up | Pending changes |
| --- | --- | --- |
| `serial` (default) | Copy every selected table in chunks, then decode and apply retained WAL | PostgreSQL retains WAL from the initial boundary throughout copying; no concurrent decoder runs during that phase |
| `parallel` | Decode changes while copying chunks and catch up after each chunk | Decoded events are staged in memory or in the destination |

This asset uses serial execution and the default chunk settings:

```yaml
name: public.orders
type: ingestr
connection: bigquery

parameters:
  source_connection: my_pg
  source_table: public.orders
  destination: bigquery
  cdc: true
  cdc_snapshot_chunked: true
  cdc_snapshot_execution: serial
  cdc_snapshot_chunk_size: 10000
  cdc_snapshot_chunk_timeout: 30s
  cdc_wal_staging: inmemory
  cdc_wal_buffer_bytes: 67108864
```

Run it with `bruin run assets/public.orders.asset.yml` to snapshot, catch up, and exit. Add `stream: true` and run `bruin run --stream assets/public.orders.asset.yml` to continue replicating changes after catch-up. Single-table and multi-table selections are supported. Bruin uses `merge` for CDC assets and omits run interval bounds for chunked snapshots because they read complete selected tables.

For parallel execution, keep `cdc_wal_staging: inmemory` to buffer decoded events in the process, or set these parameters on the asset to persist pending events in the destination:

```yaml
cdc_snapshot_execution: parallel
cdc_wal_staging: destination
```

Serial execution requires `cdc_wal_staging: inmemory`. It does not collect decoded events while copying; the source retains WAL instead. Combining serial execution with destination staging is rejected.

With ingestr v1.1.86+, `memory` is an alias for `inmemory`. Disk staging is available only for normal snapshot spooling and cannot be used with chunking.

All execution and staging choices save chunk cursors and recovery metadata in the destination's managed `cdc_snapshot_wal` table. Parallel execution with destination staging also stores pending logical change events there, not PostgreSQL WAL segment files. Preserve this table and the connector's other managed CDC state across restarts.

Short transactions limit the snapshot horizon held on the source, but do not limit WAL retention. Serial execution needs enough source disk space for WAL retained throughout copying, then advances its checkpoint through catch-up. Parallel execution with memory staging also retains WAL needed for recovery; destination staging trades extra destination writes and storage for durable pending events. The memory budget is a buffering threshold, not a limit on total process memory or source WAL size. A table is certified complete only after its required catch-up finishes.

Snapshot pages are bounded by approximately 16 MiB, or a smaller `batch_size` value in MiB. An explicit `page_size` also limits rows per page. Pages use temporary Arrow files; allow disk space for up to two chunks and set `TMPDIR` to disk-backed storage when necessary. A single large row can exceed the byte target. Parallel execution may prefetch one chunk while destination checkpointing completes. These reader controls are separate from `cdc_snapshot_execution`, which chooses when WAL is decoded.

Requirements and recovery:

- Use PostgreSQL 14 or newer, or 16 or newer when reading from a standby. The source needs a spare replication slot and replication connection for temporary chunk snapshots.
- Every selected table needs non-null keys backed by a compatible unique B-tree index. Composite keys are supported. For tables containing text, binary, JSON, array, decimal, or unknown column types, set `REPLICA IDENTITY FULL` before bootstrapping, for example `ALTER TABLE public.orders REPLICA IDENTITY FULL`.
- The destination must support CDC merges, truncation, and managed CDC state. Chunked snapshots use checkpointed merges for batch and full-refresh runs.
- Chunk size, timeout, and memory budget must be positive. An expired chunk fails safely and can be retried on the next run.
- Do not combine chunking with `cdc_snapshot_spool: true`, `cdc_snapshot_mode: never`, `cdc_start_lsn`, `sql_limit`, `sql_exclude_columns`, an incremental key or predicate, asset-level interval bounds, or `extract_partition_by`.
- Keep the slot, table selection, staging namespace, execution mode, and WAL staging mode unchanged when resuming. Changing them requires a new baseline with `bruin run --full-refresh`; streaming assets use [streaming full refresh](../assets/ingestr.md#refreshing-a-stream).
- Schema changes, replaced destinations, and source `TRUNCATE` during an unfinished bootstrap stop the run. Establish a new baseline with full refresh. After bootstrap, ordinary CDC truncate handling applies.

See the [ingestr large-snapshot guide](https://getbruin.com/docs/ingestr/supported-sources/postgres.html#large-initial-snapshots) for detailed index and recovery requirements.

### CDC without snapshots

With ingestr v1.1.78 or later, PostgreSQL CDC can start from destination tables you loaded externally. Set `cdc_snapshot_mode: never` and supply their snapshot boundary in `cdc_start_lsn`. This works for batch and streaming assets.

The destination data must represent a transaction-consistent snapshot at the supplied LSN, with all subsequent changes retained in the replication slot. Ingestr verifies the slot and table schemas, but cannot verify that the preloaded rows match that boundary. Use the verified boundary from your external loading process, not the current WAL position after loading or an Aurora snapshot timestamp.

Requirements:

- Set `source_table` to one table or a comma-separated list, rather than `"*"`. All selected tables must share the same external snapshot boundary.
- Set `cdc_slot` to an existing, inactive, persistent `pgoutput` slot in the source database. Ingestr does not create or replace the slot in this mode and rejects unavailable WAL, a future LSN, or a boundary older than the slot's confirmed or restart position.
- Preload every destination table with compatible columns and merge keys. The source must have usable merge keys, and the destination must support managed CDC state and CDC merges.

For the initial bootstrap, configure the asset as follows. Replace the example LSN with your verified boundary:

```yaml
name: public.orders
type: ingestr
connection: bigquery

parameters:
  source_connection: my_pg
  source_table: public.orders
  destination: bigquery
  cdc: true
  cdc_publication: my_publication
  cdc_slot: my_slot
  cdc_snapshot_mode: never
  cdc_start_lsn: "0/16B6C50"
  stream: true
```

Launch it with `bruin run --stream assets/public.orders.asset.yml`. For a batch run, omit `stream: true` and run the asset without `--stream`.

With ingestr v1.1.79 or later, keep the same `cdc_start_lsn` on subsequent runs or omit it: ingestr resumes from its latest managed checkpoint, not the original boundary. A different boundary cannot reset progress. With v1.1.78, remove the LSN after a successful bootstrap. The snapshot policy is saved with the connector, so subsequent runs can omit both snapshot options. For example:

```yaml
parameters:
  source_connection: my_pg
  source_table: public.orders
  destination: bigquery
  cdc: true
  cdc_publication: my_publication
  cdc_slot: my_slot
  stream: true
```

An interrupted bootstrap can retry the same `cdc_start_lsn` with `cdc_snapshot_mode: never`, or resume with both options omitted. Keep the connector identity, selected tables, destination, and staging namespace unchanged across retries and subsequent runs.

Continue using ingestr v1.1.78 or later even when omitting the snapshot options: older versions do not enforce the saved `never` policy.

For a new connector, omitting `cdc_snapshot_mode` defaults to `auto`, which permits snapshots and backfills. For a connector with a saved `never` policy, omission preserves `never`; explicitly switching to `auto` is rejected. Both `bruin run --full-refresh` and `parameters.full_refresh: true` are rejected for that connector. Missing checkpoints, replaced destinations, or source schema and identity changes stop the run instead of triggering a recovery snapshot or backfill.

Ingestr adds CDC metadata columns without rewriting the preloaded rows, so untouched rows initially have NULL metadata. To include those live rows, filter with `COALESCE(_cdc_deleted, false) = false`. Source WAL `TRUNCATE` events still clear the destination table even under `never`.

See the [ingestr snapshot-free CDC guide](https://getbruin.com/docs/ingestr/supported-sources/postgres.html#cdc-without-snapshots) for the full setup and recovery requirements.

### Loading an external baseline

Use `postgres_snapshot` on an ordinary PostgreSQL ingestr asset to load a snapshot before adopting it into CDC. This requires ingestr v1.1.83+. Configure `my_pg` and `warehouse` as source and destination PostgreSQL connections, and use a new destination table and dedicated replication slot for the handoff. Keep source schema, replica identity, and publication membership fixed; postpone `TRUNCATE` and table-rewriting DDL until adoption finishes.

1. In a normal source SQL session, create a publication containing the complete table:

   ```sql
   CREATE PUBLICATION orders_pub FOR TABLE public.orders;
   ```

2. In a separate terminal, open an interactive replication connection to the same source primary:

   ```bash
   psql -X 'postgresql://replicator@source-host/source_db?replication=database'
   ```

   At its prompt, outside a transaction, run:

   ```sql
   CREATE_REPLICATION_SLOT orders_slot LOGICAL pgoutput EXPORT_SNAPSHOT;
   ```

   Record the returned `snapshot_name` and `consistent_point`. Leave this session **connected and idle** until every load finishes. Running another command or disconnecting expires the exported snapshot; avoid `psql -c` here.

3. Replace the example snapshot ID below with the returned `snapshot_name`, then run this asset with `bruin run assets/public.orders.asset.yml`:

   ```yaml
   name: public.orders
   type: ingestr
   connection: warehouse
   parameters:
     source_connection: my_pg
     source_table: public.orders
     incremental_strategy: replace
     postgres_snapshot: "00000003-0000001B-1"
   ```

   Copy every row and data column without limits, incremental filters, masking, or transformations. The reader imports the snapshot into its source transactions and fails if it has expired. For multiple tables, use the same snapshot ID for every load and keep the exporting session open until all succeed.

4. After successful loading, close the export session. Keep the slot, and verify from a normal SQL connection that it is an inactive, persistent `pgoutput` slot in the source database with all WAL from `consistent_point` still available. Do not consume or advance that slot during loading.

5. Replace the asset's parameters with the CDC configuration below. Use the returned `consistent_point` as `cdc_start_lsn`, replacing the example value:

   ```yaml
   parameters:
     source_connection: my_pg
     source_table: public.orders
     cdc: true
     cdc_publication: orders_pub
     cdc_slot: orders_slot
     cdc_snapshot_mode: never
     cdc_start_lsn: "0/16B6C50"
     stream: true
   ```

   Keep the same destination and launch `bruin run --stream assets/public.orders.asset.yml`. Rerunning this asset uses the latest saved checkpoint. For multiple tables loaded into their original table names, use a comma-separated selection, `cdc_dest_schema: public`, and `cdc_table_naming: table` so the CDC targets match the baseline.

The returned snapshot/LSN pair establishes one consistent boundary; a later `pg_current_wal_lsn()` value does not. Keep sufficient source space for WAL throughout copying and catch-up. Ingestr verifies schemas and slot availability but cannot prove that baseline rows came from the supplied snapshot. An expired snapshot or lost WAL requires a new coordinated baseline, not substituting a current snapshot ID or LSN. See the [upstream baseline walkthrough](https://getbruin.com/docs/ingestr/tutorials/cdc-postgres-snapshot.html) for source privileges, slot inspection queries, and recovery details.

### Table discovery and naming

For multiple selected tables, `cdc_dest_schema` chooses the target schema/dataset and `cdc_table_naming` chooses names inside it:

| Source | `cdc_dest_schema: replica`, naming `schema_table` | Naming `table` |
| --- | --- | --- |
| `sales.orders` | `replica.sales_orders` | `replica.orders` |
| `public.users` | `replica.public_users` | `replica.users` |

`schema_table` is the default. Ingestr rejects collisions before copying. A single-table asset uses its asset name or `destination_table`; `cdc_dest_schema` is ignored there, and `cdc_table_naming: table` is rejected. Changing naming, selected tables, or target namespaces can change connector identity, its slot, and its initial snapshot. Keep these settings stable when resuming.

With `source_table: "*"`, batch runs discover new eligible tables on their next run. A running stream checks every `cdc_discover_interval` (default `30s`); use a nonnegative duration or `0`/`off` to disable mid-stream discovery. When new tables appear, ingestr exits with a restart request. Rerun the Bruin asset to backfill them and resume replication; Bruin does not supervise restarts. An explicit table list never grows automatically.

For a custom publication, add new tables to it yourself unless it publishes all tables. Chunked checkpoints fix the selected table set: adding tables requires full refresh. Snapshot policy `never` prohibits automatic backfills. Retain the same connector state and source WAL across restarts.

### Replica identity and schema changes

Ingestr derives merge keys from the source's replica identity. `REPLICA IDENTITY USING INDEX` uses that index's key columns, excluding `INCLUDE` columns. Tables without usable replica identity are skipped; keyless tables with `REPLICA IDENTITY FULL` produce an append-only change log. Set `REPLICA IDENTITY FULL` before changing merge-key values when PostgreSQL could omit unchanged TOAST values from the new row image. Incomplete key changes fail rather than silently replacing missing values.

Publication coverage must include inserts, updates, deletes, and truncates; this is checked even when discovery is disabled. Restore missing publication coverage before establishing a new baseline with full refresh. Stored generated columns require PostgreSQL 18+ and `publish_generated_columns = stored` on the publication. Ingestr configures managed publications; configure custom ones yourself. Earlier server versions and virtual generated columns are rejected. With snapshot policy `never`, schema/identity changes require recovery of the existing valid state or a new external baseline and connector.

### Examples

#### Basic CDC replication
```yaml
name: public.users
type: ingestr
connection: bigquery

parameters:
  source_connection: my_pg
  source_table: public.users
  destination: bigquery
  cdc: "true"
```

#### CDC with hard deletes

This batch asset removes rows deleted at the source:

```yaml
name: public.users
type: ingestr
connection: bigquery

parameters:
  source_connection: my_pg
  source_table: public.users
  destination: bigquery
  cdc: "true"
  cdc_delete_behavior: hard
```

Add `stream: true` to use the same delete behavior continuously, then launch the asset with `bruin run --stream assets/public.users.asset.yml`.

#### CDC with explicit publication and slot
```yaml
name: public.users
type: ingestr
connection: bigquery

parameters:
  source_connection: my_pg
  source_table: public.users
  destination: bigquery
  cdc: "true"
  cdc_publication: my_publication
  cdc_slot: my_slot
```

#### CDC with stream mode
```yaml
name: public.orders
type: ingestr
connection: bigquery

parameters:
  source_connection: my_pg
  source_table: public.orders
  destination: bigquery
  cdc: "true"
  stream: true
```

A streaming CDC asset (`stream: true`) runs continuously, so it is excluded from a normal `bruin run` and is launched on its own:

```bash
bruin run --stream assets/public.orders.asset.yml
```

The stream runs in the foreground until you stop it with `Ctrl+C`, then flushes and exits cleanly. See [Streaming assets](../assets/ingestr.md#streaming-assets) for the full behaviour and restrictions.

#### Tuning and observing a stream

`cdc_stream_flush_interval` and `cdc_stream_flush_records` control when buffered changes are written; defaults are `30s` and `50000`. These aliases override generic `flush_interval` and `flush_records`, and apply only to streaming runs. `extract_parallelism` controls snapshot readers; `destination_parallelism` independently controls writes where the destination supports concurrency. Normal parallel snapshot readers and eligible direct StarRocks snapshot loading are included in ingestr v1.1.85+ without an additional CDC switch.

Set `metrics_addr` to expose Prometheus metrics at `/metrics` for the process lifetime. With ingestr v1.1.87+, this also works while a finite batch snapshot is copying. The compatibility alias `cdc_stream_metrics_addr` works in both modes; `metrics_addr` takes precedence when both are supplied.

Streaming metrics require ingestr v1.0.73+. Exact pins before v1.1.10 expose the legacy `/debug/vars` endpoint instead of Prometheus; current bundled versions use `/metrics`.

```yaml
name: public.orders
type: ingestr
connection: bigquery

parameters:
  source_connection: my_pg
  source_table: public.orders
  destination: bigquery
  cdc: "true"
  stream: true
  cdc_stream_flush_interval: 30s
  cdc_stream_flush_records: 10000
  metrics_addr: 127.0.0.1:6060
```

```bash
curl -s http://localhost:6060/metrics
```

PostgreSQL slot metrics include `ingestr_replication_slot_retained_wal_bytes`, `ingestr_replication_slot_safe_wal_bytes`, `ingestr_replication_slot_max_wal_keep_bytes`, `ingestr_replication_slot_wal_status`, and `ingestr_replication_slot_collection_success`. Retained WAL measures the span from `restart_lsn` to the server WAL head. Acknowledged progress (`confirmed_flush_lsn`) can advance while the slot still retains older WAL. The safe allowance can be absent when undefined, and a retention limit of `-1` means unlimited. Check collection success before interpreting missing values. Metrics observe retention; they do not cap disk use or stop snapshots automatically.

`cdc_binary: true` enables PostgreSQL's binary pgoutput format on PostgreSQL 14+. It requires column types supported by ingestr's binary decoder; keep the default `false` for tables using unsupported or extension-specific types.

#### Wildcard CDC — replicate all tables
When `source_table` is set to `"*"`, all tables in the publication are replicated to the destination.

```yaml
name: public.all_tables
type: ingestr
connection: bigquery

parameters:
  source_connection: my_pg
  source_table: "*"
  destination: bigquery
  cdc: "true"
```

### `pg.source`

Defines PostgreSQL source assets for documenting existing tables and views in your PostgreSQL database. These assets are no-op (they don't execute), but are useful for:

- Documenting existing PostgreSQL tables and views
- Adding column descriptions and metadata
- Establishing lineage relationships
- Query preview functionality in the VSCode extension

#### Example: Document an existing PostgreSQL table

```yaml
name: public.orders
type: pg.source
description: "All customer orders with their current status and totals"
connection: postgres-default

tags:
  - ecommerce
  - transactions
domains:
  - sales

meta:
  business_owner: "Sales Team"
  data_steward: "data-eng@company.com"
  refresh_frequency: "daily"

depends:
  - public.customers

columns:
  - name: order_id
    type: "SERIAL"
    description: "Auto-incrementing unique identifier for each order"
  - name: customer_id
    type: "INTEGER"
    description: "Foreign key referencing the customers table"
  - name: total_amount
    type: "NUMERIC(12,2)"
    description: "Total monetary value of the order"
  - name: order_date
    type: "TIMESTAMPTZ"
    description: "Timestamp when the order was placed"
  - name: status
    type: "VARCHAR(50)"
    description: "Current order status such as pending, shipped, or delivered"
```
