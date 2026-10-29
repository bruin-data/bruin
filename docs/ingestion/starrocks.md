# StarRocks

[StarRocks](https://www.starrocks.io/) is a high-performance analytical (OLAP) database. Besides its own internal storage, it can query open lakehouse table formats — Apache Iceberg, Hudi, Hive, and Delta Lake — through external catalogs.

Bruin supports StarRocks as both a source and a destination for [Ingestr assets](/assets/ingestr): you can ingest data from StarRocks (including lakehouse tables) into another warehouse, or load data from another source into StarRocks' internal storage.

For more information, please refer [here](https://getbruin.com/docs/ingestr/supported-sources/starrocks.html).

Follow the steps below to correctly set up StarRocks as a data source and run ingestion:

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to StarRocks, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      starrocks:
        - name: "my_starrocks"
          host: "localhost"
          username: "root"
          port: 9030                    # optional, defaults to 9030
          password: "YOUR_PASSWORD"     # optional
          database: "analytics"         # optional
          catalog: "iceberg_catalog"    # optional
          ssl: "true"                   # optional
          http_port: 8030               # optional, destination only
          http_scheme: "http"            # optional, destination only
          replication_num: 1            # optional, destination only
```

- `host`: the StarRocks FE (frontend) hostname or IP address. Required.
- `username`: the StarRocks user. Required.
- `port`: the FE query port that speaks the MySQL protocol. Optional, defaults to `9030`.
- `password`: the password for the user. Optional.
- `database`: the default database for unqualified table names. Optional.
- `catalog`: the default catalog. Optional, defaults to the internal catalog (`default_catalog`). Set this to an external catalog name (e.g. an Iceberg/Hudi/Hive catalog configured in StarRocks) to read lakehouse tables.
- `ssl`: enable TLS for the SQL query-port connection. Optional — use `true` to verify the server certificate, or `skip-verify` for TLS without verification. This setting does not control Stream Load HTTPS or its certificate verification.
- `http_port`: the FE HTTP or HTTPS port used for Stream Load when StarRocks is the destination. Optional, defaults to `8030` for either scheme.
- `http_scheme`: `http` (default) or `https` for Stream Load when StarRocks is the destination. Requires ingestr v1.1.78 or later when explicitly set. HTTPS verifies certificates using the system trust store and rejects plaintext redirect and error-log targets.
- `replication_num`: the replica count for tables created when StarRocks is the destination. Optional, defaults to the cluster default (set `1` for a single-backend cluster).

### Step 2: Create an asset file for data ingestion

To ingest data from StarRocks, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., starrocks_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.events
type: ingestr
connection: postgres

parameters:
  source_connection: my_starrocks
  source_table: 'analytics.events'
  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. It will be always ingestr type for StarRocks.
- `connection`: This is the destination connection.
- `source_connection`: The name of the StarRocks connection defined in .bruin.yml.
- `source_table`: The table in StarRocks to ingest.

## Source tables

StarRocks organizes tables as `catalog.database.table`. The `source_table` accepts any of these forms:

| Format | Description |
|--------|-------------|
| `table` | uses the default catalog and database from the connection |
| `database.table` | uses the default catalog from the connection |
| `catalog.database.table` | fully qualified — reads from any internal or external catalog |

Anything specified in the `source_table` takes priority over the catalog/database defaults from the connection.

Examples:

- `analytics.events` — the `events` table in the `analytics` database (internal catalog).
- `iceberg_catalog.lakehouse.trips` — an Apache Iceberg table reached through the `iceberg_catalog` external catalog.
- `hudi_catalog.lakehouse.payments` — an Apache Hudi table reached through the `hudi_catalog` external catalog.

Because StarRocks federates lakehouse formats through external catalogs, reading an Iceberg/Hudi/Hive/Delta table uses the same `catalog.database.table` form — no extra configuration is needed on the Bruin side.

## Incremental loading

Provide `incremental_key` together with an interval to pull only the rows in that window, and use the `merge` strategy with a primary key to upsert them. Without an interval, ingestr reads all rows and merges on the primary key (an idempotent full refresh).

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/starrocks_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given StarRocks table into your Postgres database.

## Using StarRocks as a destination

StarRocks can also be the destination of an Ingestr asset. Set `destination: starrocks` and point `connection` (or `destination_connection`) at the StarRocks connection. The asset `name` sets the destination `database.table` in StarRocks' internal catalog.

```yaml
name: analytics.events
type: ingestr
connection: my_starrocks

parameters:
  source_connection: my_postgres
  source_table: 'public.events'
  destination: starrocks
```

Bruin creates the destination table if it does not exist and loads rows via StarRocks [Stream Load](https://docs.starrocks.io/docs/loading/StreamLoad/). The `replace`, `append`, and `merge` strategies are supported; `merge` requires a primary key and produces a StarRocks PRIMARY KEY table. Only the internal catalog is writable — external lakehouse catalogs (Iceberg/Hudi/Hive) are read-only, so they can be used as a source but not a destination.

For an HTTPS Stream Load endpoint such as CelerData on port `443`, configure the connection as follows:

```yaml
connections:
  starrocks:
    - name: my_starrocks
      host: your-celerdata-host
      username: your-user
      password: YOUR_PASSWORD
      database: analytics
      port: 9030
      ssl: "true"
      http_scheme: https
      http_port: 443
```

These connection settings apply to batch ingestion and streaming CDC. Set `http_port` explicitly when the HTTPS endpoint uses a port other than `8030`.

### CDC replication

StarRocks supports PostgreSQL and MySQL CDC snapshots, incremental catch-up, continuous streaming, and full refresh. Configure the source for [PostgreSQL CDC](/platforms/postgres#cdc-change-data-capture) or [MySQL CDC](/platforms/mysql#cdc-change-data-capture) before running these assets.

For a batch PostgreSQL sync, save this asset as `assets/starrocks_events.yml`:

```yaml
name: analytics.events
type: ingestr
connection: my_starrocks

parameters:
  source_connection: my_postgres
  source_table: public.events
  destination: starrocks
  cdc: "true"
```

```bash
bruin run assets/starrocks_events.yml
```

The first run snapshots `public.events` into `analytics.events`; later runs resume from the stored checkpoint, catch up, and exit. Bruin automatically selects `merge` for CDC, and ingestr obtains keys from the source. For keyed sources, an existing destination must be a PRIMARY KEY table with matching key columns. Use `bruin run --full-refresh assets/starrocks_events.yml` to rebuild it from a fresh snapshot.

For continuous MySQL replication, save this asset as `assets/starrocks_orders.yml`:

```yaml
name: analytics.orders
type: ingestr
connection: my_starrocks

parameters:
  source_connection: my_mysql
  source_table: orders
  destination: starrocks
  cdc: "true"
  cdc_server_id: "23001"
  stream: true
  cdc_stream_flush_interval: 5s
  cdc_stream_flush_records: 10000
```

```bash
bruin run --stream assets/starrocks_orders.yml
```

The same streaming parameters work with PostgreSQL. Streaming assets run until interrupted and require `bruin run --stream`; normal runs skip them. See [Refreshing a stream](/assets/ingestr#refreshing-a-stream) for full refresh while streaming.

### Multiple CDC tables

Set `source_table: "*"` to replicate all eligible source tables, or use a comma-separated list such as `source_table: public.events,public.orders` to select a subset. Set `cdc_dest_schema: analytics` to choose the destination database, or set `database: analytics` on the StarRocks connection. Multi-table CDC requires one of these database settings.

In multi-table mode, ingestr chooses each target name from the source schema and table: PostgreSQL `sales.events` becomes `analytics.sales_events`. PostgreSQL's `public` schema is omitted, so `public.events` becomes `analytics.events`. The asset `name` does not select the individual targets. For a single source table, the asset `name` selects the target and `cdc_dest_schema` is ignored.

### CDC keys, deletes, and requirements

Keyed tables keep one row per source key, including composite keys. By default, source deletes remain as rows with `_cdc_deleted = true`; query the current mirror with:

```sql
SELECT * FROM analytics.events WHERE NOT _cdc_deleted;
```

Set `cdc_delete_behavior: hard` to remove deleted rows instead. For externally loaded PostgreSQL baselines using [`cdc_snapshot_mode: never`](/platforms/postgres#cdc-without-snapshots), untouched rows have NULL CDC metadata; filter with `COALESCE(_cdc_deleted, false) = false` to include them.

- StarRocks PRIMARY KEY restrictions apply, including the cluster's encoded key size limit (128 bytes by default). PostgreSQL tables without keys but with `REPLICA IDENTITY FULL` become append-only change logs and can contain replayed events. Filtering `_cdc_deleted` does not turn those logs into a current-state mirror.
- Ingestr stores checkpoints and ownership metadata in `_bruin_staging`. Preserve its tables and views across runs. The destination user needs permission to create databases, tables, and views, inspect metadata and load transactions, and select, insert, alter, delete, truncate, and drop managed tables.
- The client must reach both the SQL query port and the Stream Load HTTP or HTTPS endpoint, including the backend address returned by the frontend's redirect. Configure `http_port`, `http_scheme`, and `replication_num` on the existing StarRocks connection as needed.
- Keep one writer per target and avoid external destination schema changes during ingestion. Multi-table replication does not apply changes atomically across all tables. MySQL changes to a captured table's schema stop replication and require a full refresh.

See the [ingestr StarRocks reference](https://getbruin.com/docs/ingestr/supported-sources/starrocks.html#postgresql-cdc) for source schema changes, PostgreSQL TOAST handling, and destination limits.
