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
          replication_num: 1            # optional, destination only
```

- `host`: the StarRocks FE (frontend) hostname or IP address. Required.
- `username`: the StarRocks user. Required.
- `port`: the FE query port that speaks the MySQL protocol. Optional, defaults to `9030`.
- `password`: the password for the user. Optional.
- `database`: the default database for unqualified table names. Optional.
- `catalog`: the default catalog. Optional, defaults to the internal catalog (`default_catalog`). Set this to an external catalog name (e.g. an Iceberg/Hudi/Hive catalog configured in StarRocks) to read lakehouse tables.
- `ssl`: enable a TLS-encrypted connection. Optional — use `true` to verify the server certificate, or `skip-verify` for TLS without verification.
- `http_port`: the FE HTTP port used for Stream Load when StarRocks is the destination. Optional, defaults to `8030`.
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

StarRocks can also be the destination of an Ingestr asset. Set `destination: starrocks` and point `connection` (or `destination_connection`) at the StarRocks connection; the `name` of the asset is the fully qualified `database.table` written into StarRocks' internal catalog.

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
