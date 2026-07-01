# PlanetScale

[PlanetScale](https://planetscale.com/) is a managed, MySQL-compatible database platform built on [Vitess](https://vitess.io/). Because it speaks the MySQL wire protocol, Bruin connects to PlanetScale through a regular [`mysql` connection](/platforms/mysql), and it can be used as both a **source** and a **destination** for [Ingestr assets](/assets/ingestr). PlanetScale [change data capture](#change-data-capture-cdc) is also supported.

Follow the steps below to set up PlanetScale and run ingestion.

## Configuration

### Step 1: Add a connection to .bruin.yml file

PlanetScale uses the MySQL connector, so add a `mysql` connection to the `connections` section of your `.bruin.yml` file. Use the connection details from your PlanetScale branch's password:

```yaml
  mysql:
    - name: "planetscale"
      username: "xxxxxxxxxxxxx"
      password: "pscale_pw_xxxxxxxxxxxx"
      host: "aws.connect.psdb.cloud"
      port: 3306
      database: "my_database"
```

- `name`: The name to identify this connection
- `username`: The PlanetScale username (from the branch password)
- `password`: The PlanetScale password
- `host`: The PlanetScale host, e.g. `aws.connect.psdb.cloud`
- `port`: The MySQL protocol port, usually `3306`
- `database`: The PlanetScale database (keyspace) name

> [!NOTE]
> PlanetScale requires encrypted connections. TLS is enabled automatically for `*.psdb.cloud` hosts, so you do not need any extra configuration. For a custom domain or private endpoint that does not end in `.psdb.cloud`, set `?tls=true` on the connection or ingest through CDC's `cdc_backend`.

### Step 2: Create an asset file for data ingestion

To ingest data from PlanetScale, create an [asset configuration](/assets/ingestr#asset-structure) file (e.g. `planetscale_ingestion.yml`) inside the assets folder:

```yaml
name: public.orders
type: ingestr
connection: bigquery

parameters:
  source_connection: planetscale
  source_table: 'orders'
  destination: bigquery
```

- `name`: The name of the asset.
- `type`: Set this to `ingestr` to use the ingestr data pipeline.
- `connection`: The destination connection where the data should be stored.
- `source_connection`: The name of the PlanetScale (`mysql`) connection defined in `.bruin.yml`.
- `source_table`: The name of the table in PlanetScale that you want to ingest.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/planetscale_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given PlanetScale table into your destination.

## PlanetScale as a destination

Because PlanetScale speaks MySQL, you can also use it as a destination by pointing an ingestr asset's `connection` and `destination` at a PlanetScale `mysql` connection:

```yaml
name: orders
type: ingestr
connection: planetscale

parameters:
  source_connection: my_postgres
  source_table: 'public.orders'
  destination: planetscale
```

When loading into PlanetScale, keep two things in mind:

- **Direct DDL must be allowed on the target branch.** The `replace` strategy and any table creation issue `CREATE` / `RENAME` statements. On a branch with safe migrations enabled, PlanetScale rejects these — load into a development branch (or a branch with safe migrations off), or pre-create the tables and use `append`/`merge`. The PlanetScale database (keyspace) must already exist; ingestr does not create it.
- **Only unsharded keyspaces are supported** as destinations. A sharded keyspace is rejected at connect time.

## Change data capture (CDC)

PlanetScale CDC streams inserts, updates, and deletes through PlanetScale's hosted `psdbconnect` API over TLS. It reuses the database credentials already in the connection — no separate token is required — and is selected automatically for `*.psdb.cloud` hosts.

CDC is enabled by setting `cdc: "true"` on an ingestr asset with a PlanetScale (`mysql`) source connection.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `cdc` | Yes | Set to `"true"` to enable CDC mode |
| `cdc_backend` | No | Force the backend: `planetscale` selects psdbconnect (useful for custom domains / private endpoints), `vstream` selects the self-hosted Vitess VStream path |
| `cdc_dest_schema` | No | Destination schema to use for multi-table CDC runs |
| `incremental_strategy` | No | Defaults to `"merge"` when CDC is enabled; can be overridden to `"append"` |

Requirements:

- PlanetScale credentials with read access to the branch/keyspace.
- Source tables must have primary keys, or `primary_key` must be set on the asset columns.
- Source tables must not contain `ENUM`, `SET`, or `BIT` columns.

> [!NOTE]
> When CDC is enabled, primary key columns do not need to be specified in the asset definition — they are determined automatically from the source table. PlanetScale delivers only the primary keys of deleted rows; the destination marks them deleted without disturbing the other columns.

### Example: PlanetScale CDC

```yaml
name: orders
type: ingestr
connection: bigquery

parameters:
  source_connection: planetscale
  source_table: 'orders'
  destination: bigquery
  cdc: "true"
```

### Example: CDC on a custom domain

For a custom domain or private endpoint that does not end in `.psdb.cloud`, force the psdbconnect backend explicitly:

```yaml
name: orders
type: ingestr
connection: bigquery

parameters:
  source_connection: planetscale
  source_table: 'orders'
  destination: bigquery
  cdc: "true"
  cdc_backend: planetscale
```

> [!NOTE]
> If a run fails because the stored CDC position is invalid, re-run with `--full-refresh` to rebuild the destination from a fresh snapshot.

For self-hosted Vitess (VStream over vtgate's gRPC port), see [Vitess](/ingestion/vitess).
