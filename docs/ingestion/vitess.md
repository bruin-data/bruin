# Vitess

[Vitess](https://vitess.io/) is a MySQL-compatible database clustering and sharding system originally built at YouTube. It speaks the MySQL wire protocol through vtgate, and Bruin connects to it through a dedicated `vitess` connection. Vitess can be used as both a **source** and a **destination** for [Ingestr assets](/assets/ingestr), and Vitess [change data capture](#change-data-capture-cdc) is also supported through VStream.

> [!TIP]
> If you use the managed [PlanetScale](/ingestion/planetscale) platform, follow the PlanetScale guide instead — it uses the hosted `psdbconnect` API rather than a directly reachable vtgate gRPC port.

Follow the steps below to set up Vitess and run ingestion.

## Configuration

### Step 1: Add a connection to .bruin.yml file

Add a `vitess` connection to the `connections` section of your `.bruin.yml` file, pointing at your vtgate endpoint:

```yaml
  vitess:
    - name: "vitess"
      username: "user"
      password: "password"
      host: "vtgate.internal"
      port: 15306
      database: "commerce"
      # Optional, required only for change data capture:
      grpc_port: 15991
```

- `name`: The name to identify this connection
- `username`: The username to connect through vtgate
- `password`: The password for the user
- `host`: The vtgate host
- `port`: The vtgate MySQL protocol port (e.g. `15306`)
- `database`: The Vitess keyspace to connect to
- `grpc_port`: (Optional) vtgate's gRPC port, used by [CDC](#change-data-capture-cdc) (e.g. `15991`)
- `grpc_host`: (Optional) Host override for the gRPC connection when it differs from the MySQL host
- `grpc_tls`: (Optional) Set to `true` to use TLS for the gRPC connection
- `ssl_ca_path`: (Optional) The path to the CA certificate file
- `ssl_cert_path`: (Optional) The path to the client certificate file
- `ssl_key_path`: (Optional) The path to the client key file

### Step 2: Create an asset file for data ingestion

To ingest data from Vitess, create an [asset configuration](/assets/ingestr#asset-structure) file (e.g. `vitess_ingestion.yml`) inside the assets folder:

```yaml
name: public.orders
type: ingestr
connection: bigquery

parameters:
  source_connection: vitess
  source_table: 'orders'
  destination: bigquery
```

- `name`: The name of the asset.
- `type`: Set this to `ingestr` to use the ingestr data pipeline.
- `connection`: The destination connection where the data should be stored.
- `source_connection`: The name of the Vitess connection defined in `.bruin.yml`.
- `source_table`: The name of the table in the keyspace that you want to ingest.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/vitess_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given Vitess table into your destination.

## Vitess as a destination

You can also use Vitess as a destination by pointing an ingestr asset's `connection` and `destination` at a Vitess connection:

```yaml
name: orders
type: ingestr
connection: vitess

parameters:
  source_connection: my_postgres
  source_table: 'public.orders'
  destination: vitess
```

When loading into Vitess, keep two things in mind:

- **The target keyspace must already exist.** ingestr cannot create keyspaces through vtgate.
- **Only unsharded (single-shard) keyspaces are supported** as destinations, due to vindex and atomic-operation constraints.

## Change data capture (CDC)

Vitess CDC streams inserts, updates, and deletes through vtgate's VStream gRPC API. It is enabled by setting `cdc: "true"` on an ingestr asset with a Vitess source connection, and requires the vtgate gRPC port — configure it once as `grpc_port` on the connection (see [Step 1](#step-1-add-a-connection-to-bruin-yml-file)).

| Parameter | Required | Description |
|-----------|----------|-------------|
| `cdc` | Yes | Set to `"true"` to enable CDC mode |
| `cdc_grpc_port` | No | Overrides the connection's `grpc_port` for this asset |
| `cdc_grpc_host` | No | Overrides the connection's `grpc_host` for this asset |
| `cdc_grpc_tls` | No | Overrides the connection's `grpc_tls` for this asset |
| `cdc_dest_schema` | No | Destination schema to use for multi-table CDC runs |
| `incremental_strategy` | No | Defaults to `"merge"` when CDC is enabled; can be overridden to `"append"` |

Requirements:

- Read access to the keyspace over both the MySQL protocol and vtgate's VStream gRPC API.
- Source tables must have primary keys, or `primary_key` must be set on the asset columns.
- Source tables must not contain `ENUM`, `SET`, or `BIT` columns.

> [!NOTE]
> When CDC is enabled, primary key columns do not need to be specified in the asset definition — they are determined automatically from the source table.

### Example: Vitess VStream CDC

```yaml
name: orders
type: ingestr
connection: bigquery

parameters:
  source_connection: vitess
  source_table: 'orders'
  destination: bigquery
  cdc: "true"
```

The example above relies on `grpc_port` being set on the `vitess` connection. For the managed PlanetScale platform, see [PlanetScale](/ingestion/planetscale).
