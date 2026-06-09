# CrateDB

[CrateDB](https://cratedb.com/) is a distributed SQL database for real-time search and analytics workloads.

Bruin supports CrateDB as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from CrateDB into your data warehouse.

In order to set up a CrateDB connection, you need to add a configuration item in the `.bruin.yml` file and in the `asset` file.

Follow the steps below to correctly set up CrateDB as a data source and run ingestion.

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to CrateDB, add a `cratedb` connection to the connections section of the `.bruin.yml` file. CrateDB uses the PostgreSQL wire protocol on port `5432` by default.

```yaml
connections:
  cratedb:
    - name: "cratedb"
      username: "crate"
      password: ""
      host: "localhost"
      port: 5432
      ssl_mode: "disable"
```

- `name`: The name to identify this CrateDB connection
- `username`: The CrateDB username
- `password`: The password for the specified username
- `host`: The host address of the CrateDB server
- `port`: The port number the database server is listening on. Defaults to `5432`.
- `ssl_mode`: Optional PostgreSQL SSL mode. Supported values are `disable`, `allow`, `prefer`, `require`, `verify-ca`, and `verify-full`.

For CrateDB Cloud, use the cluster hostname and `ssl_mode: "require"`:

```yaml
connections:
  cratedb:
    - name: "cratedb-cloud"
      username: "admin"
      password: "<PASSWORD>"
      host: "<CLUSTERNAME>.eks1.eu-west-1.aws.cratedb.net"
      port: 5432
      ssl_mode: "require"
```

### Step 2: Create an asset file for data ingestion

To ingest data from CrateDB, create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., `cratedb_ingestion.yml`) inside the assets folder and add the following content:

```yaml
name: public.cratedb_summits
type: ingestr
connection: postgres

parameters:
  source_connection: cratedb
  source_table: "sys.summits"

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to `ingestr` to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the CrateDB connection defined in `.bruin.yml`.
- `source_table`: The schema-qualified table in CrateDB that you want to ingest, for example `sys.summits`.
- `destination`: The destination platform where the data will be ingested.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/cratedb_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given CrateDB table into your destination database.

For more information, see the [ingestr CrateDB documentation](https://getbruin.com/docs/ingestr/supported-sources/cratedb.html).
