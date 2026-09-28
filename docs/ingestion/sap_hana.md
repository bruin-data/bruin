# SAP HANA

SAP HANA is an in-memory, column-oriented, relational database management system.

Bruin supports SAP HANA both as a source and as a destination for [Ingestr assets](/assets/ingestr). You can use it to ingest data from SAP HANA into your data warehouse, or load data from other sources into SAP HANA.

In order to set up SAP HANA connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.
Follow the steps below to correctly set up SAP HANA as a data source and run ingestion.

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to SAP HANA, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      hana:
        - name: "connection_name"
          username: "hana_user"
          password: "hana123"
          host: "hana-xyz.sap.com"
          port: 39013
          database: "systemdb"
```

- `name`: The name to identify this SAP HANA connection
- `username`: The SAP HANA username with access to the database
- `password`: The password for the specified username
- `host`: The host address of the SAP HANA server
- `port`: The port number the database server is listening on ( default is 30015)
- `database`:  The name of the database to connect to

### Step 2: Create an asset file for data ingestion

To ingest data from SAP HANA, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., hana_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.hana
type: ingestr
connection: postgres

parameters:
  source_connection: connection_name
  source_table: 'users.details'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the SAP HANA connection defined in .bruin.yml.
- `source_table`: The name of the data table in SAP HANA that you want to ingest.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/hana_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given SAP HANA table into your Postgres database.

## Using SAP HANA as a Destination

SAP HANA can also be used as a destination to load data from other sources. The supported incremental strategies are `replace`, `append`, `merge`, and `delete+insert`.

### Example: Loading data into SAP HANA

To use SAP HANA as a destination, create an asset file that specifies SAP HANA as the `destination`:

```yaml
name: analytics.users
type: ingestr
connection: connection_name

parameters:
  source_connection: postgres
  source_table: 'public.users'

  destination: hana
```

- `name`: The destination table in SAP HANA, as `schema.table` or `table`.
- `connection`: The name of the SAP HANA connection defined in `.bruin.yml`, used as the destination.
- `source_connection`: The name of the source connection (e.g., Postgres), which must also be configured as a Bruin connection.
- `source_table`: The table from the source to ingest.
- `destination`: Set to `hana` to use SAP HANA as the destination.

When you run this asset, Bruin will load data from the source into the specified SAP HANA table.

For SAP HANA Cloud, use port `443` in the connection; TLS is enabled automatically.
