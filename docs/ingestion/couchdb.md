# CouchDB

[Apache CouchDB](https://couchdb.apache.org/) is an open-source document database that stores data as JSON.

Bruin supports CouchDB as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from CouchDB into your data warehouse.

In order to set up a CouchDB connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up CouchDB as a data source and run ingestion.

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to CouchDB, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      couchdb:
        - name: "couchdb"
          username: "admin"
          password: "password123"
          host: "localhost"
          port: 5984
          ssl: false
```

- `name`: The name to identify this CouchDB connection
- `username`: (Optional) The CouchDB username. Can be omitted for databases that permit anonymous reads
- `password`: (Optional) The password for the specified username
- `host`: The host address of the CouchDB server
- `port`: (Optional) The port the server is listening on. Defaults to `5984` over HTTP and `443` over HTTPS
- `ssl`: (Optional) Set to `true` to connect over HTTPS. Defaults to `false`

For a server behind HTTPS:

```yaml
    connections:
      couchdb:
        - name: "couchdb-cloud"
          username: "admin"
          password: "password123"
          host: "couchdb.example.com"
          ssl: true
```

Bruin URL-encodes the username and password, so credentials containing special characters such as `@`, `/`, `#`, or `?` can be used as-is.

### Step 2: Create an asset file for data ingestion

To ingest data from CouchDB, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., couchdb_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.orders
type: ingestr
connection: postgres

parameters:
  source_connection: couchdb
  source_table: 'orders'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the CouchDB connection defined in .bruin.yml.
- `source_table`: The name of the CouchDB database to read. The database is not part of the connection, so one connection can be used for every database on the server.

To update existing rows by primary key instead of replacing the destination table, use the `merge` strategy:

```yaml
name: public.orders
type: ingestr
connection: postgres

parameters:
  source_connection: couchdb
  source_table: 'orders'

  destination: postgres
  incremental_strategy: merge
```

To load only a sample of the documents, set `sql_limit`:

```yaml
name: public.orders_sample
type: ingestr
connection: postgres

parameters:
  source_connection: couchdb
  source_table: 'orders'
  sql_limit: 1000

  destination: postgres
```

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/couchdb_ingestion.yml
```

As a result of this command, Bruin will ingest the documents from the given CouchDB database into your Postgres database.

## Source tables

| Source table | Primary key | Incremental key | Strategy |
|--------------|-------------|-----------------|----------|
| `<database_name>` | `_id` | - | `replace` |

Columns are inferred from the documents and include `_id` and `_rev`. Nested objects and arrays are stored as JSON.

## Incremental loading

CouchDB does not support incremental keys, time intervals, or change data capture. Every run scans the full database:

- The default `replace` strategy replaces the destination with the current documents.
- The `merge` strategy updates existing rows by `_id`, but does not remove rows for documents deleted from CouchDB.

## Limitations

- Design documents, local documents, and deleted documents are not ingested.
- Attachment metadata is included, but attachment contents are not downloaded.
- Reads are not a consistent snapshot if documents change during ingestion.
- Replacing from an empty database clears the destination and creates only `_id` (and any custom primary-key columns) as string columns, since there are no documents to infer other columns from.
