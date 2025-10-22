# Couchbase
Couchbase is a distributed NoSQL cloud database that delivers unmatched performance, scalability, and flexibility for building modern applications.

Bruin supports Couchbase as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Couchbase into your data warehouse.

In order to set up Couchbase connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up Couchbase as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file
To connect to Couchbase, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:
```yaml
    connections:
      couchbase:
        - name: "couchbase"
          username: "admin"
          password: "password123"
          host: "localhost"
          bucket: "mybucket"
          ssl: false
```
- `name`: The name to identify this Couchbase connection
- `username`: The Couchbase username with access to the cluster
- `password`: The password for the specified username
- `host`: The host address of the Couchbase server (e.g., localhost or an IP address)
- `bucket`: (Optional) The name of the bucket to connect to. Can be specified in the URI path or separately
- `ssl`: (Optional) Set to true for Couchbase Capella (cloud) deployments, false or omit for Couchbase Server (self-hosted/on-premises)

For Couchbase Capella (cloud) deployments:
```yaml
    connections:
      couchbase:
        - name: "couchbase-cloud"
          username: "admin"
          password: "password123"
          host: "cb.xxx.cloud.couchbase.com"
          bucket: "travel-sample"
          ssl: true
```

### Step 2: Create an asset file for data ingestion

To ingest data from Couchbase, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., couchbase_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.couchbase
type: ingestr
connection: postgres

parameters:
  source_connection: couchbase
  source_table: 'mybucket.myscope.mycollection'

  destination: postgres
```

For default scope and collection:
```yaml
name: public.couchbase
type: ingestr
connection: postgres

parameters:
  source_connection: couchbase
  source_table: 'mybucket._default._default'

  destination: postgres
```

When bucket is specified in connection config:
```yaml
name: public.couchbase
type: ingestr
connection: postgres

parameters:
  source_connection: couchbase-cloud
  source_table: 'inventory.airport'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Couchbase connection defined in .bruin.yml.
- `source_table`: The table/collection in Couchbase. Format can be either `bucket.scope.collection` or `scope.collection` (when bucket is in connection config).

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/couchbase_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Couchbase collection into your Postgres database.
