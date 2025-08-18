# SAP HANA
SAP HANA is an in-memory, column-oriented, relational database management system.

Bruin supports SAP HANA  as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from SAP HANA into your data warehouse.

In order to set up SAP HANA connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.
Follow the steps below to correctly set up SAP HANA as a data source and run ingestion.

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
```     
bruin run assets/hana_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given SAP HANA table into your Postgres database.
