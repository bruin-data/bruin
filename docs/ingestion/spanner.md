# GCP Spanner
GCP Spanner is a fully managed, mission-critical database service that combines the capabilities of relational databases, key-value stores, and search engines.

Bruin supports GCP Spanner as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Spanner into your data warehouse. 

In order to set up a GCP Spanner connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up GCP Spanner as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file
To connect to GCP Spanner, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:
```yaml
    connections:
      spanner:
        - name: "my-spanner"
          project_id: "project_123"
          instance_id: "instance_123"
          database: "my-db"
          service_account_file: "./service_account.json"
```
URI parameters:
- `project_id`: Your Google Cloud project ID
- `instance_id`: The Spanner instance ID
- `database`: The database name
- `service_account_file`: The path to the service account JSON file
- `service_account_json`: The service account JSON content itself. This is an alternative to using `service_account_file`

### Step 2: Create an asset file for data ingestion

To ingest data from GCP Spanner, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., gcp_spanner_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.spanner
type: ingestr
connection: neon

parameters:
  source_connection: my-spanner
  source_table: 'tbl1'

  destination: postgres
```
- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: The name of the destination connection. For example, "neon" is a connection name.
- `source_connection`: The name of the GCP Spanner connection defined in .bruin.yml.
- `source_table`: The name of the data table in GCP Spanner that you want to ingest.
- `destination`: The name or type of the destination connection, which is Postgres.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/gcp_spanner_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given GCP Spanner table into your Postgres database.

<img alt="Spanner" src="./media/spanner_ingestion.png">