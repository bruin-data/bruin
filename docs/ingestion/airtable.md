# Airtable
[Airtable](https://www.airtable.com/) is a cloud-based platform that combines spreadsheet and database functionalities, designed for data management and collaboration.

Bruin supports Airtable as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Airtable into your data warehouse.

In order to set up Airtable connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You need the `base_id` and `access_token`. For details on how to obtain these credentials, please refer [here](https://dlthub.com/docs/dlt-ecosystem/verified-sources/airtable#setup-guide).

Follow the steps below to correctly set up Airtable as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to Airtable, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      airtable:
        - name: "connection_name"
          base_id: "id123",
          access_token: "key123",
```

- `base_id`: A unique identifier for an Airtable base.
- `access_token`: A personal access token for authentication with the Airtable API.

### Step 2: Create an asset file for data ingestion

To ingest data from Airtable, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., airtable_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.airtable
type: ingestr
connection: postgres

parameters:
  source_connection: connection_name
  source_table: 'details'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the airtable connection defined in .bruin.yml.
- `source_table`: The name of the data table in airtable that you want to ingest. For example, `details` is the table of airtable that you want to ingest.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/airtable_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Airtable table into your Postgres database.


<img width="1159" alt="airtable" src="https://github.com/user-attachments/assets/416f8a07-be28-43a2-a227-9d6077276f1d">
