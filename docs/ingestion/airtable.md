# Airtable
[Airtable](https://www.airtable.com/) is a cloud-based platform that combines spreadsheet and database functionalities, designed for data management and collaboration.

Bruin supports Airtable as a source for [Ingestr assets](https://bruin-data.github.io/bruin/assets/ingestr.html), and you can use it to ingest data from Airtable into your data warehouse.

In order to set up Airtable connection, you need to add a configuration item in the `.bruin.yml` file and in `asset file`. 
You will need the `base_id` and `access_token`. For details on how to obtain these credentials, read [here](https://dlthub.com/docs/dlt-ecosystem/verified-sources/airtable#setup-guide).

Follow the steps below to correctly set up Airtable as a data source and run ingestion.

```yml
    connections:
      airtable:
        - name: "connection_name"
          base_id: "id123",
          access_token: "key123",
```

- base_id: A unique identifier for an Airtable base.
- access_token: A personal access token for authentication with the Airtable API.

**Step 2: Create an Asset File for Data Ingestion**

To ingest data from Airtable Ads, you need to create an [asset configuration file](https://bruin-data.github.io/bruin/assets/ingestr.html#template). This file defines the data flow from the source to the destination. Create a YAML file (e.g., airtable_ingestion.yml) and add the following content:

```yaml
name: public.airtable
type: ingestr
connection: postgres

parameters:
  source_connection: connection_name
  source_table: 'Details'

  destination: postgres
```
- name: The name of the asset.

- type: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.

- connection: This is the destination connection, which defines where the data should be stored. For example: "postgres" indicates that the ingested data will be stored in a PostgreSQL database.

**parameters:**
- source_connection: The name of the airtable connection defined in .bruin.yml.

- source_table: The name of the data table in airtable that you want to ingest. For example, "Details" is the table of airtable that you want to ingest.

**Step 3: [Run](https://bruin-data.github.io/bruin/commands/run.html) Asset to Ingest Data**
```
bruin run --file airtable_ingestion.yml
```
It will ingest airtable data to postgres. 
