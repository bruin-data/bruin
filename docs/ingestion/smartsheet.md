# Smartsheet
[Smartsheet](https://www.smartsheet.com/) is a software as a service (SaaS) offering for collaboration and work management.

ingestr supports Smartsheet as a source.

Bruin supports Smartsheet as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Smartsheet into your data warehouse.

In order to set up Smartsheet connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You need `access_token` . For details on how to obtain these credentials, please refer [here](https://getbruin.com/docs/ingestr/supported-sources/smartsheets.html#setting-up-a-smartsheet-integration).

Follow the steps below to correctly set up Smartsheet as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to Smartsheet, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
connections:
    smartsheet:
        - name: "smartsheet"
          access_token: "access_token"
```
- `access_token`: Your Smartsheet API access token.

### Step 2: Create an asset file for data ingestion

To ingest data from Smartsheet, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., smartsheet_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.smartsheet
type: ingestr
connection: postgres

parameters:
  source_connection: smartsheet
  source_table: '1234567890123456'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: "postgres" indicates that the ingested data will be stored in a PostgreSQL database.
- `source_connection`: The name of the Smartsheet connection defined in .bruin.yml.
- `source_table`: The source table will be the `sheet_id` of the Smartsheet you want to ingest. You can find the sheet_id by opening the sheet in Smartsheet and going to File > Properties. The Sheet ID will be listed there.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/Smartsheet_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Smartsheet table into your Postgres database.
