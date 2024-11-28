# Adjust
[Adjust](https://www.adjust.com/) is a mobile marketing analytics platform that provides solutions for measuring and optimizing campaigns, as well as protecting user data.

Bruin supports Adjust as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Adjust into your data warehouse.

In order to set up Adjust connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You need the `api_key`. For details on how to obtain these credentials, please refer [here](https://dev.adjust.com/en/api/rs-api/authentication/).

Follow the steps below to correctly set up Adjust as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to Adjust, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      adjust:
        - name: "my_adjust"
          api_key: "abc123"
```
- `api_key`: The API key for the Adjust account.
- `lookback_days`: Optional. The number of days to go back than the given start date for data. Defaults to 30 days. To know more about it, please refer [here](https://bruin-data.github.io/ingestr/supported-sources/adjust.html#lookback-days)

### Step 2: Create an asset file for data ingestion

To ingest data from Adjust, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., adjust_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.adjust
type: ingestr
connection: postgres

parameters:
  source_connection: my_adjust
  source_table: 'creatives'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Adjust connection defined in .bruin.yml.
- `source_table`: The name of the data table in Adjust that you want to ingest. For example, `creatives` is the table of Adjust that you want to ingest. You can find the available source tables in Adjust [here](https://bruin-data.github.io/ingestr/supported-sources/adjust.html#tables).

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/adjust_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Adjust table into your Postgres database.