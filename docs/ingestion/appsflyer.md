# Appsflyer
[Appsflyer](https://www.appsflyer.com/) is a mobile marketing analytics and attribution platform that helps businesses track, measure, and optimize their app marketing efforts across various channels.

Bruin supports Appsflyer as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Appsflyer into your data warehouse.

In order to set up an Appsflyer connection, you need to add a configuration item to `connections` in the `.bruin.yml` file and in `asset` file. You need the `api_key`. For more information on how to get these credentials check the Appsflyer section in [Ingestr documentation](https://bruin-data.github.io/ingestr/getting-started/quickstart.html)

Follow the steps below to correctly set up Appsflyer as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to Appsflyer, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
connections:
  appsflyer:
    - name: "my_appsflyer"
      api_key: "abc123"
```

- `api_key`: The API key for the Appsflyer account.

### Step 2: Create an asset file for data ingestion

To ingest data from Appsflyer, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., appsflyer_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.appsflyer
type: ingestr
connection: postgres

parameters:
  source_connection: my_appsflyer
  source_table: 'campaigns'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Appsflyer connection defined in .bruin.yml.
- `source_table`: The name of the data table in Appsflyer that you want to ingest.

## Available Source Tables

- `campaigns`: Retrieves data for campaigns, detailing the app's costs, loyal users, total installs, and revenue over multiple days.
- `creatives`: Retrieves data for a creative asset, including revenue and cost.
- `custom:<dimensions>:<metrics>`: Retrieves data for custom tables, which can be specified by the user.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/appsflyer_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Appsflyer table into your Postgres database.