# Klaviyo
[Klaviyo](https://www.Klaviyo.com/) is a marketing automation platform that helps businesses build and manage digital relationships with their customers by connecting through personalized email and enhancing customer loyality.

Bruin supports Klaviyo as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Klaviyo into your data warehouse.

To set up a Klaviyo connection, you need to have Klaviyo API key and source table. For more information, please refer [here](https://bruin-data.github.io/ingestr/supported-sources/klaviyo.html)

Follow the steps below to correctly set up Klaviyo as a data source and run ingestion:

### Step 1: Add a connection to .bruin.yml file

To connect to Klaviyo, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      klaviyo:
        - name: "my_klaviyo"
          api_key: "YOUR_KLAVIYO_API_KEY"
```
- `api_key`: The API key used for authentication with the Klaviyo API.

### Step 2: Create an asset file for data ingestion

To ingest data from Klaviyo, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., klaviyo_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.klaviyo
type: ingestr
connection: postgres

parameters:
  source_connection: my_klaviyo
  source_table: 'events'
  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. It will be always ingestr type for Klaviyo.
- `connection`: This is the destination connection. 
- `source_connection`: The name of the Klaviyo connection defined in .bruin.yml.
- `source_table`: The name of the data table in klaviyo you want to ingest. For example, `events` would ingest data related to events. You can find the available source tables in Klaviyo [here](https://bruin-data.github.io/ingestr/supported-sources/klaviyo.html#available-tables).


### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run ingestr.klaviyo.asset.yml
```
As a result of this command, Bruin will ingest data from the given Klaviyo table into your Postgres database.