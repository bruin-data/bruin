# Applovin
[AppLovin](https://www.applovin.com/) is a mobile technology company that allows developers of all sizes to market, monetize, analyze, and publish their apps through its mobile advertising, marketing, and analytics platforms — MAX, AppDiscovery, and SparkLabs.

Bruin supports AppLovin as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from AppLovin into your data platform.

To set up a AppLovin connection, you must add a configuration item in the `.bruin.yml` and `asset` file. You need `api_key` that is report key. You can generate a report key from your [analytics dashboard](https://dash.applovin.com/login#keys).

Follow the steps below to set up AppLovin correctly as a data source and run ingestion.
### Step 1: Add a connection to the .bruin.yml file
```yaml
connections:
      applovin:
        - name: "applovin"
          api_key: "api_key_123"
```
- `api_key` (required): It is the `report key` which is used for authenticating the request.

### Step 2: Create an asset file for data ingestion
To ingest data from AppLovin, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., applovin_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.applovin
type: ingestr
connection: postgres

parameters:
  source_connection: applovin
  source_table: 'publisher-report'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the asset’s type. Set this to `ingestr` to use the ingestr data pipeline. For AppLovin, it will be always `ingestr`.
- `source_connection`: The name of the AppLovin connection defined in `.bruin.yml`.
- `source_table`: The name of the table in AppLovin to ingest. You can find the available source tables [here](https://bruin-data.github.io/ingestr/supported-sources/applovin.html#tables).
- `destination`: The name of the destination connection.


### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/applovin_asset.yml
```
As a result of this command, Bruin will ingest data from the given AppLovin table into your Postgres database.





