# Facebook Ads
Facebook Ads is the advertising platform that helps users to create targeted ads on Facebook, Instagram and Messenger.

Bruin supports Facebook Ads as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Facebook Ads into your data warehouse.

In order to set up Facebook Ads connection, you need to add a configuration item in the `.bruin.yml` file and `asset` file. You need `access_token` and `accound_id`. For details on how to obtain these credentials, please refer [here](https://dlthub.com/docs/dlt-ecosystem/verified-sources/facebook_ads#grab-credentials)

Follow the steps below to correctly set up Facebook Ads as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file
To connect to Facebook Ads, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
connections:
  facebookads:
    - name: "my_facebookads"
      access_token: "YOUR_FACEBOOK_ACCESS_TOKEN"
      account_id: "YOUR_ACCOUNT_ID"
```
- `access_token` is associated with Business Facebook App.
- `account_id`  is associated with Ad manager.

### Step 2: Create an asset file for data ingestion
To ingest data from Facebook Ads, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., facebook_ads_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.facebookads
type: ingestr
connection: postgres

parameters:
  source_connection: my_facebookads
  source_table: 'ads'
  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the  Facebook Ads connection defined in .bruin.yml.
- `source_table`: The name of the data table in Facebook Ads you want to ingest. For example, `ads` would ingest data related to ads. You can find the available source tables in Facebook Ads [here](https://bruin-data.github.io/ingestr/supported-sources/facebook-ads.html#tables).

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/facebook_ads_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Facebook Ads table into your Postgres database.