# Google Ads

[Google Ads](https://ads.google.com/), formerly known as Google Adwords, is an online advertising platform developed by Google, where advertisers bid to display brief advertisements, service offerings, product listings, and videos to web users. It can place ads in the results of search engines like Google Search (the Google Search Network), mobile apps, videos, and on non-search websites.

Bruin supports Google Ads as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Google Ads into your data warehouse.

In order to set up Google Ads connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. 

Follow the steps below to correctly set up Google Ads as a data source and run ingestion:


### Step 1: Add a connection to .bruin.yml file

To connect to Google Ads, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      googleads:
        - name: "my-googleads"
          customer_id: "YOUR_GOOGLE_ADS_CUSTOMER_ID"
          dev_token: "YOUR_DEVELOPER_TOKEN"
          service_account_file: "/path/to/service-account.json"

          # alternatively, you can specify the service account json directly
          service_account_json: |
          {
            "type": "service_account",
            ...
          }
          
```

- `customer_id`: The account ID of your google ads account.
- `dev_token`: [Developer Token](https://developers.google.com/google-ads/api/docs/get-started/dev-token) for your application. 
- `service_account_file`: The path to the service account JSON file
- `service_account_json`: The service account JSON content itself


For details on how to obtain these credentials, please refer [here](https://bruin-data.github.io/ingestr/supported-sources/google-ads.html#setting-up-a-google-ads-integration).

### Step 2: Create an asset file for data ingestion

To ingest data from Google Ads, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., google_ads_integration.asset.yml) inside the assets folder and add the following content:

```yaml
name: public.campaigns
type: ingestr
connection: postgres

parameters:
  source_connection: my-googleads
  source_table: 'campaigns'
  destination: postgres
```

- name: The name of the asset.
- type: Specifies the type of the asset. It will be always ingestr type for Google Ads.
- connection: This is the destination connection.
- source_connection: The name of the Google Ads connection defined in .bruin.yml.
- source_table: The name of the resource in Google Ads you want to ingest. 

You can find a list of supported tables [here](https://bruin-data.github.io/ingestr/supported-sources/google-ads.html#tables).

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/google_ads_integration.asset.yml
```
As a result of this command, Bruin will ingest data for the given Google Ads resource into your Postgres database.