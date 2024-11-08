# Facebook Ads
Facebook Ads is the advertising platform that helps users to create targeted ads on Facebook, Instagram and Messenger.
ingestr supports Facebook-Ads as a source for [ingestr assets](https://bruin-data.github.io/bruin/assets/ingestr.html), allowing you to ingest data from Facebook-Ads into your data warehouse.

In order to have set up Facebook-Ads connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema. For more information on how to get these credentials, read [here](https://bruin-data.github.io/ingestr/supported-sources/facebook-ads.html)

Follow the steps below to correctly set up Facebook Ads as a data source and run ingestion.

**Step 1: Add a Connection to .bruin.yml**

To connect to Facebook Ads, you need to add a configuration item to the connections section of the [.bruin.yml file](https://bruin-data.github.io/bruin/connections/overview.html). This configuration must comply with the following schema:

```yaml
connections:
  facebookads:
    - name: "my_facebookads"
      access_token: "YOUR_FACEBOOK_ACCESS_TOKEN"
      account_id: "YOUR_ACCOUNT_ID"
```

**Step 2: Create an Asset File for Data Ingestion**

To ingest data from Facebook Ads, you need to create an [asset configuration file](https://bruin-data.github.io/bruin/assets/ingestr.html#template). This file defines the data flow from the source to the destination. Create a YAML file (e.g., facebook_ads_ingestion.yml) and add the following content:

```yaml
name: public.facebookads
type: ingestr
connection: postgres

parameters:
  source_connection: my_facebookads
  source_table: 'ads'
  destination: postgres
```

**name**: The name of the asset.

**type**: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.

**connection:** This is the destination connection, which defines where the data should be stored. For example: "postgres" indicates that the ingested data will be stored in a PostgreSQL database.

**parameters:**
**source_connection:** The name of the Facebook Ads connection defined in .bruin.yml.

**source_table**: The name of the data table in Facebook Ads you want to ingest. For example, "ads" would ingest data related to ads. [Available source tables in Facebook_Ads](https://bruin-data.github.io/ingestr/supported-sources/facebook-ads.html#available-tables)


**Step 3: [Run](https://bruin-data.github.io/bruin/commands/run.html) Asset to Ingest Data**
```
bruin run --file facebook_ads.yml
```
It will ingest facebook ads data to postgres. 