# Facebook Ads
Facebook Ads is the advertising platform that helps users to create targeted ads on Facebook, Instagram and Messenger.
ingestr supports Facebook-Ads as a source for [ingestr assets](https://bruin-data.github.io/bruin/assets/ingestr.html), allowing you to ingest data from Facebook-Ads into your data warehouse.

In order to have set up Facebook-Ads connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema. For more information on how to get these credentials check the Facebook-Ads section in [Ingestr documentation](https://bruin-data.github.io/ingestr/getting-started/quickstart.html)
Follow the steps below to correctly set up Facebook Ads as a data source and run ingestion.

**Step 1: Add a Connection to .bruin.yml**

To connect to Facebook Ads, you need to add a configuration item to the connections section of the .bruin.yml file. This configuration must comply with the following schema:

```connections:
  facebook-ads:
    - name: "my_facebook_ads_connection"
      access_token: "YOUR_FACEBOOK_ACCESS_TOKEN"
      account_id: "YOUR_ACCOUNT_ID"
```

**Step 2: Create an Asset File for Data Ingestion**

To ingest data from Facebook Ads, you need to create an asset configuration file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., facebook_ads_ingestion.yml) and add the following content:

```
name: public.facebookads
type: ingestr
connection: postgres

parameters:
  source_connection: facebook-ads
  source_table: 'ads'
    destination: postgres
```

**name**: The name of the asset.

**type**: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.

**connection:** This is the destination connection, which defines where the data should be stored. For example: "postgres" indicates that the ingested data will be stored in a PostgreSQL database.

**parameters:**
**source_connection:** The name of the Facebook Ads connection defined in .bruin.yml.

**source_table**: The name of the data table in Facebook Ads you want to ingest. For example, "ads" would ingest data related to ads.


**Run the Asset to Ingest Data**
```
bruin ingestr run --file facebook_ads_ingestion.yml
```
It will ingest facebook ads data to postgres. 