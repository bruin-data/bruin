# Shopify
[Shopify](https://www.Shopify.com/) is a comprehensive e-commerce platform that enables individuals and businesses to create online stores.

ingestr supports Shopify as a source for [ingestr assets](https://bruin-data.github.io/bruin/assets/ingestr.html), allowing you to ingest data from Shopify into your data warehouse.

To set up a Shopify connection, you need to have Shopify API key, shopify store URI and source table. For more information, read [here](https://bruin-data.github.io/ingestr/supported-sources/shopify.html)

Follow the steps below to correctly set up shopify as a data source and run ingestion:

**Step 1: Create an Asset File for Data Ingestion**

To ingest data from Shopify, you need to create an [asset configuration file](https://bruin-data.github.io/bruin/assets/ingestr.html#template). This file defines the data flow from the source to the destination.
(For e.g., ingestr.shopify.asset.yml) and add the following content:

***File: ingestr.shopify.asset.yml***
```yaml
name: public.shopify
type: ingestr
connection: postgres

parameters:
  source_connection: my_shopify
  source_table: 'order'
  destination: postgres
```
- name: The name of the asset.
- type: Specifies the type of the asset. It will be always ingestr type for Shopify.
- connection: This is the destination connection.
**parameters:**
- source_connection: The name of the Shopify connection defined in .bruin.yml.
- source_table: The name of the data table in shopify you want to ingest. For example, "order" would ingest data related to order.
  [Available source tables in Shopify](https://bruin-data.github.io/ingestr/supported-sources/shopify.html#available-tables)
Step 2: Add a Connection to [.bruin.yml](https://bruin-data.github.io/bruin/connections/overview.html) that stores connections and secrets to be used in pipelines.
You need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.

***File: .bruin.yml***
```yaml
    connections:
      shopify:
        - name: "my_Shopify"
          api_key: "YOUR_Shopify_API_KEY"
```
**Step 3: [Run](https://bruin-data.github.io/bruin/commands/run.html) Asset to Ingest Data**
```
bruin run ingestr.shopify.asset.yml
```
It will ingest shopify data to postgres.