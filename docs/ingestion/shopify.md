# Shopify
[Shopify](https://www.Shopify.com/) is a comprehensive e-commerce platform that enables individuals and businesses to create online stores.

Bruin supports Shopify as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Shopify into your data warehouse.

In order to set up Shopify connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You need the `url` of your Shopify store and the `api_key`. For details on how to obtain these credentials, please refer [here](https://dlthub.com/docs/dlt-ecosystem/verified-sources/shopify#setup-guide).

Follow the steps below to correctly set up Shopify as a data source and run ingestion:

### Step 1: Add a connection to .bruin.yml file
To ingest data from Shopify, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., shopify_ingestion.yml) inside the assets folder and add the following content:

```yaml
   connections:
    shopify:
        - name: my-shopify
          url: test.myshopify.com
          api_key: abckey
```
- `api_key`: the API key used for authentication with Shopify

### Step 2: Create an asset file for data ingestion

```yaml
name: public.shopify
type: ingestr
connection: postgres

parameters:
  source_connection: my-shopify
  source_table: 'orders'

  destination: postgres
```
- `name`: The name of the asset.
- `type`: Specifies the type of the asset. It will be always ingestr type for Shopify.
- `connection`: This is the destination connection.
- `source_connection`: The name of the Shopify connection defined in .bruin.yml.
- `source_table`: The name of the data table in Shopify you want to ingest. Available tables:

Table    PK    Inc Key    Inc Strategy    Details
orders    id    updated_at    merge    Retrieves Shopify order data including customer info, line items, and shipping details
customers    id    updated_at    merge    Retrieves Shopify customer data including contact info and order history
discounts    id    updated_at    merge    Retrieves Shopify discount data using GraphQL API (use instead of deprecated price_rules)
products    id    updated_at    merge    Retrieves Shopify product information including variants, images, and inventory
inventory_items    id    updated_at    merge    Retrieves Shopify inventory item details and stock levels
transactions    id    id    merge    Retrieves Shopify transaction data for payments and refunds
balance    currency    -    merge    Retrieves Shopify balance information for financial tracking
events    id    created_at    merge    Retrieves Shopify event data for audit trails and activity tracking
price_rules    id    updated_at    merge    DEPRECATED - Use discounts table instead

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/shopify_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Shopify table into your Postgres database.

<img width="1217" alt="shopify" src="https://github.com/user-attachments/assets/0fe4b3e9-e9b8-4967-b892-4dc539683155">
