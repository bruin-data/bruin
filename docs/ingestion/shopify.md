# Shopify

[Shopify](https://www.Shopify.com/) is a comprehensive e-commerce platform that enables individuals and businesses to create online stores.

Bruin supports Shopify as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Shopify into your data warehouse.

To set up a Shopify connection, you need your store URL and Shopify Admin API access token. For the underlying connector reference, see the [ingestr Shopify documentation](https://getbruin.com/docs/ingestr/supported-sources/shopify.html).

Follow the steps below to correctly set up Shopify as a data source and run ingestion:

## Setting up a Shopify Integration

To use the Shopify API, create a custom app in the Shopify Dev Dashboard and install it in your store.

### Step 1: Create or Select an App

1. Go to the [Shopify Dev Dashboard](https://dev.shopify.com/dashboard)
2. Select an existing app or create a new one

### Step 2: Configure API Scopes

In the app configuration, make sure the app has read scopes for the data you want to ingest:

- `read_products`
- `read_customers`
- `read_orders`
- `read_inventory`
- `read_locations`

These are examples, not an exhaustive list. Add every Admin API access scope required by the source tables you select, including the relevant permissions for discounts, events, transactions, balance, and price rules.

After changing scopes:

1. Create a new app version
2. Release the new app version

### Step 3: Install the App in Your Store

1. Open the Shopify store admin: `https://admin.shopify.com/store/your-store-name`
2. Go to **Settings** → **Apps and sales channels**
3. Find and open your app
4. Install or reinstall the app so the new scopes become active

### Step 4: Get the Admin API Access Token

1. After installation, go back to **Apps and sales channels**
2. Click on your app
3. Click **API credentials**
4. Copy the **Admin API access token**

> **Important**: The access token is displayed only once. Copy and store it securely.

Use your store name, for example `my-store.myshopify.com`, as `url`, and use the Admin API access token as `api_key` in `.bruin.yml`. The `api_key` parameter is named for compatibility; its value is not the app's API key.

## Configuration

### Step 1: Add a connection to .bruin.yml file

To ingest data from Shopify, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., shopify_ingestion.yml) inside the assets folder and add the following content:

```yaml
connections:
    shopify:
      - name: my-shopify
        url: my-store.myshopify.com
        api_key: your_admin_api_access_token
```

- `url`: the Shopify store domain, such as `my-store.myshopify.com`.
- `api_key`: the **Admin API access token** from your Shopify app. The parameter is named `api_key` for compatibility, but its value is not the app's API key.

For this token-based connection, the store domain and its Admin API access token are the only required values. You do not need to provide the app's `client_id` or `client_secret`.

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
- `source_table`: The name of the data table in Shopify you want to ingest.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------|---------|
| `orders` | id | updated_at | merge | Retrieves Shopify order data including customer info, line items, and shipping details |
| `customers` | id | updated_at | merge | Retrieves Shopify customer data including contact info and order history |
| `discounts` | id | updated_at | merge | Retrieves Shopify discount data using GraphQL API (use instead of deprecated price_rules) |
| `products` | id | updated_at | merge | Retrieves Shopify product information including variants, images, and inventory |
| `inventory_items` | id | updated_at | merge | Retrieves Shopify inventory item details and stock levels |
| `transactions` | id | id | merge | Retrieves Shopify transaction data for payments and refunds |
| `balance` | currency | - | merge | Retrieves Shopify balance information for financial tracking |
| `events` | id | created_at | merge | Retrieves Shopify event data for audit trails and activity tracking |
| `price_rules` | id | updated_at | merge | **DEPRECATED** - Use `discounts` table instead |

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/shopify_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given Shopify table into your Postgres database.

<img width="1217" alt="shopify" src="https://github.com/user-attachments/assets/0fe4b3e9-e9b8-4967-b892-4dc539683155">
