# Shopify

Shopify is an e-commerce platform that allows businesses to create and manage online stores. Bruin supports connecting to Shopify's Admin API to access store data.

## Connection

```yaml
connections:
    shopify:
        - name: "shopify-default"
          api_key: "********"
          url: "<YOUR STORE URL>"
```

The following fields are required:
- `url`: Your Shopify store's URL
- `api_key`: A private app access token or admin API access token

## Assets

Bruin provides out-of-the-box support for Shopify through `ingestr` type assets. The `ingestr` [integration](https://bruin-data.github.io/ingestr/supported-sources/shopify.html) allows you to easily import data from your Shopify store. Below is an example of a Shopify ingestr asset configuration:

```yaml
name: shopify_raw.discounts
type: ingestr

description: This asset manages the ingestion of Shopify discount data into BigQuery. It captures comprehensive discount information including discount IDs, discount rules, conditions, and associated metadata. The asset includes data quality checks to ensure critical fields like discount ID are properly populated and valid.
  
columns:
  - name: id
    type: integer
    description: "Unique identifier for the discount"
    primary_key: true
    checks:
        - name: not_null
  - name: discount
    type: json
    description: "Complete discount information including rules and conditions"
  - name: metafields_first250
    type: json
    description: "First 250 metafields associated with the discount"
    
parameters:
  source_connection: shopify-default
  source_table: discounts
  destination: duckdb
```