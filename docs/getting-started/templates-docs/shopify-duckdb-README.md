# Bruin - Shopify to DuckDB Template

This pipeline is a simple example of a Bruin pipeline that copies data from Shopify to DuckDB. It demonstrates how to use the `bruin` CLI to build and run a pipeline.

The pipeline includes two sample assets already:
- `raw.shopify`: A simple ingestr asset that copies a table from Shopify to DuckDB

## Setup
The pipeline already includes an empty `.bruin.yml` file, fill it with your connections and environments. You can read more about connections [here](https://bruin-data.github.io/bruin/connections/overview.html).

Here's a sample `.bruin.yml` file:

```yaml
default_environment: default
environments:
    default:
        connections:
            duckdb:
                - name: "duckdb-default"
                  path: "<path to database>"

            shopify:
                - name: "my-shopify-connection"
                  api_key: "********"
                  url: "******.myshopify.com"
```
## Running the pipeline
bruin CLI can run the whole pipeline or any task with the downstreams:
```shell
bruin run assets/shopify.asset.yml
```

```shell
❯ bruin run ./templates/shopify-duckdb/                                                       (bruin) 
Analyzed the pipeline 'bruin-init' with 1 assets.

Pipeline: bruin-init (.)
  No issues found

✓ Successfully validated 1 assets across 1 pipeline, all good.

Starting the pipeline execution...

Executed 1 tasks in 9.656s
```

You can also run a single task:

```shell
bruin run assets/hello.py                            
```

```shell
❯ bruin run ./templates/shopify-duckdb/                                                       (bruin) 
Analyzed the pipeline 'bruin-init' with 1 assets.

Pipeline: bruin-init (.)
  No issues found

✓ Successfully validated 1 assets across 1 pipeline, all good.

Starting the pipeline execution...


Executed 1 tasks in 9.656s
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, good luck!


NOTE: Please find assets example for balance and transactions table in the [Shopify Cookbook](https://bruin-data.github.io/docs/cookbook/shopify-pipelines.html)

- Balance Table
```yaml
name: shopify_raw.balance
type: ingestr

description: This asset manages the ingestion of Shopify customer data into BigQuery. It captures comprehensive customer information including personal details, contact information, order history, marketing preferences, and address data. The asset includes data quality checks to ensure critical fields like customer ID and email are properly populated.

parameters:
  source_connection: shopify-default
  source_table: balance
  destination: duckdb

```

- Transactions Table
```yaml
name: shopify_raw.transactions
type: ingestr

description: This asset manages the ingestion of Shopify transaction data into BigQuery. It captures detailed payment transaction information including transaction ID, order ID, amount, currency, payment method, status, and processed date. This data is essential for financial reporting, reconciliation, and analyzing payment patterns across the Shopify store.

parameters:
  source_connection: shopify-default
  source_table: transactions
  destination: duckdb

```