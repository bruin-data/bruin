# Shopify to ClickHouse

This template builds a Shopify analytics pipeline with Bruin, ingestr, and ClickHouse. It ingests Shopify customers, products, orders, inventory items, discounts, and events; then creates conformed models and reporting marts you can adapt for your store.

The template uses the `shopify` ClickHouse database and the connection names `shopify-default` and `clickhouse-default`. Rename the database or connections consistently if they do not fit your project.

## Project structure

```text
shopify-clickhouse/
├── pipeline.yml
├── README.md
└── assets/
    ├── t1/                         # Raw Shopify ingestion assets
    │   ├── t1_customers.asset.yml
    │   ├── t1_products.asset.yml
    │   ├── t1_orders.asset.yml
    │   ├── t1_inventory_items.asset.yml
    │   ├── t1_discounts.asset.yml
    │   └── t1_events.asset.yml
    ├── t2/                         # Conformed commerce models
    │   ├── t2_customers.sql
    │   ├── t2_products.sql
    │   ├── t2_inventory_items.sql
    │   ├── t2_orders.sql
    │   └── t2_order_line_items.sql
    └── t3/                         # Analytics marts
        ├── t3_daily_revenue.sql
        ├── t3_daily_kpis.sql
        ├── t3_payment_reconciliation.sql
        ├── t3_customer_cohorts.sql
        └── t3_product_performance.sql
```

## What it creates

The pipeline is organized into three layers:

- `t1`: raw Shopify API data, incrementally merged into ClickHouse.
- `t2`: conformed customers, products, inventory items, orders, and order line items.
- `t3`: daily revenue and KPI marts, payment-status reconciliation, customer cohorts, and product performance.

All timestamps and reporting dates use UTC. Monetary metrics remain at Shopify currency grain: the template deliberately does not convert or combine currencies.

## Before you begin

1. Create a Shopify custom app, install it in your store, and give it the Admin API scopes required for the resources you want to ingest. See the [Shopify ingestion guide](https://getbruin.com/docs/ingestr/supported-sources/shopify.html) for the underlying connector requirements.
2. Create a `shopify` database in ClickHouse, or replace every `shopify.` asset name with your preferred database name.
3. Add the Shopify and ClickHouse connections to your project-root `.bruin.yml`. Do not commit credentials.

For example:

```yaml
default_environment: default
environments:
  default:
    connections:
      shopify:
        - name: shopify-default
          url: your-store.myshopify.com
          api_key: <shopify-admin-api-access-token>
      clickhouse:
        - name: clickhouse-default
          host: <clickhouse-host>
          port: 8443
          username: <clickhouse-username>
          password: <clickhouse-password>
          database: shopify
          secure: 1
```

`secure: 1` is appropriate for ClickHouse Cloud and other TLS-enabled deployments. Adjust the connection values for your ClickHouse installation.

Create the database before the first run:

```sql
CREATE DATABASE IF NOT EXISTS shopify;
```

## Run the pipeline

Initialize the template:

```bash
bruin init shopify-clickhouse
cd shopify-clickhouse
```

Set `start_date` in `pipeline.yml` to the earliest Shopify history you need, then validate the generated pipeline from the repository root:

```bash
bruin validate shopify-clickhouse
```

For the first load, run a full refresh over the same historical period. Replace the dates with your desired range:

```bash
bruin run shopify-clickhouse \
  --full-refresh \
  --start-date "2020-01-01 00:00:00" \
  --end-date "YYYY-MM-DD 23:59:59.999999"
```

After the initial load, run the pipeline on its daily schedule or supply a bounded interval to safely reprocess changed records:

```bash
bruin run shopify-clickhouse \
  --start-date "YYYY-MM-DD 00:00:00" \
  --end-date "YYYY-MM-DD 23:59:59.999999"
```

## Customize it

The T1 assets preserve Shopify structures that are useful as a starting point, including nested JSON for orders and products. T2 models expose analytics-friendly fields while omitting direct customer identifiers and street addresses. Review the selected API resources, column definitions, quality checks, and business rules before treating this as a production model.

In particular, the payment reconciliation mart summarizes Shopify order financial statuses; it is not a payout or gateway-settlement ledger. Add Shopify Payments or another payment-source integration if you need settlement-level reporting.

The template uses `merge` materializations for incrementally updated entities and daily marts. Order lines use `delete+insert` keyed by order so removed lines do not remain after an order edit. The customer-cohort and product-performance marts use `create+replace`, because they summarize the full available history.

## Explore the marts

After a successful run, start with the daily KPIs:

```sql
SELECT *
FROM shopify.t3_daily_kpis
ORDER BY metric_date DESC, currency
LIMIT 10;
```

The marts are designed as a useful baseline, not a universal ecommerce semantic model. Extend them with your store's cancellation, fulfillment, tax, returns, and currency policies.
