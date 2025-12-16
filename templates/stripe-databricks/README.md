# Bruin - Stripe to Databricks

This pipeline demonstrates a complete bronze-to-silver ingestion workflow using Stripe as a data source and Databricks as the destination. It presents a canonical ELT pattern: raw collection via `ingestr` followed by curated transformations in SQL.

## Included Assets

### Bronze Layer (Raw Ingestion)
- `customers_raw` - Ingests customer data from Stripe
- `subscriptions_raw` - Ingests subscription data from Stripe
- `charges_raw` - Ingests charge/payment data from Stripe

### Silver Layer (Transformed)
- `silver_customer_subscription_simple.sql` - Joins customers with their subscriptions and most recent charge, providing a clean, analysis-ready view of customer activity

## Running the Pipeline

Initialize a new project from this template:

```bash
bruin init stripe-databricks my-stripe-pipeline
cd my-stripe-pipeline
```

Run the entire pipeline:

```bash
bruin run
```

Run a single asset:

```bash
bruin run assets/bronze_customer_data_raw.asset.yml
```

## Pipeline Flow

```
customers_raw ──────┐
                    │
subscriptions_raw ──┼──► silver_customer_subscription_simple
                    │
charges_raw ────────┘
```

The pipeline will:
1. Ingest customers, subscriptions, and charges from Stripe into Databricks bronze tables
2. Build `silver_customer_subscription_simple`, joining customers with their subscriptions and most recent charge

That's it, good luck!

