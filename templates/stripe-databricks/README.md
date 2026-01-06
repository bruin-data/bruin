# Bruin - Stripe to Databricks

This pipeline demonstrates a complete bronze-to-silver ingestion workflow using Stripe as a data source and Databricks as the destination. It presents a canonical ELT pattern: raw collection via `ingestr` followed by curated transformations in SQL.

## Included Assets

### Bronze Layer (Raw Ingestion)
- `bronze_customer_data_raw` - Ingests customer data from Stripe
- `bronze_subscription_data_raw` - Ingests subscription data from Stripe
- `bronze_charge_data_raw` - Ingests charge/payment data from Stripe
- `bronze_balance_transaction_data_raw` - Ingests balance transaction data from Stripe

### Silver Layer (Transformed)
- `silver_customer_subscription_simple` - Joins customers with their subscriptions and most recent balance transaction

## Data Model

Balance transactions in Stripe don't have a direct `customer` field. To link them to customers, we join through charges:

```
customer ← charge (via charge.customer) → balance_transaction (via charge.balance_transaction)
```

## Running the Pipeline

Initialize a new project from this template:

```bash
bruin init stripe-databricks my-stripe-pipeline
```

Run the pipeline for the first time using the `full-refresh` flag. The pipeline will start processing data, starting from the `start_date` specified in `pipeline.yml`:

```bash
bruin run --full-refresh my-stripe-pipeline
```

Alternatively, specify the date range in the `run` command by using the `start-date` and `end-date` flags:

```bash
bruin run --start-date 2025-01-01 --end-date 2025-01-30 my-stripe-pipeline
```

Run a single asset:

```bash
bruin run assets/raw/bronze_customer_data_raw.asset.yml
```

## Pipeline Flow

```
bronze_customer_data_raw ─────────────┐
                                      │
bronze_subscription_data_raw ─────────┼──► silver_customer_subscription_simple
                                      │
bronze_charge_data_raw ───────────────┤
                                      │
bronze_balance_transaction_data_raw ──┘
```

The pipeline will:
1. Ingest customers, subscriptions, charges, and balance transactions from Stripe into Databricks bronze tables
2. Build `silver_customer_subscription_simple`, joining customers with their subscriptions and most recent balance transaction (linked through charges)

That's it, good luck!
