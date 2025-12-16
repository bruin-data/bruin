# Bruin - Stripe to Databricks

This pipeline demonstrates a complete bronze-to-silver ingestion workflow using Stripe as a data source and Databricks as the destination. It presents a canonical ELT pattern: raw collection via `ingestr` followed by curated transformations in SQL.

## Included Assets

### Bronze Layer (Raw Ingestion)
- `customers_raw` - Ingests customer data from Stripe
- `subscriptions_raw` - Ingests subscription data from Stripe
- `charges_raw` - Ingests charge/payment data from Stripe

### Silver Layer (Transformed)
- `silver_customer_subscription_simple.sql` - Joins customers with their subscriptions and most recent charge, providing a clean, analysis-ready view of customer activity

## Setup

The pipeline includes a `.bruin.yml` file with placeholder connection values:

```yaml
default_environment: default
environments:
  default:
    connections:
      stripe:
        - name: "stripe-default"
          api_key: "sk_test_your_stripe_secret_key"

      databricks:
        - name: "databricks-default"
          host: "your-workspace.cloud.databricks.com"
          token: "your-databricks-token"
          path: "/sql/1.0/warehouses/your-warehouse-id"
          port: 443
          catalog: "your_catalog"
          schema: "stripe_sandbox"
```

Update the connections with your Stripe API key and Databricks credentials. You can read more about connections [here](https://bruin-data.github.io/bruin/connections/overview.html).

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

