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

## Configuration

### Stripe API Key Setup

Before running the pipeline, you need to configure your Stripe API credentials. **Important: You must use a Secret API key, not a Publishable API key.** Using a publishable key will result in errors like:

```
PermissionError: This API call cannot be made with a publishable API key. Please use a secret API key.
```

#### Getting Your Stripe Secret API Key

1. Log in to your [Stripe Dashboard](https://dashboard.stripe.com/)
2. Click ⚙️ **Settings** in the top-right corner
3. Navigate to **Developers** from the top menu
4. Click **"Manage API Keys"**
5. In the **"Standard Keys"** section, click **"Reveal test key"** (for test mode) or **"Reveal live key"** (for production) beside the **Secret Key**
6. Copy the secret API key (it starts with `sk_test_` for test mode or `sk_live_` for production)

> **Note:** The Stripe UI may change over time. For the most up-to-date instructions, refer to the [dlt Stripe documentation](https://dlthub.com/docs/dlt-ecosystem/verified-sources/stripe#grab-credentials). Since Bruin uses dlt's Stripe ingestion under the hood, the credential setup process is the same.

#### Configuring the Connection in Bruin

Add your Stripe connection to the `.bruin.yml` file in your project root:

```yaml
default_environment: default
environments:
  default:
    connections:
      stripe:
        - name: 'stripe-default'
          api_key: 'sk_test_YOUR_SECRET_KEY_HERE'  # Use your secret API key
```

The connection name `stripe-default` matches the `source_connection` specified in `pipeline.yml`. If you use a different connection name, make sure to update `pipeline.yml` accordingly.

For more information on managing connections in Bruin, see the [Bruin connections documentation](https://getbruin.com/docs/bruin/commands/connections) and [Stripe credentials guide](https://getbruin.com/docs/bruin/ingestion/stripe.html#step-1-add-a-connection-to-bruin-yml-file).

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
