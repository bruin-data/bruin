# Stripe Billing Analytics to BigQuery

`stripe-bigquery` is a focused Stripe billing analytics pipeline for BigQuery.
It loads the customer, product, price, subscription, subscription-item, and
invoice resources into `stripe_raw`; builds reusable billing models in
`stripe_stage`; and publishes recurring-revenue and invoice-billing reports in
`stripe_reports`.

The template contains 19 assets across three layers. It is a practical starting
point for analyzing Stripe subscription billing while keeping the data model
small enough to adapt to your own reporting needs.

## Project structure

```text
stripe-bigquery/
├── pipeline.yml
├── README.md
├── dashboards/
│   └── stripe-billing-analytics.yml
└── assets/
    ├── stripe_raw/
    │   ├── customer.asset.yml
    │   ├── product.asset.yml
    │   ├── price.asset.yml
    │   ├── subscription.asset.yml
    │   ├── subscription_item.asset.yml
    │   └── invoice.asset.yml
    ├── stripe_stage/
    │   ├── customers.sql
    │   ├── products.sql
    │   ├── prices.sql
    │   ├── subscriptions.sql
    │   ├── subscription_items.sql
    │   ├── invoices.sql
    │   ├── invoice_line_items.sql
    │   ├── subscription_item_daily_snapshot.sql
    │   └── customer_currency_daily_mrr_snapshot.sql
    └── stripe_reports/
        ├── monthly_mrr_by_customer.sql
        ├── monthly_mrr_movements.sql
        ├── monthly_subscription_kpis.sql
        └── monthly_invoice_billings.sql
```

## What it creates

### Raw Stripe data

The `stripe_raw` dataset contains the six Stripe resources that underpin the
billing models. Ingestr loads them with the shared `stripe-default` connection,
and BigQuery is the destination. The source assets use Stripe's `created` field
for incremental discovery and merge the records returned by Stripe.

### Conformed billing models

The `stripe_stage` dataset exposes typed customer, product, price,
subscription, subscription-item, invoice, and invoice-line-item models. It
also records immutable daily snapshots of subscription items and customer MRR
by native currency. Run the pipeline daily after a consistent UTC cutoff so
the snapshots represent a reliable observation history.

### Billing reports

The `stripe_reports` dataset provides four ready-to-query tables:

- `monthly_mrr_by_customer` — month-end observed MRR and annualized run rate by billing customer and currency.
- `monthly_mrr_movements` — customer-level new, reactivation, expansion, contraction, and churn movements.
- `monthly_subscription_kpis` — recurring-revenue, retention, and customer-count metrics by currency.
- `monthly_invoice_billings` — non-draft, non-void invoice billings by finalization month, with a labeled creation-date fallback.

## Example report rows

The following illustrative rows use minor currency units: `9900` USD means
$99.00. Query the four tables in `stripe_reports` directly, then adapt the
columns to the definitions used by your finance and revenue teams.

### `monthly_mrr_by_customer`

| metric_month | as_of_snapshot_date | stripe_customer_id | currency | active_subscription_count | ending_mrr_minor | run_rate_arr_minor |
| --- | --- | --- | --- | ---: | ---: | ---: |
| 2026-01-01 | 2026-01-31 | cus_acme | usd | 1 | 9900 | 118800 |
| 2026-02-01 | 2026-02-28 | cus_acme | usd | 1 | 12900 | 154800 |
| 2026-02-01 | 2026-02-28 | cus_pine | usd | 2 | 4900 | 58800 |

### `monthly_mrr_movements`

| metric_month | stripe_customer_id | currency | beginning_mrr_minor | ending_mrr_minor | expansion_mrr_minor | churned_mrr_minor | movement_type |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| 2026-02-01 | cus_acme | usd | 9900 | 12900 | 3000 | 0 | expansion |
| 2026-02-01 | cus_oak | usd | 2500 | 0 | 0 | 2500 | churn |
| 2026-02-01 | cus_pine | usd | 0 | 4900 | 0 | 0 | new |

### `monthly_subscription_kpis`

| metric_month | currency | ending_mrr_minor | new_mrr_minor | expansion_mrr_minor | churned_mrr_minor | ending_active_customer_count | net_revenue_retention_rate_excluding_reactivation |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-01-01 | usd | 12400 | 12400 | 0 | 0 | 2 | — |
| 2026-02-01 | usd | 17800 | 4900 | 3000 | 2500 | 2 | 1.04 |
| 2026-02-01 | eur | 8100 | 0 | 1100 | 0 | 1 | 1.16 |

### `monthly_invoice_billings`

| invoice_billing_month | invoice_billing_date_basis | currency | issued_invoice_count | subscription_invoice_count | invoiced_billings_minor | invoiced_subscription_billings_minor |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| 2026-01-01 | invoice_finalized_at | usd | 14 | 12 | 139400 | 128700 |
| 2026-02-01 | invoice_finalized_at | usd | 16 | 13 | 171800 | 160200 |
| 2026-02-01 | invoice_created_at_fallback | eur | 2 | 2 | 16200 | 16200 |

## Metric policy

All monetary values remain in each currency's minor units. Do not add amounts
across currencies without an FX source and explicit conversion policy.

MRR is a gross list-price run rate from active and past-due subscriptions with
licensed recurring monthly or annual prices. Free, one-time, metered, and
discounted amounts are excluded. Run-rate ARR is MRR multiplied by 12; neither
measure is recognized revenue, bookings, cash, or a financial statement.

The daily snapshots capture the Stripe state visible when the pipeline runs.
The first run creates a baseline observation and does not reconstruct prior
daily MRR history from current Stripe records.

## Configure connections

Initializing the template adds placeholder connections to the repository-level
`.bruin.yml`. Keep the connection names `gcp-default` and `stripe-default`, or
rename them consistently in both `.bruin.yml` and `pipeline.yml`.

```yaml
default_environment: default

environments:
  default:
    connections:
      google_cloud_platform:
        - name: gcp-default
          project_id: your-gcp-project-id
          service_account_file: /path/to/service-account.json
      stripe:
        - name: stripe-default
          api_key: ${STRIPE_API_KEY}
```

Set the Stripe secret key in your shell before running the pipeline:

```bash
export STRIPE_API_KEY='sk_test_your_secret_key'
```

Use either an `sk_test_...` or `sk_live_...` secret key. A publishable
`pk_...` key cannot read Stripe data. Keep test and live data in separate
BigQuery projects or datasets and do not commit credentials.

## Run the pipeline

Initialize a project from the template:

```bash
bruin init stripe-bigquery my-stripe-pipeline
```

Set `start_date` in `pipeline.yml` to the earliest Stripe history you need,
then run the fast validation checks:

```bash
bruin validate --fast my-stripe-pipeline
```

On a new BigQuery destination, load every table before running query validation:

```bash
bruin run --full-refresh --no-validation my-stripe-pipeline
```

Run this pipeline-wide full refresh only for the first load, or when you
intentionally reset the reporting history. It recreates the daily snapshot
tables, so re-running it removes the observations used by the MRR movement and
retention reports.

After the initial load, run `bruin validate my-stripe-pipeline` and schedule
the pipeline daily. The raw tables hold the Stripe objects returned by
incremental discovery. When you need to reload older mutable Stripe records,
use a targeted reload and keep the snapshot tables intact.

## Customize it

Use the stage models as a stable interface for your own reports. Review the
selected assets, source fields, quality checks, and metric policy before using
the results for financial reporting. Extend the pipeline with your business
definitions, identity mapping, and FX policy where required.

## View the billing dashboard

The included DAC dashboard visualizes native-currency MRR, ARR, customer count,
retention, movement components, invoice billings, and the latest customer MRR
distribution. It deliberately filters to one currency at a time because the
reporting tables retain money in native minor units and do not apply FX rates.

After configuring `gcp-default`, install a verified DAC release by following
the [DAC installation guide](https://getbruin.com/docs/dac/getting-started/installation.html).
Then install its dashboard-authoring skill from the project root:

```bash
dac skills install --dir . create-dashboard
```

Restart your coding agent after installing the skill so it can use the local
`create-dashboard` instructions. Then validate the dashboard, execute its
queries, and serve the DAC app:

```bash
dac --config .bruin.yml validate --dir dashboards
dac --config .bruin.yml check --dir dashboards
dac --config .bruin.yml serve --dir dashboards --port 8321
```

### Dashboard preview

![Stripe billing analytics dashboard preview](/stripe-billing-analytics-demo.png)

This screenshot uses synthetic data solely to demonstrate the dashboard's
layout and charts.

The first snapshot month is a baseline, so MRR movement and retention metrics
may be unavailable until a following contiguous monthly snapshot is present.
