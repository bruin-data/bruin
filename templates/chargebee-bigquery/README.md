# Chargebee to BigQuery

A Bruin pipeline that ingests Chargebee billing data into BigQuery and models it
into analytics-ready staging and reporting layers. It is built for seed to
Series A B2B SaaS teams who have outgrown Chargebee's built-in RevenueStory
reports and want metrics they can define themselves, join to other systems, and
trust.

Two design goals shape it:

1. **Model for reuse.** The staging layer conforms Chargebee into clean tables
   with stable keys, a `source_system` stamp, and an `email_domain`, so it can
   later be unioned and joined with Stripe, a CRM (HubSpot/Salesforce), and ad
   data on a shared customer/account grain.
2. **Report what Chargebee doesn't.** The reporting layer computes the MRR
   movement waterfall, net/gross revenue retention, quick ratio, plan-level MRR,
   revenue concentration, and dunning/involuntary-churn risk — each tunable per
   company through pipeline variables.

## Layers

```
chargebee_raw     ingestr-loaded source tables (customers, subscriptions, …)
   → chargebee_stage    conformed, typed, de-duplicated models + daily MRR snapshot
      → chargebee_reports   monthly KPIs, MRR movements, plan/concentration/dunning
```

Amounts stay in native-currency **minor units** (cents) as `NUMERIC` all the way
through, with a `_minor` suffix; every metric is reported **per currency** and is
never FX-converted, so you never accidentally sum across currencies. Convert to
major units (divide by 100, or 1 for zero-decimal currencies like JPY) at the BI
layer.

### Raw (`chargebee_raw`)

`customer`, `subscription`, `invoice`, `transaction`, `event` — loaded
incrementally by ingestr (merge on `id`, keyed on `updated_at`, and `occurred_at`
for events).

### Staging (`chargebee_stage`)

| Model | Purpose |
| ----- | ------- |
| `customers` | Conformed customers; epoch→TIMESTAMP, `email_domain`, `source_system`, acquisition channel. The customer/account join grain. |
| `subscriptions` | Conformed subscriptions; `is_mrr_eligible` flag driven by the `mrr_active_statuses` variable. |
| `subscription_items` | Subscription line items unnested from the `subscription_items` array, with each recurring item normalized to a monthly run rate. |
| `invoices` | Invoice headers; money as `_minor` NUMERIC; recurring vs one-off. |
| `transactions` | Payments/refunds with success and failed-payment flags. |
| `customer_currency_daily_mrr_snapshot` | Daily MRR observation per customer × currency. History spine for every monthly report (see below). |

### Reports (`chargebee_reports`)

| Report | Insight |
| ------ | ------- |
| `monthly_mrr_by_customer` | Month-end MRR per customer × currency, with segment fields for CRM joins. |
| `monthly_mrr_movements` | Per-customer new / expansion / contraction / churn / reactivation ledger that rolls forward exactly to ending MRR. |
| `monthly_subscription_kpis` | Company scorecard: MRR, ARR, the movement waterfall, GRR, NRR, logo churn, ARPA, and SaaS quick ratio. |
| `monthly_invoice_billings` | Billed vs collected revenue, recurring vs one-off — distinct from MRR. |
| `mrr_by_plan` | Current MRR by plan/add-on item price; product-level concentration Chargebee doesn't expose. |
| `revenue_concentration` | Top-N customers by MRR with top-1/5/N shares — customer-concentration risk. |
| `failed_payment_dunning` | Payment success rate and recoverable vs at-risk failed amounts (involuntary-churn exposure). |

## How MRR history works

Chargebee subscriptions are mutable — a repriced or cancelled subscription only
ever shows its *current* state, so past MRR cannot be read back from the API. The
`customer_currency_daily_mrr_snapshot` model solves this: on every run it stamps
the pipeline end date and records that day's MRR, replacing only that day's
partition (idempotent re-runs). Historical subscription episodes can also be
backfilled from their `started_at`/`cancelled_at` dates and normalized item MRR;
repricing history still requires snapshots taken while each price was current.
The movement/retention reports need two contiguous monthly snapshots before
they classify anything. Run the pipeline on its daily schedule and the history
builds itself.

## Customization (pipeline variables)

Set in `pipeline.yml`, overridable per run with `--var`:

| Variable | Default | Effect |
| -------- | ------- | ------ |
| `mrr_active_statuses` | `["active","non_renewing"]` | Which subscription statuses count toward MRR. Add `in_trial` to count trials. |
| `concentration_top_n` | `10` | How many top customers `revenue_concentration` ranks (top-1/5 always emitted). |
| `dunning_retry_window_days` | `30` | How long a failed payment is treated as recoverable before it becomes at-risk. |

```bash
bruin run --var concentration_top_n=5 --var 'mrr_active_statuses=["active","non_renewing","in_trial"]' .
```

## Dashboard

`dashboards/chargebee-billing-analytics.yml` is a ready-made DAC dashboard over
the reporting layer. It is one dashboard with four tabs — Overview, Retention &
Expansion, Monetization & Portfolio, and Collections & Risk — covering ending
MRR / ARR, retention, signed MRR movements, customer mix, plan packaging, invoice
collections, and account follow-up. All tabs are filterable by currency and date
range. The Overview tab pairs the ending-MRR trend with net MRR change, gross
MRR added/lost, and a movement-driver table that maps each driver to a suggested
action. The Retention tab uses a two-series signed flow chart (MRR added vs MRR lost)
and account-level movement details; the portfolio pie is customer count by MRR band,
while plan-level revenue is kept in the table. Preview it locally:

```bash
dac serve --dir . --open
```

## Combining with other sources

`chargebee_stage.customers` is the join surface: it carries `source_system`,
`company_name`, and a lower-cased `email_domain`. To build a conformed account
across systems, union it with an equivalent Stripe/CRM staging model and resolve
identity with, in order of reliability: an explicit CRM foreign key → billing
email → normalized company domain. CAC, LTV:CAC, and CAC-payback reports then sit
on top of that account crosswalk once CRM and ad-spend sources are flowing.

## Setup

1. Set the connection values in `.bruin.yml`, directly or via environment
   variables:
   - `CHARGEBEE_SITE` — your Chargebee **site name only**, i.e. the `xxx` in
     `https://xxx.chargebee.com` (do **not** include `.chargebee.com`).
   - `CHARGEBEE_API_KEY` — a Chargebee API key.
   - `gcp-default` — your BigQuery project id and region.
2. `bruin validate .`

## Running

### First run: full backfill / full refresh

Load all history and (re)create every table:

```bash
bruin run --full-refresh .
```

To backfill an explicit window, add a date range:

```bash
bruin run --full-refresh --start-date 2023-01-01 --end-date 2024-01-01 .
```

### Subsequent runs: incremental

```bash
bruin run .
```

Raw tables merge only changed records; the daily MRR snapshot appends the day;
reports rebuild from the snapshot. This is what the `daily` schedule does
automatically.

## Seeding a realistic demo site

For a Chargebee **test** site, the repository includes a stdlib-only seeder that
creates 500 fake B2B SaaS customers, monthly plan prices from 100 to 1000 in
each selected native currency, and historical subscription episodes across six
months. The generated mix is approximately 62% active, 12% non-renewing, 16%
cancelled, and 10% reactivated customers. It uses `example.com` addresses,
offline collection, deterministic IDs, and Chargebee idempotency keys.

Chargebee must have Product Catalog 2.0, the requested currencies enabled, and
the subscription import operation available on the test site. The import API is
used because normal subscription creation only backdates within site-configured
limits; the import operation accepts historical lifecycle timestamps.

From the repository root:

```bash
export CHARGEBEE_SITE=your-site-test
export CHARGEBEE_API_KEY=test_xxx
python3 scripts/seed_chargebee_demo.py --dry-run
python3 scripts/seed_chargebee_demo.py --apply --yes
```

The default run creates one current-term invoice for active and non-renewing
subscriptions without attempting payment. To omit those invoices, add
`--no-invoices`. To use a different repeatable batch, change `--run-id`; reuse
the same run ID to safely retry a partially completed run.

The script refuses currencies that are not active in Chargebee. Enable
multi-currency and add `USD`, `EUR`, and `GBP` in the test site first, or pass a
different active set with `--currencies USD,EUR`. In a disposable test site,
`--add-missing-currencies` can add the supported demo currencies with fixed
exchange rates before seeding:

```bash
python3 scripts/seed_chargebee_demo.py --apply --yes --add-missing-currencies
```

## Extending

- Unnest `invoice.line_items` (service periods `date_from`/`date_to`) to
  reconstruct historical MRR from the immutable invoice ledger.
- Add cohort retention and a bottoms-up ARR forecast on `monthly_mrr_movements`.
- Wire the CRM/ads account crosswalk to unlock CAC, LTV:CAC, and payback.
