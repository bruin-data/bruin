# Adapty

[Adapty](https://adapty.io/) is a subscription monetization platform for mobile and web apps. It exposes a batch Analytics Export API and a paginated paywall list through its Server-side API.

Bruin supports Adapty as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Adapty into your data warehouse.

In order to set up an Adapty connection, you need to add a configuration item in the `.bruin.yml` file and in the `asset` file. You need the secret `api_key` from **App Settings → General → API keys**. Use the secret API key, not a public SDK key.

Follow the steps below to correctly set up Adapty as a data source and run ingestion.

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to Adapty, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      adapty:
        - name: "my_adapty"
          api_key: "secret_live_..."
          lookback_days: 30
          timezone: "UTC"
```

- `api_key`: Required. The app-specific secret API key from Adapty.
- `lookback_days`: Optional. Number of days loaded when no analytics interval is supplied. Defaults to `30`; use `0` to load only today.
- `timezone`: Optional. IANA timezone used by Adapty to group analytics dates. Defaults to `UTC`.

### Step 2: Create an asset file for data ingestion

To ingest data from Adapty, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., adapty_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.adapty
type: ingestr
connection: postgres

parameters:
  source_connection: my_adapty
  source_table: 'analytics?chart_id=revenue'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Adapty connection defined in .bruin.yml.
- `source_table`: The name of the data table in Adapty that you want to ingest. See the tables below.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/adapty_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given Adapty table into your Postgres database.

## Available Source Tables

| Table | Required parameters | PK | Inc Strategy | Details |
| ----- | ------------------- | -- | ------------ | ------- |
| `analytics` | `chart_id` | – | delete+insert on `date` | Revenue, MRR, ARR, ARPU/ARPPU, subscriptions, trials, refunds, billing issues, installs, and related chart metrics. |
| `cohorts` | – | – | delete+insert on `date` | Cohort revenue, subscriber, subscription, ARPU, ARPPU, and ARPAS analytics. |
| `conversion` | `from_period`, `to_period` | – | delete+insert on `date` | Conversion between two subscription states. Use `from_period=null` when there is no starting state. |
| `funnel` | – | – | delete+insert on `date` | Subscription funnel and churn analytics. |
| `ltv` | – | – | delete+insert on `date` | Actual lifetime value for revenue, proceeds, and net revenue. |
| `retention` | – | – | delete+insert on `date` | Subscriber retention analytics. |
| `placements` | `placement_type` | – | replace | Exported paywall or onboarding placement configuration. |
| `paywalls` | – | `paywall_id` | merge (inc key `updated_at`) | All paginated paywalls, including their state, deletion marker, and nested products. |

The six analytics tables are requested one calendar day at a time and receive an ingestr-managed `date` column. When no interval is supplied, they load the last `lookback_days` days by default.

The paywall API does not accept an `updated_at` filter. ingestr scans its paginated list and applies the incremental interval client-side. Adapty returns archived and deleted paywalls with `state` and `is_deleted`, allowing merge loads to retain lifecycle changes.

Provider response objects and arrays, such as cohort values and paywall products, remain nested rather than being flattened.

## Analytics table parameters

Parameters are URL-style query parameters on the source table. All six analytics tables accept these optional filters:

- `compare_date`: Exactly two `YYYY-MM-DD` dates.
- `store`, `country`, `store_product_id`, `duration`
- `attribution_source`, `attribution_status`, `attribution_channel`, `attribution_campaign`, `attribution_adgroup`, `attribution_adset`, `attribution_creative`
- `offer_category`, `offer_type`, `offer_id`

List values are comma-separated. Each table also accepts its endpoint-specific parameters:

| Table | Optional parameters |
| ----- | ------------------- |
| `analytics` | `date_type`, `segmentation` |
| `cohorts` | `period_type`, `value_type`, `value_field`, `accounting_type`, `renewal_days`, `prediction_months` |
| `conversion` | `date_type`, `segmentation` |
| `funnel` | `show_value_as`, `segmentation` |
| `ltv` | `period_type`, `segmentation` |
| `retention` | `segmentation`, `use_trial` |

Valid `chart_id` values are: `revenue`, `mrr`, `arr`, `arppu`, `subscriptions_active`, `subscriptions_new`, `subscriptions_renewal_cancelled`, `subscriptions_expired`, `trials_active`, `trials_new`, `trials_renewal_cancelled`, `trials_expired`, `grace_period`, `billing_issue`, `refund_events`, `refund_money`, `non_subscriptions`, `arpu`, and `installs`.

## Examples

Load daily purchase revenue:

```yaml
parameters:
  source_connection: my_adapty
  source_table: 'analytics?chart_id=revenue'
```

Load refund totals for selected stores and countries:

```yaml
parameters:
  source_connection: my_adapty
  source_table: 'analytics?chart_id=refund_money&store=app_store,play_store&country=us,gb'
```

Load cohort revenue by days:

```yaml
parameters:
  source_connection: my_adapty
  source_table: 'cohorts?period_type=days&value_field=revenue&accounting_type=net_revenue&renewal_days=0,1,3,7,14,30'
```

Load paywall placement configuration:

```yaml
parameters:
  source_connection: my_adapty
  source_table: 'placements?placement_type=paywall'
```

Load all paywall definitions:

```yaml
parameters:
  source_connection: my_adapty
  source_table: 'paywalls'
```
