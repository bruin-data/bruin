# Bruin - Ecommerce Template

The **ecommerce** template is an interactive template that scaffolds a complete ecommerce analytics pipeline. It sets up raw ingestion from your selected sources, a staging layer that cleans and joins them, and a set of reports for revenue, customers, products, marketing, and KPIs.

## Usage

```bash
bruin init ecommerce
```

The wizard will prompt you to pick your tech stack:

- **Data Warehouse**: ClickHouse, BigQuery, or Snowflake
- **Payments**: Shopify Payments or Stripe
- **Email Marketing**: Klaviyo or HubSpot
- **Advertising**: Facebook Ads, Google Ads, TikTok Ads (multi-select)
- **Web Analytics**: GA4 or Mixpanel

Only the assets matching your selections are generated, so the resulting pipeline contains just what your stack needs.

## What's included

### Raw ingestion

Shopify is always included. The remaining ingestion assets are added based on your wizard selections.

- **Shopify** (always): `shopify_orders`, `shopify_customers`, `shopify_products`, `shopify_inventory`
- **Stripe payments**: `stripe_charges`, `stripe_refunds`, `stripe_customers`, `stripe_payouts`
- **Klaviyo**: `klaviyo_campaigns`, `klaviyo_flows`, `klaviyo_metrics`
- **HubSpot**: `hubspot_contacts`, `hubspot_deals`, `hubspot_campaigns`
- **Facebook Ads**: `facebook_campaigns`, `facebook_ad_insights`
- **Google Ads**: `google_campaigns`, `google_ad_insights`
- **TikTok Ads**: `tiktok_campaigns`, `tiktok_ad_insights`
- **GA4**: `ga4_events`, `ga4_sessions`
- **Mixpanel**: `mixpanel_events`, `mixpanel_funnels`

### Staging layer

Cleaned and joined models across the raw sources:

- `stg_orders`: unified orders with payment status from Shopify and Stripe
- `stg_customers`: customer profile combined with marketing engagement
- `stg_products`: product catalog with inventory
- `stg_marketing_spend`: ad spend rolled up across Facebook, Google, and TikTok
- `stg_web_sessions`: session and event data from GA4 or Mixpanel

### Reports

- `rpt_daily_revenue`: daily gross and net revenue trends
- `rpt_customer_cohorts`: cohort retention and lifetime value
- `rpt_product_performance`: product-level sales, returns, and margin
- `rpt_marketing_roi`: spend, attributed revenue, and ROI per channel
- `rpt_daily_kpis`: top-level KPIs (revenue, orders, AOV, sessions, conversion)

## Folder structure

```plaintext
ecommerce/
├── assets/
│   ├── raw/         # ingestr assets for selected sources
│   ├── staging/     # cleaned, joined models
│   └── reports/     # business-facing reports
├── pipeline.yml
└── README.md
```

## Setup

After running `bruin init ecommerce`, fill in your connections in `.bruin.yml`. You'll need a connection for your chosen warehouse and one for each ingestion source you selected. See the [connections docs](https://getbruin.com/docs/bruin/commands/connections.html) for the full list of supported credential formats.

Example `.bruin.yml` for a Shopify → BigQuery setup with Stripe and Klaviyo:

```yaml
default_environment: default
environments:
    default:
        connections:
            google_cloud_platform:
                - name: "gcp"
                  service_account_file: "<path to service account file>"
                  project_id: "your-project-id"

            shopify:
                - name: "shopify-default"
                  api_key: "********"
                  url: "******.myshopify.com"

            stripe:
                - name: "stripe-default"
                  api_key: "********"

            klaviyo:
                - name: "klaviyo-default"
                  api_key: "********"
```

## Running the pipeline

Run the whole pipeline:

```shell
bruin run ./ecommerce
```

Or run a single asset (with downstreams via `--downstream`):

```shell
bruin run ./ecommerce/assets/reports/rpt_daily_revenue.sql --downstream
```
