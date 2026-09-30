# Ecommerce Pipeline Template

An interactive template that sets up a complete ecommerce analytics pipeline.

## What's included

- **Data Ingestion**: Shopify + your choice of payments, marketing, ads, and analytics sources
- **Staging Layer**: Cleaned and joined data across all sources
- **Reports**: Daily revenue, customer cohorts, product performance, marketing ROI, and daily KPIs

### Marketing attribution caveat

`rpt_marketing_roi` estimates channel revenue and ROAS by allocating each day's paid order revenue in proportion to that day's sessions per channel. The source models do not share a stable user or session identifier, so this is a directional estimate rather than user- or session-level attribution. Adapt the report to join on a stable identifier if your sources provide one.

## Usage

```bash
bruin init ecommerce
```

You'll be prompted to select your tech stack:
- **Data Warehouse**: ClickHouse, BigQuery, or Snowflake
- **Payments**: Shopify Payments or Stripe
- **Email Marketing**: Klaviyo or HubSpot
- **Advertising**: Facebook Ads, Google Ads, TikTok Ads (multi-select)
- **Web Analytics**: GA4 or Mixpanel
