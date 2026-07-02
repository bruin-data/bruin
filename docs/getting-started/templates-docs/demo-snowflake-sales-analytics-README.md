# Bruin - Snowflake Sales Analytics Demo Template

This template creates an end-to-end Snowflake analytics demo with dummy retail sales data.
Python bronze assets generate source-shaped tables, Snowflake SQL silver assets normalize the data,
and gold assets produce decision-ready outputs for limited-edition SKU reviews.

## Included assets

- `assets/bronze/*.py` generates deterministic product, retailer, store, sales, distribution, inventory, promotion, and cost data.
- `assets/silver/sku_daily_sales.sql` creates the normalized daily sales fact.
- `assets/silver/limited_edition_performance.sql` and `assets/silver/retailer_channel_scorecard.sql` build analytical marts.
- `assets/gold/*.sql` builds lifecycle recommendations, decision drivers, channel alerts, and weekly SKU council summaries.

The template includes only placeholder Snowflake connection values. Replace them in `.bruin.yml` before running.

## Setup

Initialize the template:

```bash
bruin init demo-snowflake-sales-analytics my-sales-demo
cd my-sales-demo
```

Edit `.bruin.yml` with a Snowflake connection named `snowflake-default`:

```yaml
snowflake:
  - name: "snowflake-default"
    username: "YOUR_SNOWFLAKE_USERNAME"
    password: "YOUR_SNOWFLAKE_PASSWORD"
    account: "YOUR_SNOWFLAKE_ACCOUNT"
    database: "SALES_ANALYTICS_DEMO"
    schema: "BRONZE"
    warehouse: "COMPUTE_WH"
    role: "YOUR_SNOWFLAKE_ROLE"
```

## Running the demo

Validate the pipeline:

```bash
bruin validate --fast .
```

Run a short interval first:

```bash
bruin run --full-refresh --start-date 2026-06-01 --end-date 2026-06-08 .
```

The Python bronze assets are interval-aware. `BRUIN_START_DATE` is inclusive and `BRUIN_END_DATE` is exclusive, so the command above generates one week of activity.
