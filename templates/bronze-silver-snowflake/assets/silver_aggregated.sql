/* @bruin
name: silver_exchange_rates
type: sf.sql
depends:
  - bronze_exchange_rates
materialization:
  type: table

description: |
  Silver layer transformation - aggregates and cleans bronze exchange rate data.
  Demonstrates typical transformation patterns for Snowflake.
@bruin */

SELECT
    base AS base_currency,
    date AS exchange_date,
    rates:USD::FLOAT AS usd_rate,
    rates:EUR::FLOAT AS eur_rate,
    rates:GBP::FLOAT AS gbp_rate,
    rates:JPY::FLOAT AS jpy_rate,
    CURRENT_TIMESTAMP() AS processed_at
FROM {{ ref('bronze_exchange_rates') }}
WHERE date IS NOT NULL
