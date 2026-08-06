/* @bruin
name: stripe_stage.prices
type: bq.sql
description: >
  Conformed Stripe prices with recurring-cadence attributes and monetary values
  retained in native-currency minor units.

materialization:
  type: table
  strategy: truncate+insert

depends:
  - stripe_raw.price
  - stripe_stage.products

tags:
  - stripe_stage
  - stripe
  - billing
  - prices

columns:
  - name: stripe_price_id
    type: STRING
    description: Stripe price identifier and natural key.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
@bruin */

SELECT
  p.id AS stripe_price_id,
  p.product AS stripe_product_id,
  product.product_name,
  TIMESTAMP_SECONDS(SAFE_CAST(p.created AS INT64)) AS price_created_at,
  p.currency,
  p.type AS price_type,
  p.active AS is_active,
  p.livemode AS is_live_mode,
  SAFE_CAST(p.unit_amount AS NUMERIC) AS unit_amount_minor,
  p.billing_scheme,
  p.recurring AS recurring_config,
  p.type = 'recurring' AS is_recurring,
  JSON_VALUE(p.recurring, '$.interval') AS recurring_interval,
  COALESCE(
    SAFE_CAST(JSON_VALUE(p.recurring, '$.interval_count') AS INT64),
    1
  ) AS recurring_interval_count,
  COALESCE(
    JSON_VALUE(p.recurring, '$.usage_type'),
    'licensed'
  ) AS recurring_usage_type,
  p.metadata AS price_metadata
FROM stripe_raw.price AS p
LEFT JOIN stripe_stage.products AS product
  ON p.product = product.stripe_product_id;
