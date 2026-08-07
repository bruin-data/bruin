/* @bruin
name: stripe_stage.prices
type: bq.sql
description: >
  Conformed Stripe prices with recurring-cadence attributes and monetary values
  retained in native-currency minor units.

materialization:
  type: table

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
  - name: stripe_product_id
    type: STRING
    description: Stripe product the price belongs to.
  - name: product_name
    type: STRING
    description: Product name resolved from `stripe_stage.products`.
  - name: price_created_at
    type: TIMESTAMP
    description: When the price was created in Stripe.
  - name: currency
    type: STRING
    description: Three-letter ISO currency code the price is charged in.
  - name: price_type
    type: STRING
    description: Stripe price type, either `one_time` or `recurring`.
  - name: is_active
    type: BOOL
    description: Whether the price can be used for new purchases.
  - name: is_live_mode
    type: BOOL
    description: Whether the price exists in Stripe live mode.
  - name: unit_amount_minor
    type: NUMERIC
    description: >
      Amount charged per unit in native-currency minor units. It is null for
      tiered prices, which carry no single unit amount and are excluded from MRR.
  - name: billing_scheme
    type: STRING
    description: How the amount is computed, either `per_unit` or `tiered`.
  - name: recurring_config
    type: JSON
    description: Full Stripe recurring configuration retained for reference.
  - name: is_recurring
    type: BOOL
    description: Whether the price is recurring, derived from `price_type`.
  - name: recurring_interval
    type: STRING
    description: >
      Billing interval read from the recurring configuration, for example
      `month` or `year`. Only monthly and annual intervals contribute to MRR.
  - name: recurring_interval_count
    type: INT64
    description: >
      Number of intervals in one billing period, defaulted to 1 when Stripe
      omits it.
  - name: recurring_usage_type
    type: STRING
    description: >
      Usage type read from the recurring configuration, defaulted to
      `licensed`. Metered prices are excluded from MRR.
  - name: price_metadata
    type: JSON
    description: Full key-value metadata set on the Stripe price.
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
