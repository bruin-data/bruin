/* @bruin
name: stripe_stage.products
type: bq.sql
description: Conformed Stripe product catalog for plan and packaging analysis.

materialization:
  type: table
  strategy: truncate+insert

depends:
  - stripe_raw.product

tags:
  - stripe_stage
  - stripe
  - billing
  - products

columns:
  - name: stripe_product_id
    type: STRING
    description: Stripe product identifier and natural key.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
@bruin */

SELECT
  id AS stripe_product_id,
  TIMESTAMP_SECONDS(SAFE_CAST(created AS INT64)) AS product_created_at,
  name AS product_name,
  active AS is_active,
  livemode AS is_live_mode,
  metadata AS product_metadata
FROM stripe_raw.product;
