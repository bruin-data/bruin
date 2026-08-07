/* @bruin
name: stripe_stage.products
type: bq.sql
description: Conformed Stripe product catalog for plan and packaging analysis.

materialization:
  type: table

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
  - name: product_created_at
    type: TIMESTAMP
    description: When the product was created in Stripe.
  - name: product_name
    type: STRING
    description: Product name shown to customers on invoices and receipts.
  - name: is_active
    type: BOOL
    description: Whether the product is currently available for purchase.
  - name: is_live_mode
    type: BOOL
    description: Whether the product exists in Stripe live mode.
  - name: product_metadata
    type: JSON
    description: Full key-value metadata set on the Stripe product.
@bruin */

SELECT
  id AS stripe_product_id,
  TIMESTAMP_SECONDS(SAFE_CAST(created AS INT64)) AS product_created_at,
  name AS product_name,
  active AS is_active,
  livemode AS is_live_mode,
  metadata AS product_metadata
FROM stripe_raw.product;
