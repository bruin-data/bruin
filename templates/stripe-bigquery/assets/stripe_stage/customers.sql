/* @bruin
name: stripe_stage.customers
type: bq.sql
description: >
  Conformed Stripe billing accounts. This model keeps customer identifiers and
  metadata available for downstream billing analytics; direct contact details
  remain in the raw source and are intentionally excluded from reports.

materialization:
  type: table
  strategy: truncate+insert

depends:
  - stripe_raw.customer

tags:
  - stripe_stage
  - stripe
  - billing
  - customers

columns:
  - name: stripe_customer_id
    type: STRING
    description: Stripe billing customer identifier and natural key.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
@bruin */

SELECT
  id AS stripe_customer_id,
  TIMESTAMP_SECONDS(SAFE_CAST(created AS INT64)) AS customer_created_at,
  name AS customer_name,
  email AS customer_email,
  livemode AS is_live_mode,
  metadata AS customer_metadata,
  JSON_VALUE(metadata, '$.crm_account_id') AS crm_account_id,
  JSON_VALUE(metadata, '$.segment') AS customer_segment,
  JSON_VALUE(metadata, '$.region') AS customer_region,
  JSON_VALUE(metadata, '$.sales_owner') AS sales_owner,
  JSON_VALUE(metadata, '$.acquisition_channel') AS acquisition_channel
FROM stripe_raw.customer;
