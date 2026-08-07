/* @bruin
name: stripe_stage.customers
type: bq.sql
description: >
  Conformed Stripe billing accounts. This model keeps customer identifiers and
  metadata available for downstream billing analytics; direct contact details
  remain in the raw source and are intentionally excluded from reports.

materialization:
  type: table

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
  - name: customer_created_at
    type: TIMESTAMP
    description: When the customer was created in Stripe.
  - name: customer_name
    type: STRING
    description: Customer's full name or business name.
  - name: customer_email
    type: STRING
    description: >
      Customer's email address. It is kept here for operational lookups and is
      not carried into the reporting layer.
  - name: is_live_mode
    type: BOOL
    description: Whether the customer exists in Stripe live mode.
  - name: customer_metadata
    type: JSON
    description: Full key-value metadata set on the Stripe customer.
  - name: crm_account_id
    type: STRING
    description: >
      CRM account identifier read from the `crm_account_id` metadata key. It is
      null unless your Stripe customers carry that key.
  - name: customer_segment
    type: STRING
    description: Customer segment read from the `segment` metadata key.
  - name: customer_region
    type: STRING
    description: Customer region read from the `region` metadata key.
  - name: sales_owner
    type: STRING
    description: Sales owner read from the `sales_owner` metadata key.
  - name: acquisition_channel
    type: STRING
    description: >
      Acquisition channel read from the `acquisition_channel` metadata key.
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
