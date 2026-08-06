/* @bruin
name: stripe_stage.invoices
type: bq.sql
description: >
  Conformed invoice facts for billings, current AR, and invoice-line analysis.
  Amounts are native-currency minor units and are not accounting revenue.
  invoice_lines_has_more flags Stripe's paginated embedded line payload; a true
  value means the flattened invoice-line model is not a complete subledger.

materialization:
  type: table
  strategy: truncate+insert

depends:
  - stripe_raw.invoice

tags:
  - stripe_stage
  - stripe
  - billing
  - invoices

columns:
  - name: stripe_invoice_id
    type: STRING
    description: Stripe invoice identifier and natural key.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: invoice_lines_has_more
    type: BOOL
    description: >
      Whether Stripe reports more pages in the embedded invoice.lines payload.
      True means invoice_line_items is incomplete; null means the source did
      not provide a parseable pagination indicator.
@bruin */

SELECT
  id AS stripe_invoice_id,
  customer AS stripe_customer_id,
  TIMESTAMP_SECONDS(SAFE_CAST(created AS INT64)) AS invoice_created_at,
  TIMESTAMP_SECONDS(SAFE_CAST(due_date AS INT64)) AS invoice_due_at,
  TIMESTAMP_SECONDS(SAFE_CAST(
    JSON_VALUE(status_transitions, '$.finalized_at') AS INT64
  )) AS invoice_finalized_at,
  status AS invoice_status,
  billing_reason,
  collection_method,
  currency,
  livemode AS is_live_mode,
  paid AS is_paid,
  SAFE_CAST(attempt_count AS INT64) AS payment_attempt_count,
  SAFE_CAST(amount_due AS NUMERIC) AS invoice_amount_due_minor,
  SAFE_CAST(amount_paid AS NUMERIC) AS invoice_amount_paid_minor,
  SAFE_CAST(amount_remaining AS NUMERIC) AS invoice_amount_remaining_minor,
  SAFE_CAST(subtotal AS NUMERIC) AS invoice_subtotal_minor,
  SAFE_CAST(total AS NUMERIC) AS invoice_total_minor,
  billing_reason IN (
    'subscription_create',
    'subscription_cycle',
    'subscription_update',
    'subscription_threshold'
  ) AS is_subscription_invoice,
  SAFE_CAST(JSON_VALUE(lines, '$.has_more') AS BOOL) AS invoice_lines_has_more,
  lines AS invoice_lines
FROM stripe_raw.invoice;
