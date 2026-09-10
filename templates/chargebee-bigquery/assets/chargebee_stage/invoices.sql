/* @bruin
name: chargebee_stage.invoices
type: bq.sql
description: >
  Conformed Chargebee invoices for billed-revenue and accounts-receivable
  analysis. Unix timestamps are cast to TIMESTAMP and all money is kept in
  native-currency minor units as NUMERIC. `is_subscription_invoice` separates
  recurring subscription billings from one-off invoices. Invoiced amounts are
  billings, not recognized revenue and not committed MRR. Deleted invoices are
  filtered out.

materialization:
  type: table

depends:
  - chargebee_raw.invoice

tags:
  - chargebee_stage
  - chargebee
  - billing
  - invoices

columns:
  - name: chargebee_invoice_id
    type: STRING
    description: Chargebee invoice identifier and natural key.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: chargebee_customer_id
    type: STRING
    description: Customer the invoice was raised for.
    checks:
      - name: not_null
  - name: chargebee_subscription_id
    type: STRING
    description: Subscription the invoice was generated for, when applicable.
  - name: source_system
    type: STRING
    description: Constant `chargebee`.
    checks:
      - name: not_null
  - name: invoice_status
    type: STRING
    description: >
      Invoice status, one of paid, posted, payment_due, not_paid, voided,
      pending.
  - name: is_recurring
    type: BOOL
    description: Whether the invoice was generated from a subscription cycle.
  - name: is_first_invoice
    type: BOOL
    description: Whether this is the first invoice for the subscription.
  - name: is_subscription_invoice
    type: BOOL
    description: Whether the invoice is tied to a subscription.
  - name: currency_code
    type: STRING
    description: Native currency the invoice is billed in.
  - name: invoice_total_minor
    type: NUMERIC
    description: Invoice total in native-currency minor units.
  - name: invoice_sub_total_minor
    type: NUMERIC
    description: Invoice subtotal before tax in minor units.
  - name: invoice_tax_minor
    type: NUMERIC
    description: Tax charged on the invoice in minor units.
  - name: amount_paid_minor
    type: NUMERIC
    description: Amount paid against the invoice in minor units.
  - name: amount_due_minor
    type: NUMERIC
    description: Amount still due on the invoice in minor units.
  - name: amount_adjusted_minor
    type: NUMERIC
    description: Amount adjusted on the invoice in minor units.
  - name: credits_applied_minor
    type: NUMERIC
    description: Credits applied to the invoice in minor units.
  - name: write_off_amount_minor
    type: NUMERIC
    description: Amount written off on the invoice in minor units.
  - name: invoice_date
    type: TIMESTAMP
    description: When the invoice was generated.
  - name: invoice_due_at
    type: TIMESTAMP
    description: When payment on the invoice is due.
  - name: invoice_paid_at
    type: TIMESTAMP
    description: When the invoice was paid.
  - name: invoice_updated_at
    type: TIMESTAMP
    description: When the invoice was last modified.
@bruin */

SELECT
  id AS chargebee_invoice_id,
  customer_id AS chargebee_customer_id,
  subscription_id AS chargebee_subscription_id,
  'chargebee' AS source_system,
  status AS invoice_status,
  COALESCE(recurring, FALSE) AS is_recurring,
  COALESCE(first_invoice, FALSE) AS is_first_invoice,
  subscription_id IS NOT NULL AS is_subscription_invoice,
  currency_code,
  SAFE_CAST(total AS NUMERIC) AS invoice_total_minor,
  SAFE_CAST(sub_total AS NUMERIC) AS invoice_sub_total_minor,
  SAFE_CAST(tax AS NUMERIC) AS invoice_tax_minor,
  SAFE_CAST(amount_paid AS NUMERIC) AS amount_paid_minor,
  SAFE_CAST(amount_due AS NUMERIC) AS amount_due_minor,
  SAFE_CAST(amount_adjusted AS NUMERIC) AS amount_adjusted_minor,
  SAFE_CAST(credits_applied AS NUMERIC) AS credits_applied_minor,
  SAFE_CAST(write_off_amount AS NUMERIC) AS write_off_amount_minor,
  TIMESTAMP_SECONDS(SAFE_CAST(date AS INT64)) AS invoice_date,
  TIMESTAMP_SECONDS(SAFE_CAST(due_date AS INT64)) AS invoice_due_at,
  -- Read optional fields from the row JSON so accounts whose inferred raw
  -- schema omits `paid_at` still compile, while accounts that provide it keep
  -- the payment timestamp.
  TIMESTAMP_SECONDS(SAFE_CAST(
    JSON_VALUE(TO_JSON_STRING(invoice), '$.paid_at') AS INT64
  )) AS invoice_paid_at,
  TIMESTAMP_SECONDS(SAFE_CAST(updated_at AS INT64)) AS invoice_updated_at
FROM chargebee_raw.invoice AS invoice
WHERE NOT COALESCE(deleted, FALSE);
