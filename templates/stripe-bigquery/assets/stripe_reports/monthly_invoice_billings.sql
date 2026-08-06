/* @bruin
name: stripe_reports.monthly_invoice_billings
type: bq.sql
description: >
  Monthly invoice billings by native currency. Billings are grouped by Stripe's
  invoice_finalized_at when it is available; rows with
  invoice_created_at_fallback use the invoice creation timestamp because the
  finalization transition was unavailable. This report excludes draft and void
  invoices and does not present current paid status as historical collections.

materialization:
  type: table
  strategy: truncate+insert

depends:
  - stripe_stage.invoices

tags:
  - stripe_reports
  - stripe
  - finance
  - billing
@bruin */

WITH billable_invoices AS (
  SELECT
    currency,
    is_subscription_invoice,
    invoice_total_minor,
    COALESCE(invoice_finalized_at, invoice_created_at) AS invoice_billing_at,
    CASE
      WHEN invoice_finalized_at IS NOT NULL THEN 'invoice_finalized_at'
      ELSE 'invoice_created_at_fallback'
    END AS invoice_billing_date_basis
  FROM stripe_stage.invoices
  WHERE invoice_status NOT IN ('draft', 'void')
)

SELECT
  DATE_TRUNC(DATE(invoice_billing_at), MONTH) AS invoice_billing_month,
  invoice_billing_date_basis,
  currency,
  COUNT(*) AS issued_invoice_count,
  COUNTIF(is_subscription_invoice) AS subscription_invoice_count,
  SUM(COALESCE(invoice_total_minor, 0)) AS invoiced_billings_minor,
  SUM(IF(
    is_subscription_invoice,
    COALESCE(invoice_total_minor, 0),
    CAST(0 AS NUMERIC)
  )) AS invoiced_subscription_billings_minor
FROM billable_invoices
GROUP BY 1, 2, 3;
