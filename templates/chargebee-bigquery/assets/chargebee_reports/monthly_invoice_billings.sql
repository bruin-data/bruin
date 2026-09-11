/* @bruin
name: chargebee_reports.monthly_invoice_billings
type: bq.sql
description: >
  Monthly billed and collected amounts from invoices, per native currency, split
  into recurring subscription billings and one-off billings. This is invoiced
  cash flow (what was billed and paid), which is deliberately distinct from
  committed MRR: an annual plan bills its full year in one month, so billings are
  lumpy where MRR is smooth. Voided invoices are excluded. Amounts are native
  currency in minor units.

materialization:
  type: table

depends:
  - chargebee_stage.invoices

tags:
  - chargebee_reports
  - chargebee
  - billing
  - invoices

columns:
  - name: invoice_billing_month
    type: DATE
    description: First day of the month the invoice was generated in.
    primary_key: true
    checks:
      - name: not_null
  - name: currency_code
    type: STRING
    description: Native currency. Do not sum across currencies.
    primary_key: true
    checks:
      - name: not_null
  - name: invoice_count
    type: INT64
    description: Number of non-voided invoices generated in the month.
  - name: billed_total_minor
    type: NUMERIC
    description: Total billed across all non-voided invoices, in minor units.
  - name: billed_recurring_minor
    type: NUMERIC
    description: Billed amount from recurring subscription invoices, in minor units.
  - name: billed_one_time_minor
    type: NUMERIC
    description: Billed amount from non-recurring invoices, in minor units.
  - name: collected_total_minor
    type: NUMERIC
    description: Amount paid against invoices generated in the month, in minor units.
  - name: outstanding_due_minor
    type: NUMERIC
    description: Amount still due on invoices generated in the month, in minor units.
  - name: paid_invoice_count
    type: INT64
    description: Number of invoices generated in the month that are fully paid.
@bruin */

SELECT
  DATE(DATE_TRUNC(invoice_date, MONTH)) AS invoice_billing_month,
  currency_code,
  COUNT(*) AS invoice_count,
  SUM(invoice_total_minor) AS billed_total_minor,
  SUM(IF(is_recurring, invoice_total_minor, CAST(0 AS NUMERIC))) AS billed_recurring_minor,
  SUM(IF(NOT is_recurring, invoice_total_minor, CAST(0 AS NUMERIC))) AS billed_one_time_minor,
  SUM(amount_paid_minor) AS collected_total_minor,
  SUM(amount_due_minor) AS outstanding_due_minor,
  COUNTIF(invoice_status = 'paid') AS paid_invoice_count
FROM chargebee_stage.invoices
WHERE invoice_status != 'voided'
  AND invoice_date IS NOT NULL
  AND currency_code IS NOT NULL
GROUP BY 1, 2;
