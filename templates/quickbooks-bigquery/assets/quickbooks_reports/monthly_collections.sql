/* @bruin
name: quickbooks_reports.monthly_collections
type: bq.sql
owner: finance@example.com
description: >
  How fast customers pay, by month: amount invoiced, cash collected against
  invoices, receivables at month end, days sales outstanding (DSO), and
  payment timing. Month-end receivables are rebuilt from invoice dates and
  payment dates, so they ignore credit memos and other non-payment credits.
  Only complete months up to the run's end date are included.

materialization:
  type: table

depends:
  - quickbooks_stage.invoices
  - quickbooks_stage.payment_applications

tags:
  - quickbooks_reports
  - quickbooks
  - accounting
  - collections

domains:
  - finance

meta:
  grain: one row per month
  answers: >
    What is our DSO? Are customers paying on time? How much cash did we
    collect last month?
  contains_pii: "false"
  agent_notes: >
    DSO is the simple one-month method: receivables at month end divided by
    the month's invoicing, times the days in the month.

columns:
  - name: month
    type: DATE
    description: First day of the month.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: invoiced_amount
    type: NUMERIC
    description: Total of invoices issued in the month.
    checks:
      - name: not_null
      - name: non_negative
  - name: collected_amount
    type: NUMERIC
    description: Payments applied to invoices, by payment date, in the month.
    checks:
      - name: not_null
      - name: non_negative
  - name: receivables_end_of_month
    type: NUMERIC
    description: >
      Unpaid invoice balances at the end of the month: invoices issued by then,
      less payments applied by then.
    checks:
      - name: not_null
      - name: non_negative
  - name: dso_days
    type: FLOAT64
    description: >
      Days sales outstanding, `receivables_end_of_month / invoiced_amount *
      days in month`; null when nothing was invoiced.
    checks:
      - name: non_negative
  - name: payments_count
    type: INT64
    description: Number of invoice payments received in the month.
    checks:
      - name: not_null
  - name: avg_days_to_pay
    type: FLOAT64
    description: Average days from invoice date to payment, for payments in the month.
  - name: on_time_payment_rate
    type: FLOAT64
    description: Share of payments in the month received on or before the due date, from 0 to 1.
    checks:
      - name: min
        value: 0
      - name: max
        value: 1

custom_checks:
  - name: collections reconcile to payment applications
    description: >
      Collected amounts add up to the payment applications in the same
      months.
    query: |
      SELECT COUNTIF(ABS(report.collected - source.collected) > 0.01)
      FROM (SELECT month, collected_amount AS collected FROM {{ this }}) AS report
      JOIN (
        SELECT DATE_TRUNC(payment_date, MONTH) AS month, SUM(applied_amount) AS collected
        FROM quickbooks_stage.payment_applications
        GROUP BY month
      ) AS source
        USING (month)
    value: 0
@bruin */

WITH months AS (
  SELECT month
  FROM UNNEST(
    GENERATE_DATE_ARRAY(
      (SELECT DATE_TRUNC(MIN(invoice_date), MONTH) FROM quickbooks_stage.invoices),
      DATE_TRUNC(DATE('{{ end_date }}'), MONTH),
      INTERVAL 1 MONTH
    )
  ) AS month
  WHERE LAST_DAY(month) <= DATE('{{ end_date }}')
),

invoiced AS (
  SELECT DATE_TRUNC(invoice_date, MONTH) AS month, SUM(total_amount) AS invoiced_amount
  FROM quickbooks_stage.invoices
  GROUP BY month
),

collected AS (
  SELECT
    DATE_TRUNC(payment_date, MONTH) AS month,
    SUM(applied_amount) AS collected_amount,
    COUNT(*) AS payments_count,
    AVG(days_to_pay) AS avg_days_to_pay,
    AVG(IF(days_past_due IS NULL, NULL, IF(days_past_due <= 0, 1, 0))) AS on_time_payment_rate
  FROM quickbooks_stage.payment_applications
  GROUP BY month
),

invoiced_to_date AS (
  SELECT months.month, SUM(invoice.total_amount) AS amount
  FROM months
  JOIN quickbooks_stage.invoices AS invoice
    ON invoice.invoice_date <= LAST_DAY(months.month)
  GROUP BY months.month
),

collected_to_date AS (
  SELECT months.month, SUM(application.applied_amount) AS amount
  FROM months
  JOIN quickbooks_stage.payment_applications AS application
    ON application.payment_date <= LAST_DAY(months.month)
    AND application.invoice_date <= LAST_DAY(months.month)
  GROUP BY months.month
),

receivables AS (
  SELECT
    months.month,
    IFNULL(invoiced_to_date.amount, 0) - IFNULL(collected_to_date.amount, 0) AS receivables_end_of_month
  FROM months
  LEFT JOIN invoiced_to_date USING (month)
  LEFT JOIN collected_to_date USING (month)
)

SELECT
  months.month,
  ROUND(IFNULL(invoiced.invoiced_amount, 0), 2) AS invoiced_amount,
  ROUND(IFNULL(collected.collected_amount, 0), 2) AS collected_amount,
  ROUND(GREATEST(receivables.receivables_end_of_month, 0), 2) AS receivables_end_of_month,
  ROUND(
    SAFE_DIVIDE(
      CAST(GREATEST(receivables.receivables_end_of_month, 0) AS FLOAT64),
      CAST(invoiced.invoiced_amount AS FLOAT64)
    ) * EXTRACT(DAY FROM LAST_DAY(months.month)),
    1
  ) AS dso_days,
  IFNULL(collected.payments_count, 0) AS payments_count,
  ROUND(collected.avg_days_to_pay, 1) AS avg_days_to_pay,
  ROUND(collected.on_time_payment_rate, 3) AS on_time_payment_rate
FROM months
LEFT JOIN invoiced USING (month)
LEFT JOIN collected USING (month)
LEFT JOIN receivables USING (month);
