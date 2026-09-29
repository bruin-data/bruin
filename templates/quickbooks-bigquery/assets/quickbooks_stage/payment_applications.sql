/* @bruin
name: quickbooks_stage.payment_applications
type: bq.sql
description: >
  Links customer payments to the invoices they paid, one row per payment line
  applied to an invoice. Use it for collections and days-to-pay analysis.
  Credit memos applied inside a payment are excluded, so a payment's rows can
  add up to more than its cash total when credits were applied.

materialization:
  type: table

depends:
  - quickbooks_raw.payments
  - quickbooks_stage.invoices

tags:
  - quickbooks_stage
  - quickbooks
  - accounting
  - payments
  - collections

columns:
  - name: payment_id
    type: STRING
    description: Payment that settled the invoice; joins to `quickbooks_stage.payments`.
    primary_key: true
    checks:
      - name: not_null
  - name: line_index
    type: INT64
    description: Zero-based position of the line in the payment's line array.
    primary_key: true
    checks:
      - name: not_null
  - name: invoice_id
    type: STRING
    description: Invoice the payment was applied to; joins to `quickbooks_stage.invoices`.
    primary_key: true
    checks:
      - name: not_null
  - name: customer_id
    type: STRING
    description: Paying customer.
  - name: payment_date
    type: DATE
    description: Date the payment was received.
    checks:
      - name: not_null
  - name: invoice_date
    type: DATE
    description: Issue date of the paid invoice; null if the invoice was not loaded.
  - name: invoice_due_date
    type: DATE
    description: Due date of the paid invoice.
  - name: applied_amount
    type: NUMERIC
    description: Amount of the payment applied to the invoice, in the payment currency.
    checks:
      - name: not_null
      - name: positive
  - name: days_to_pay
    type: INT64
    description: Days from the invoice date to the payment date.
  - name: days_past_due
    type: INT64
    description: >
      Days from the invoice due date to the payment date; negative when paid
      early.

custom_checks:
  - name: payment application keys are unique
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT payment_id, line_index, invoice_id
        FROM {{ this }}
        GROUP BY 1, 2, 3
        HAVING COUNT(*) > 1
      )
    value: 0
  - name: every applied invoice exists
    description: >
      Non-blocking: payment lines that reference an invoice missing from
      `quickbooks_stage.invoices`, which usually means the invoice was deleted
      in QuickBooks.
    query: |
      SELECT COUNTIF(invoice_date IS NULL)
      FROM {{ this }}
    value: 0
    blocking: false
@bruin */

WITH applications AS (
  SELECT
    payment.id AS payment_id,
    line_index,
    JSON_VALUE(linked_txn, '$.TxnId') AS invoice_id,
    JSON_VALUE(payment.customer_ref, '$.value') AS customer_id,
    payment.txn_date AS payment_date,
    ROUND(SAFE_CAST(JSON_VALUE(line, '$.Amount') AS NUMERIC), 2) AS applied_amount
  FROM quickbooks_raw.payments AS payment
  CROSS JOIN UNNEST(
    IFNULL(JSON_QUERY_ARRAY(payment.line), ARRAY<JSON>[])
  ) AS line WITH OFFSET AS line_index
  CROSS JOIN UNNEST(
    IFNULL(JSON_QUERY_ARRAY(line, '$.LinkedTxn'), ARRAY<JSON>[])
  ) AS linked_txn
  WHERE JSON_VALUE(linked_txn, '$.TxnType') = 'Invoice'
)

SELECT
  application.payment_id,
  application.line_index,
  application.invoice_id,
  application.customer_id,
  application.payment_date,
  invoice.invoice_date,
  invoice.due_date AS invoice_due_date,
  application.applied_amount,
  DATE_DIFF(application.payment_date, invoice.invoice_date, DAY) AS days_to_pay,
  DATE_DIFF(application.payment_date, invoice.due_date, DAY) AS days_past_due
FROM applications AS application
LEFT JOIN quickbooks_stage.invoices AS invoice
  ON application.invoice_id = invoice.invoice_id;
