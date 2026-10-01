/* @bruin
name: quickbooks_stage.invoices
type: bq.sql
description: >
  One row per QuickBooks invoice, with typed amounts, the billed customer and
  payment terms resolved, and a payment status derived from the open balance.
  Amounts are in the invoice currency. The open balance reflects the payments
  applied as of the last load.

materialization:
  type: table

depends:
  - quickbooks_raw.invoices

tags:
  - quickbooks_stage
  - quickbooks
  - accounting
  - invoices

columns:
  - name: invoice_id
    type: STRING
    description: QuickBooks invoice identifier.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: invoice_number
    type: STRING
    description: Customer-facing invoice number, for example `INV-1001`.
  - name: customer_id
    type: STRING
    description: Billed customer; joins to `quickbooks_stage.customers`.
    checks:
      - name: not_null
  - name: customer_name
    type: STRING
    description: Billed customer's display name at the time of the last load.
  - name: invoice_date
    type: DATE
    description: Date the invoice was issued.
    checks:
      - name: not_null
  - name: due_date
    type: DATE
    description: Date payment is due.
  - name: payment_terms
    type: STRING
    description: Payment terms on the invoice, for example `Net 30`.
  - name: currency
    type: STRING
    description: Invoice currency code, for example `USD`.
    checks:
      - name: not_null
  - name: total_amount
    type: NUMERIC
    description: Invoice total including tax and discounts.
    checks:
      - name: not_null
      - name: non_negative
  - name: tax_amount
    type: NUMERIC
    description: Total tax on the invoice; 0 when no tax applies.
    checks:
      - name: not_null
  - name: open_balance
    type: NUMERIC
    description: Amount still owed on the invoice at the last load.
    checks:
      - name: not_null
      - name: non_negative
  - name: amount_paid
    type: NUMERIC
    description: >
      Amount settled so far, `total_amount - open_balance`. It includes
      payments and applied credits.
    checks:
      - name: non_negative
  - name: invoice_status
    type: STRING
    description: >
      `paid` when nothing is owed, `partially_paid` when part of the total is
      owed, `open` when the full total is owed, and `zero_amount` for invoices
      with a zero total, which includes voided invoices.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - paid
          - partially_paid
          - open
          - zero_amount
  - name: customer_memo
    type: STRING
    description: Message shown to the customer on the invoice.
  - name: private_note
    type: STRING
    description: Internal note not shown to the customer.
  - name: email_status
    type: STRING
    description: '`NotSet`, `NeedToSend`, or `EmailSent`.'
  - name: created_at
    type: TIMESTAMP
    description: When the invoice was created in QuickBooks.
  - name: updated_at
    type: TIMESTAMP
    description: When the invoice was last modified in QuickBooks.
    checks:
      - name: not_null

custom_checks:
  - name: open balance never exceeds the invoice total
    query: |
      SELECT COUNT(*)
      FROM {{ this }}
      WHERE open_balance > total_amount
    value: 0
unit_tests:
  - name: derives_status_and_tax
    inputs:
      - asset: quickbooks_raw.invoices
        rows:
          - id: "paid"
            total_amt: 500
            balance: 0
            customer_ref: {value: "c1"}
            txn_tax_detail: {TotalTax: 40}
            doc_number: null
            txn_date: null
            due_date: null
            sales_term_ref: null
            currency_ref: null
            customer_memo: null
            private_note: null
            email_status: null
            meta_data: null
            lastupdatedtime: null
          - {id: "partial", total_amt: 500, balance: 200}
          - {id: "open", total_amt: 500, balance: 500}
          - {id: "void", total_amt: 0, balance: 0}
    expected:
      match: exact
      rows:
        - {invoice_id: "paid", customer_id: "c1", tax_amount: 40, open_balance: 0, amount_paid: 500, invoice_status: paid}
        - {invoice_id: "partial", tax_amount: 0, amount_paid: 300, invoice_status: partially_paid}
        - {invoice_id: "open", amount_paid: 0, invoice_status: open}
        - {invoice_id: "void", invoice_status: zero_amount}
@bruin */

WITH invoices AS (
  SELECT
    id AS invoice_id,
    doc_number AS invoice_number,
    JSON_VALUE(customer_ref, '$.value') AS customer_id,
    JSON_VALUE(customer_ref, '$.name') AS customer_name,
    txn_date AS invoice_date,
    due_date,
    JSON_VALUE(sales_term_ref, '$.name') AS payment_terms,
    JSON_VALUE(currency_ref, '$.value') AS currency,
    ROUND(CAST(total_amt AS NUMERIC), 2) AS total_amount,
    ROUND(
      IFNULL(SAFE_CAST(JSON_VALUE(txn_tax_detail, '$.TotalTax') AS NUMERIC), 0),
      2
    ) AS tax_amount,
    ROUND(CAST(IFNULL(balance, 0) AS NUMERIC), 2) AS open_balance,
    JSON_VALUE(customer_memo, '$.value') AS customer_memo,
    private_note,
    email_status,
    TIMESTAMP(JSON_VALUE(meta_data, '$.CreateTime')) AS created_at,
    lastupdatedtime AS updated_at
  FROM quickbooks_raw.invoices
)

SELECT
  invoice_id,
  invoice_number,
  customer_id,
  customer_name,
  invoice_date,
  due_date,
  payment_terms,
  currency,
  total_amount,
  tax_amount,
  open_balance,
  total_amount - open_balance AS amount_paid,
  CASE
    WHEN total_amount = 0 THEN 'zero_amount'
    WHEN open_balance = 0 THEN 'paid'
    WHEN open_balance < total_amount THEN 'partially_paid'
    ELSE 'open'
  END AS invoice_status,
  customer_memo,
  private_note,
  email_status,
  created_at,
  updated_at
FROM invoices;
