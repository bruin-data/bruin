/* @bruin
name: quickbooks_reports.ar_aging
type: bq.sql
owner: finance@example.com
description: >
  Open invoices with how overdue each one is, as of the run's end date. One
  row per invoice issued by that date with an open balance, using the balance
  QuickBooks reported at the last load. Use it to answer who owes money and how late it is.

materialization:
  type: table

depends:
  - quickbooks_stage.invoices
  - quickbooks_stage.customers
  - quickbooks_stage.accounts

tags:
  - quickbooks_reports
  - quickbooks
  - accounting
  - accounts-receivable
  - collections

domains:
  - finance

meta:
  grain: one row per invoice with an open balance
  contains_pii: "true"
  pii_note: Includes customer names and billing emails.
  answers: >
    Who owes us money? Which invoices are overdue and by how much? What is
    our total accounts receivable?
  agent_notes: >
    Sum open_balance by customer for what each customer owes, or by
    aging_bucket for the aging summary. Contact details are on
    quickbooks_stage.customers.

columns:
  - name: invoice_id
    type: STRING
    description: Invoice; joins to `quickbooks_stage.invoices`.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: invoice_number
    type: STRING
    description: Customer-facing invoice number.
  - name: customer_id
    type: STRING
    description: Billed customer.
    checks:
      - name: not_null
  - name: customer_name
    type: STRING
    description: Billed customer's display name.
  - name: customer_email
    type: STRING
    description: Customer's primary email, for follow-up.
  - name: invoice_date
    type: DATE
    description: Date the invoice was issued.
  - name: due_date
    type: DATE
    description: Date payment is due.
  - name: total_amount
    type: NUMERIC
    description: Invoice total.
  - name: open_balance
    type: NUMERIC
    description: Amount still owed at the last load.
    checks:
      - name: not_null
      - name: positive
  - name: as_of_date
    type: DATE
    description: Date the aging is measured from, the run's end date.
    checks:
      - name: not_null
  - name: days_overdue
    type: INT64
    description: Days past the due date as of `as_of_date`; 0 when not yet due.
    checks:
      - name: not_null
      - name: non_negative
  - name: aging_bucket
    type: STRING
    description: '`current` (not yet due), `1-30`, `31-60`, `61-90`, or `90+` days overdue.'
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - current
          - 1-30
          - 31-60
          - 61-90
          - 90+

custom_checks:
  - name: receivables match the A/R account balance
    description: >
      Non-blocking: the aging total should equal the balance of the Accounts
      Receivable accounts. A gap usually means invoices were deleted in
      QuickBooks after they were loaded, or credit memos and journal entries
      changed receivables; rerun the first-load command to reload.
    query: |
      SELECT IF(
        ABS(
          (SELECT IFNULL(SUM(open_balance), 0) FROM {{ this }})
          - (
            SELECT IFNULL(SUM(current_balance), 0)
            FROM quickbooks_stage.accounts
            WHERE account_type = 'Accounts Receivable'
          )
        ) > 1,
        1,
        0
      )
    value: 0
    blocking: false
unit_tests:
  - name: buckets_open_invoices
    inputs:
      - asset: quickbooks_stage.invoices
        rows:
          - {invoice_id: i1, invoice_number: INV-1, customer_id: c1, customer_name: Acme, invoice_date: "1999-12-01", due_date: "2000-01-01", total_amount: 100, open_balance: 100}
          - {invoice_id: i2, customer_id: c1, invoice_date: "2000-02-01", due_date: "2099-01-31", total_amount: 80, open_balance: 50}
          - {invoice_id: i4, customer_id: c1, invoice_date: "2099-01-01", due_date: "2099-01-31", total_amount: 60, open_balance: 60}
          - {invoice_id: i3, customer_id: c2, invoice_date: "2000-01-01", due_date: "2000-01-31", total_amount: 70, open_balance: 0}
      - asset: quickbooks_stage.customers
        rows:
          - {customer_id: c1, email: ap@acme.example}
    expected:
      match: exact
      rows:
        - {invoice_id: i1, customer_email: ap@acme.example, open_balance: 100, aging_bucket: 90+}
        - {invoice_id: i2, open_balance: 50, days_overdue: 0, aging_bucket: current}
@bruin */

WITH open_invoices AS (
  SELECT
    invoice.*,
    DATE('{{ end_date }}') AS as_of_date,
    GREATEST(DATE_DIFF(DATE('{{ end_date }}'), COALESCE(invoice.due_date, invoice.invoice_date), DAY), 0) AS days_overdue
  FROM quickbooks_stage.invoices AS invoice
  WHERE invoice.open_balance > 0
    AND invoice.invoice_date <= DATE('{{ end_date }}')
)

SELECT
  open_invoices.invoice_id,
  open_invoices.invoice_number,
  open_invoices.customer_id,
  open_invoices.customer_name,
  customer.email AS customer_email,
  open_invoices.invoice_date,
  open_invoices.due_date,
  open_invoices.total_amount,
  open_invoices.open_balance,
  open_invoices.as_of_date,
  open_invoices.days_overdue,
  CASE
    WHEN open_invoices.days_overdue = 0 THEN 'current'
    WHEN open_invoices.days_overdue <= 30 THEN '1-30'
    WHEN open_invoices.days_overdue <= 60 THEN '31-60'
    WHEN open_invoices.days_overdue <= 90 THEN '61-90'
    ELSE '90+'
  END AS aging_bucket
FROM open_invoices
LEFT JOIN quickbooks_stage.customers AS customer
  ON open_invoices.customer_id = customer.customer_id;
