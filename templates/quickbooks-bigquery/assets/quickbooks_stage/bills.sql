/* @bruin
name: quickbooks_stage.bills
type: bq.sql
description: >
  One row per QuickBooks vendor bill, with typed amounts, the vendor and
  payment terms resolved, and a payment status derived from the open balance.
  Amounts are in the bill currency. The open balance reflects the bill
  payments applied as of the last load. Bill lines are in
  `quickbooks_stage.expense_lines`.

materialization:
  type: table

depends:
  - quickbooks_raw.bills

tags:
  - quickbooks_stage
  - quickbooks
  - accounting
  - bills
  - accounts-payable

columns:
  - name: bill_id
    type: STRING
    description: QuickBooks bill identifier.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: bill_number
    type: STRING
    description: The vendor's invoice or reference number.
  - name: vendor_id
    type: STRING
    description: Billing vendor; joins to `quickbooks_stage.vendors`.
    checks:
      - name: not_null
  - name: vendor_name
    type: STRING
    description: Vendor display name at the time of the last load.
  - name: bill_date
    type: DATE
    description: Date the bill was issued.
    checks:
      - name: not_null
  - name: due_date
    type: DATE
    description: Date payment is due.
  - name: payment_terms
    type: STRING
    description: Payment terms on the bill, for example `Net 30`.
  - name: currency
    type: STRING
    description: Bill currency code, for example `USD`.
    checks:
      - name: not_null
  - name: ap_account_id
    type: STRING
    description: Accounts payable account the bill posts to.
  - name: total_amount
    type: NUMERIC
    description: Bill total including tax.
    checks:
      - name: not_null
      - name: non_negative
  - name: open_balance
    type: NUMERIC
    description: Amount still owed to the vendor at the last load.
    checks:
      - name: not_null
      - name: non_negative
  - name: amount_paid
    type: NUMERIC
    description: >
      Amount settled so far, `total_amount - open_balance`. It includes bill
      payments and applied vendor credits.
    checks:
      - name: non_negative
  - name: bill_status
    type: STRING
    description: >
      `paid` when nothing is owed, `partially_paid` when part of the total is
      owed, `open` when the full total is owed, and `zero_amount` for bills
      with a zero total.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - paid
          - partially_paid
          - open
          - zero_amount
  - name: private_note
    type: STRING
    description: Internal memo on the bill.
  - name: created_at
    type: TIMESTAMP
    description: When the bill was created in QuickBooks.
  - name: updated_at
    type: TIMESTAMP
    description: When the bill was last modified in QuickBooks.
    checks:
      - name: not_null

custom_checks:
  - name: open balance never exceeds the bill total
    query: |
      SELECT COUNT(*)
      FROM {{ this }}
      WHERE open_balance > total_amount
    value: 0
@bruin */

WITH bills AS (
  SELECT
    id AS bill_id,
    doc_number AS bill_number,
    JSON_VALUE(vendor_ref, '$.value') AS vendor_id,
    JSON_VALUE(vendor_ref, '$.name') AS vendor_name,
    txn_date AS bill_date,
    due_date,
    JSON_VALUE(sales_term_ref, '$.name') AS payment_terms,
    JSON_VALUE(currency_ref, '$.value') AS currency,
    JSON_VALUE(ap_account_ref, '$.value') AS ap_account_id,
    ROUND(CAST(total_amt AS NUMERIC), 2) AS total_amount,
    ROUND(CAST(IFNULL(balance, 0) AS NUMERIC), 2) AS open_balance,
    private_note,
    TIMESTAMP(JSON_VALUE(meta_data, '$.CreateTime')) AS created_at,
    lastupdatedtime AS updated_at
  FROM quickbooks_raw.bills
)

SELECT
  bill_id,
  bill_number,
  vendor_id,
  vendor_name,
  bill_date,
  due_date,
  payment_terms,
  currency,
  ap_account_id,
  total_amount,
  open_balance,
  total_amount - open_balance AS amount_paid,
  CASE
    WHEN total_amount = 0 THEN 'zero_amount'
    WHEN open_balance = 0 THEN 'paid'
    WHEN open_balance < total_amount THEN 'partially_paid'
    ELSE 'open'
  END AS bill_status,
  private_note,
  created_at,
  updated_at
FROM bills;
