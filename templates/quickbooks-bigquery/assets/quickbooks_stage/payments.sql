/* @bruin
name: quickbooks_stage.payments
type: bq.sql
description: >
  One row per customer payment received in QuickBooks, with typed amounts and
  the deposit account and payment method resolved. `applied_amount` is the
  part of the payment applied to invoices; the rest is unapplied credit.

materialization:
  type: table

depends:
  - quickbooks_raw.payments

tags:
  - quickbooks_stage
  - quickbooks
  - accounting
  - payments

columns:
  - name: payment_id
    type: STRING
    description: QuickBooks payment identifier.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: customer_id
    type: STRING
    description: Paying customer; joins to `quickbooks_stage.customers`.
    checks:
      - name: not_null
  - name: customer_name
    type: STRING
    description: Paying customer's display name at the time of the last load.
  - name: payment_date
    type: DATE
    description: Date the payment was received.
    checks:
      - name: not_null
  - name: currency
    type: STRING
    description: Payment currency code, for example `USD`.
    checks:
      - name: not_null
  - name: total_amount
    type: NUMERIC
    description: Total amount received.
    checks:
      - name: not_null
      - name: non_negative
  - name: unapplied_amount
    type: NUMERIC
    description: Part of the payment not applied to any invoice.
    checks:
      - name: not_null
      - name: non_negative
  - name: applied_amount
    type: NUMERIC
    description: >
      Part of the payment applied to transactions, `total_amount -
      unapplied_amount`. Besides invoices it can include applications to
      journal entries or deposits, which `quickbooks_stage.payment_applications`
      leaves out.
    checks:
      - name: non_negative
  - name: deposit_account_id
    type: STRING
    description: >
      Bank or Undeposited Funds account the payment was deposited to; joins to
      `quickbooks_stage.accounts`.
  - name: deposit_account_name
    type: STRING
    description: Name of the deposit account.
  - name: payment_method
    type: STRING
    description: Payment method name, for example `Check` or `ACH`, when recorded.
  - name: reference_number
    type: STRING
    description: Payment reference, such as a check or transfer number.
  - name: private_note
    type: STRING
    description: Internal memo on the payment.
  - name: created_at
    type: TIMESTAMP
    description: When the payment was created in QuickBooks.
  - name: updated_at
    type: TIMESTAMP
    description: When the payment was last modified in QuickBooks.
    checks:
      - name: not_null
@bruin */

WITH payments AS (
  SELECT
    id AS payment_id,
    JSON_VALUE(customer_ref, '$.value') AS customer_id,
    JSON_VALUE(customer_ref, '$.name') AS customer_name,
    txn_date AS payment_date,
    JSON_VALUE(currency_ref, '$.value') AS currency,
    ROUND(CAST(total_amt AS NUMERIC), 2) AS total_amount,
    ROUND(CAST(IFNULL(unapplied_amt, 0) AS NUMERIC), 2) AS unapplied_amount,
    JSON_VALUE(deposit_to_account_ref, '$.value') AS deposit_account_id,
    JSON_VALUE(deposit_to_account_ref, '$.name') AS deposit_account_name,
    JSON_VALUE(payment_method_ref, '$.name') AS payment_method,
    payment_ref_num AS reference_number,
    private_note,
    TIMESTAMP(JSON_VALUE(meta_data, '$.CreateTime')) AS created_at,
    lastupdatedtime AS updated_at
  FROM quickbooks_raw.payments
)

SELECT
  payment_id,
  customer_id,
  customer_name,
  payment_date,
  currency,
  total_amount,
  unapplied_amount,
  total_amount - unapplied_amount AS applied_amount,
  deposit_account_id,
  deposit_account_name,
  payment_method,
  reference_number,
  private_note,
  created_at,
  updated_at
FROM payments;
