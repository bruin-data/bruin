/* @bruin
name: chargebee_stage.transactions
type: bq.sql
description: >
  Conformed Chargebee payment transactions: payments, refunds, and reversals.
  Unix timestamps are cast to TIMESTAMP and amounts are kept in native-currency
  minor units. Boolean flags classify successful payments, refunds, and failed
  payments so downstream models can measure collections and involuntary-churn
  risk. Deleted transactions are filtered out.

materialization:
  type: table

depends:
  - chargebee_raw.transaction

tags:
  - chargebee_stage
  - chargebee
  - billing
  - transactions

columns:
  - name: chargebee_transaction_id
    type: STRING
    description: Chargebee transaction identifier and natural key.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: chargebee_customer_id
    type: STRING
    description: Customer the transaction belongs to.
  - name: chargebee_subscription_id
    type: STRING
    description: Subscription linked to the transaction, when applicable.
  - name: source_system
    type: STRING
    description: Constant `chargebee`.
    checks:
      - name: not_null
  - name: transaction_type
    type: STRING
    description: >
      Transaction type, one of authorization, payment, refund, payment_reversal.
  - name: transaction_status
    type: STRING
    description: >
      Status, one of in_progress, success, voided, failure, timeout,
      needs_attention, late_failure.
  - name: gateway
    type: STRING
    description: Payment gateway that processed the transaction.
  - name: payment_method
    type: STRING
    description: Payment method used for the transaction.
  - name: is_payment
    type: BOOL
    description: Whether the transaction is a payment (versus refund/authorization).
  - name: is_refund
    type: BOOL
    description: Whether the transaction is a refund.
  - name: is_successful
    type: BOOL
    description: Whether the transaction succeeded.
  - name: is_failed_payment
    type: BOOL
    description: >
      Whether this is a payment that failed (status failure, timeout, or
      late_failure), the basis for involuntary-churn analysis.
  - name: currency_code
    type: STRING
    description: Native currency the transaction was made in.
  - name: amount_minor
    type: NUMERIC
    description: Transaction amount in native-currency minor units.
  - name: transaction_at
    type: TIMESTAMP
    description: When the transaction occurred.
  - name: transaction_updated_at
    type: TIMESTAMP
    description: When the transaction was last modified.
@bruin */

SELECT
  id AS chargebee_transaction_id,
  customer_id AS chargebee_customer_id,
  subscription_id AS chargebee_subscription_id,
  'chargebee' AS source_system,
  type AS transaction_type,
  status AS transaction_status,
  gateway,
  payment_method,
  type = 'payment' AS is_payment,
  type = 'refund' AS is_refund,
  status = 'success' AS is_successful,
  type = 'payment' AND status IN ('failure', 'timeout', 'late_failure') AS is_failed_payment,
  currency_code,
  SAFE_CAST(amount AS NUMERIC) AS amount_minor,
  TIMESTAMP_SECONDS(SAFE_CAST(date AS INT64)) AS transaction_at,
  TIMESTAMP_SECONDS(SAFE_CAST(updated_at AS INT64)) AS transaction_updated_at
FROM chargebee_raw.transaction
WHERE NOT COALESCE(deleted, FALSE);
