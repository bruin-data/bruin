/* @bruin
name: chargebee_reports.failed_payment_dunning
type: bq.sql
description: >
  Payment reliability and involuntary-churn risk per native currency, from the
  transactions ledger. Reports the payment success rate and splits failed
  payments into those still inside the dunning retry window (recoverable) and
  older unrecovered failures (at-risk, the likely involuntary-churn amount). The
  retry window is set by the `dunning_retry_window_days` pipeline variable
  (default 30). Chargebee flags voluntary vs involuntary churn but does not
  expose a tunable dunning-recovery view. Amounts are native currency in minor
  units, measured as of the pipeline end date.

materialization:
  type: table

depends:
  - chargebee_stage.transactions
  - chargebee_raw.transaction

tags:
  - chargebee_reports
  - chargebee
  - billing
  - dunning

columns:
  - name: currency_code
    type: STRING
    description: Native currency. Do not sum across currencies.
    primary_key: true
    checks:
      - name: not_null
  - name: as_of_date
    type: DATE
    description: Pipeline end date the window was measured against.
    checks:
      - name: not_null
  - name: payment_attempt_count
    type: INT64
    description: Number of payment transactions attempted.
  - name: successful_payment_count
    type: INT64
    description: Number of successful payments.
  - name: failed_payment_count
    type: INT64
    description: Number of failed payments (failure, timeout, or late_failure).
  - name: payment_success_rate
    type: FLOAT64
    description: Successful payments over attempted payments.
  - name: failed_amount_minor
    type: NUMERIC
    description: Total amount of failed payments, in minor units.
  - name: recoverable_failed_amount_minor
    type: NUMERIC
    description: >
      Failed-payment amount still inside the dunning retry window, in minor
      units. Considered recoverable.
  - name: at_risk_failed_amount_minor
    type: NUMERIC
    description: >
      Failed-payment amount older than the dunning retry window, in minor units.
      The likely involuntary-churn exposure.
  - name: recoverable_failed_count
    type: INT64
    description: Count of failed payments still inside the retry window.
  - name: recovered_failed_count
    type: INT64
    description: Failed payments later recovered by a successful payment on the same invoice.
  - name: at_risk_failed_count
    type: INT64
    description: Count of failed payments older than the retry window.
@bruin */

WITH bounds AS (
  SELECT
    DATE('{{ end_date }}') AS as_of_date,
    TIMESTAMP(DATE_SUB(
      DATE('{{ end_date }}'),
      INTERVAL {{ var.dunning_retry_window_days }} DAY
    )) AS retry_window_start
),
raw_invoice_links AS (
  SELECT
    id AS chargebee_transaction_id,
    JSON_VALUE(linked_invoices, '$[0].invoice_id') AS chargebee_invoice_id
  FROM chargebee_raw.transaction
),
payment_outcomes AS (
  SELECT
    failed.*,
    failed_invoice.chargebee_invoice_id,
    EXISTS(
      SELECT 1
      FROM chargebee_stage.transactions AS successful
      INNER JOIN raw_invoice_links AS successful_invoice
        ON successful.chargebee_transaction_id = successful_invoice.chargebee_transaction_id
      WHERE successful.is_payment
        AND successful.is_successful
        AND successful.currency_code = failed.currency_code
        AND successful_invoice.chargebee_invoice_id IS NOT NULL
        AND successful_invoice.chargebee_invoice_id = failed_invoice.chargebee_invoice_id
        AND successful.transaction_at > failed.transaction_at
    ) AS was_recovered
  FROM chargebee_stage.transactions AS failed
  LEFT JOIN raw_invoice_links AS failed_invoice
    ON failed.chargebee_transaction_id = failed_invoice.chargebee_transaction_id
)

SELECT
  transaction.currency_code,
  bounds.as_of_date,
  COUNTIF(transaction.is_payment) AS payment_attempt_count,
  COUNTIF(transaction.is_payment AND transaction.is_successful) AS successful_payment_count,
  COUNTIF(transaction.is_failed_payment) AS failed_payment_count,
  SAFE_DIVIDE(
    COUNTIF(transaction.is_payment AND transaction.is_successful),
    NULLIF(COUNTIF(transaction.is_payment), 0)
  ) AS payment_success_rate,
  SUM(IF(transaction.is_failed_payment, transaction.amount_minor, CAST(0 AS NUMERIC)))
    AS failed_amount_minor,
  SUM(IF(
    transaction.is_failed_payment AND NOT transaction.was_recovered
      AND transaction.transaction_at >= bounds.retry_window_start,
    transaction.amount_minor,
    CAST(0 AS NUMERIC)
  )) AS recoverable_failed_amount_minor,
  SUM(IF(
    transaction.is_failed_payment AND NOT transaction.was_recovered
      AND transaction.transaction_at < bounds.retry_window_start,
    transaction.amount_minor,
    CAST(0 AS NUMERIC)
  )) AS at_risk_failed_amount_minor,
  COUNTIF(
    transaction.is_failed_payment AND NOT transaction.was_recovered
      AND transaction.transaction_at >= bounds.retry_window_start
  ) AS recoverable_failed_count,
  COUNTIF(transaction.is_failed_payment AND transaction.was_recovered)
    AS recovered_failed_count,
  COUNTIF(
    transaction.is_failed_payment AND NOT transaction.was_recovered
      AND transaction.transaction_at < bounds.retry_window_start
  ) AS at_risk_failed_count
FROM payment_outcomes AS transaction
CROSS JOIN bounds
WHERE transaction.currency_code IS NOT NULL
GROUP BY 1, 2;
