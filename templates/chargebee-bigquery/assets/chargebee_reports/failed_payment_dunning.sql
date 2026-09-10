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
    transaction.is_failed_payment AND transaction.transaction_at >= bounds.retry_window_start,
    transaction.amount_minor,
    CAST(0 AS NUMERIC)
  )) AS recoverable_failed_amount_minor,
  SUM(IF(
    transaction.is_failed_payment AND transaction.transaction_at < bounds.retry_window_start,
    transaction.amount_minor,
    CAST(0 AS NUMERIC)
  )) AS at_risk_failed_amount_minor,
  COUNTIF(
    transaction.is_failed_payment AND transaction.transaction_at >= bounds.retry_window_start
  ) AS recoverable_failed_count,
  COUNTIF(
    transaction.is_failed_payment AND transaction.transaction_at < bounds.retry_window_start
  ) AS at_risk_failed_count
FROM chargebee_stage.transactions AS transaction
CROSS JOIN bounds
WHERE transaction.currency_code IS NOT NULL
GROUP BY 1, 2;
