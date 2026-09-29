/* @bruin
name: chargebee_reports.monthly_mrr_by_customer
type: bq.sql
description: >
  End-of-month MRR by Chargebee customer and native currency, read from the
  daily MRR snapshot. Every customer-currency row in a month uses one global
  daily snapshot date (the latest available in that month). Customer name,
  company, and acquisition channel are carried through so MRR can be sliced by
  segment and later joined to CRM. MRR is a run rate from MRR-eligible
  subscriptions, not recognized revenue, cash, or bookings. Amounts are native
  currency and must not be summed across currencies.

materialization:
  type: table

depends:
  - chargebee_stage.customer_currency_daily_mrr_snapshot
  - chargebee_stage.customers

tags:
  - chargebee_reports
  - chargebee
  - billing
  - mrr

columns:
  - name: metric_month
    type: DATE
    description: First day of the reporting month.
    primary_key: true
    checks:
      - name: not_null
  - name: as_of_snapshot_date
    type: DATE
    description: Latest daily snapshot date available in the month.
    checks:
      - name: not_null
  - name: has_contiguous_global_prior_snapshot
    type: BOOL
    description: >
      Whether a snapshot exists for the immediately preceding month. Movement
      and retention metrics are only classified when it is true.
  - name: chargebee_customer_id
    type: STRING
    description: Chargebee billing customer.
    primary_key: true
    checks:
      - name: not_null
  - name: currency_code
    type: STRING
    description: Native currency. Amounts are never converted; do not sum across currencies.
    primary_key: true
    checks:
      - name: not_null
  - name: customer_name
    type: STRING
    description: Customer's full or business name carried from the customer record.
  - name: company_name
    type: STRING
    description: Company name carried from the customer record, a CRM join key.
  - name: acquisition_channel
    type: STRING
    description: Acquisition channel carried from the customer record.
  - name: active_subscription_count
    type: INT64
    description: MRR-eligible subscriptions for the customer at month end.
  - name: ending_mrr_minor
    type: NUMERIC
    description: Month-end MRR in native-currency minor units.
  - name: run_rate_arr_minor
    type: NUMERIC
    description: Annualized run rate, ending MRR multiplied by 12, in minor units.
@bruin */

WITH available_reporting_months AS (
  SELECT
    DATE_TRUNC(snapshot_date, MONTH) AS metric_month,
    MAX(snapshot_date) AS as_of_snapshot_date
  FROM chargebee_stage.customer_currency_daily_mrr_snapshot
  GROUP BY 1
),
global_reporting_snapshot_dates AS (
  SELECT
    *,
    LAG(metric_month) OVER (ORDER BY metric_month) AS previous_reporting_month
  FROM available_reporting_months
)

SELECT
  months.metric_month,
  months.as_of_snapshot_date,
  months.previous_reporting_month IS NOT NULL
    AND DATE_DIFF(
      months.metric_month,
      months.previous_reporting_month,
      MONTH
    ) = 1 AS has_contiguous_global_prior_snapshot,
  snapshot.chargebee_customer_id,
  snapshot.currency_code,
  customer.customer_name,
  customer.company_name,
  customer.acquisition_channel,
  snapshot.active_subscription_count,
  snapshot.ending_mrr_minor,
  snapshot.ending_mrr_minor * 12 AS run_rate_arr_minor
FROM global_reporting_snapshot_dates AS months
INNER JOIN chargebee_stage.customer_currency_daily_mrr_snapshot AS snapshot
  ON months.as_of_snapshot_date = snapshot.snapshot_date
LEFT JOIN chargebee_stage.customers AS customer
  ON snapshot.chargebee_customer_id = customer.chargebee_customer_id;
