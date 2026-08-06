/* @bruin
name: stripe_reports.monthly_mrr_by_customer
type: bq.sql
description: >
  End-of-month observed recurring revenue by Stripe billing customer and native
  currency. Every customer-currency row in a month uses one global daily
  snapshot date. MRR is gross list-price run rate from active or past-due
  licensed subscriptions, not recognized revenue, cash, or bookings.

materialization:
  type: table
  strategy: truncate+insert

depends:
  - stripe_stage.customer_currency_daily_mrr_snapshot
  - stripe_stage.customers

tags:
  - stripe_reports
  - stripe
  - billing
  - mrr
@bruin */

WITH available_reporting_months AS (
  SELECT
    DATE_TRUNC(snapshot_date, MONTH) AS metric_month,
    MAX(snapshot_date) AS as_of_snapshot_date
  FROM stripe_stage.customer_currency_daily_mrr_snapshot
  GROUP BY 1
),
global_reporting_snapshot_dates AS (
  SELECT
    *,
    LAG(metric_month) OVER (ORDER BY metric_month)
      AS previous_reporting_month
  FROM available_reporting_months
)

SELECT
  snapshots.metric_month,
  snapshots.as_of_snapshot_date,
  snapshots.previous_reporting_month IS NOT NULL
    AND DATE_DIFF(
      snapshots.metric_month,
      snapshots.previous_reporting_month,
      MONTH
    ) = 1 AS has_contiguous_global_prior_snapshot,
  snapshot.stripe_customer_id,
  snapshot.currency,
  customer.crm_account_id,
  customer.customer_segment,
  customer.customer_region,
  customer.sales_owner,
  customer.acquisition_channel,
  snapshot.active_subscription_count,
  snapshot.active_subscription_item_count,
  snapshot.ending_mrr_minor,
  snapshot.ending_mrr_minor * 12 AS run_rate_arr_minor
FROM global_reporting_snapshot_dates AS snapshots
INNER JOIN stripe_stage.customer_currency_daily_mrr_snapshot AS snapshot
  ON snapshots.as_of_snapshot_date = snapshot.snapshot_date
LEFT JOIN stripe_stage.customers AS customer
  ON snapshot.stripe_customer_id = customer.stripe_customer_id;
