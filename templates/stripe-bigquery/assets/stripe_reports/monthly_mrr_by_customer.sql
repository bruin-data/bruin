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

depends:
  - stripe_stage.customer_currency_daily_mrr_snapshot
  - stripe_stage.customers

tags:
  - stripe_reports
  - stripe
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
    description: >
      Latest daily snapshot date available in the month. Every customer-currency
      row in the month is observed as of this one global date.
    checks:
      - name: not_null
  - name: has_contiguous_global_prior_snapshot
    type: BOOL
    description: >
      Whether a snapshot exists for the immediately preceding month. Movement
      and retention metrics are only classified when it is true.
  - name: stripe_customer_id
    type: STRING
    description: Stripe billing customer.
    primary_key: true
    checks:
      - name: not_null
  - name: currency
    type: STRING
    description: >
      Native Stripe currency. Amounts are never converted, so do not sum across
      currencies.
    primary_key: true
    checks:
      - name: not_null
  - name: customer_name
    type: STRING
    description: Customer's full or business name carried from the customer record.
  - name: crm_account_id
    type: STRING
    description: CRM account identifier carried from the customer metadata.
  - name: customer_segment
    type: STRING
    description: Customer segment carried from the customer metadata.
  - name: customer_region
    type: STRING
    description: Customer region carried from the customer metadata.
  - name: sales_owner
    type: STRING
    description: Sales owner carried from the customer metadata.
  - name: acquisition_channel
    type: STRING
    description: Acquisition channel carried from the customer metadata.
  - name: active_subscription_count
    type: INT64
    description: Subscriptions with at least one MRR-eligible item at month end.
  - name: active_subscription_item_count
    type: INT64
    description: MRR-eligible subscription items at month end.
  - name: ending_mrr_minor
    type: NUMERIC
    description: >
      Month-end gross list-price MRR in native-currency minor units. It is not
      recognized revenue, cash, or bookings.
  - name: run_rate_arr_minor
    type: NUMERIC
    description: >
      Annualized run rate, `ending_mrr_minor` multiplied by 12. It is a run
      rate, not a contracted or recognized annual amount.
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
  customer.customer_name,
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
