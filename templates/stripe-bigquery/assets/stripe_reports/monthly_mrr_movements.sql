/* @bruin
name: stripe_reports.monthly_mrr_movements
type: bq.sql
description: >
  Customer-level MRR movement ledger. Movements are classified after summing all
  subscriptions for a billing customer, preventing internal plan swaps from
  appearing as separate churn and new-business events.

materialization:
  type: table
  strategy: truncate+insert

depends:
  - stripe_reports.monthly_mrr_by_customer

tags:
  - stripe_reports
  - stripe
  - billing
  - mrr
  - movements

custom_checks:
  - name: mrr movements roll forward to ending mrr
    description: >
      For reconcilable customer months, aggregate ending MRR must equal
      beginning MRR plus classified new, reactivation, expansion, contraction,
      and churn movements in the same native currency.
    query: |
      WITH monthly_roll_forward AS (
        SELECT
          metric_month,
          currency,
          SUM(beginning_mrr_minor) AS beginning_mrr_minor,
          SUM(ending_mrr_minor) AS ending_mrr_minor,
          SUM(net_mrr_change_minor) AS recorded_net_mrr_change_minor,
          SUM(
            new_mrr_minor
            + reactivation_mrr_minor
            + expansion_mrr_minor
            - contraction_mrr_minor
            - churned_mrr_minor
          ) AS classified_net_mrr_change_minor
        FROM {{ this }}
        WHERE net_mrr_change_minor IS NOT NULL
        GROUP BY 1, 2
      )
      SELECT COUNT(*)
      FROM monthly_roll_forward
      WHERE ending_mrr_minor IS DISTINCT FROM (
        beginning_mrr_minor + classified_net_mrr_change_minor
      )
        OR recorded_net_mrr_change_minor IS DISTINCT FROM
          classified_net_mrr_change_minor
    value: 0
@bruin */

WITH customer_mrr_history AS (
  SELECT
    *,
    LAG(metric_month) OVER customer_window AS previous_observed_month,
    LAG(ending_mrr_minor) OVER customer_window AS previous_mrr_minor,
    COUNTIF(ending_mrr_minor > 0) OVER (
      customer_window
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS prior_positive_mrr_month_count
  FROM stripe_reports.monthly_mrr_by_customer
  WINDOW customer_window AS (
    PARTITION BY stripe_customer_id, currency
    ORDER BY metric_month
  )
),
movement_base AS (
  SELECT
    *,
    previous_observed_month IS NOT NULL
      AND DATE_DIFF(metric_month, previous_observed_month, MONTH) = 1
      AS has_contiguous_prior_snapshot,
    COALESCE(previous_mrr_minor, CAST(0 AS NUMERIC)) AS beginning_mrr_minor
  FROM customer_mrr_history
)

SELECT
  metric_month,
  as_of_snapshot_date,
  has_contiguous_global_prior_snapshot,
  stripe_customer_id,
  currency,
  crm_account_id,
  customer_segment,
  customer_region,
  sales_owner,
  acquisition_channel,
  previous_observed_month,
  has_contiguous_prior_snapshot,
  beginning_mrr_minor,
  ending_mrr_minor,
  CASE
    WHEN has_contiguous_prior_snapshot
      THEN ending_mrr_minor - beginning_mrr_minor
    WHEN previous_observed_month IS NULL
      AND has_contiguous_global_prior_snapshot
      THEN ending_mrr_minor
    ELSE NULL
  END AS net_mrr_change_minor,
  CASE
    WHEN (
      has_contiguous_prior_snapshot
      OR (
        previous_observed_month IS NULL
        AND has_contiguous_global_prior_snapshot
      )
    )
      AND beginning_mrr_minor = 0
      AND ending_mrr_minor > 0
      AND prior_positive_mrr_month_count = 0
      THEN ending_mrr_minor
    ELSE CAST(0 AS NUMERIC)
  END AS new_mrr_minor,
  CASE
    WHEN has_contiguous_prior_snapshot
      AND beginning_mrr_minor = 0
      AND ending_mrr_minor > 0
      AND prior_positive_mrr_month_count > 0
      THEN ending_mrr_minor
    ELSE CAST(0 AS NUMERIC)
  END AS reactivation_mrr_minor,
  CASE
    WHEN has_contiguous_prior_snapshot
      AND beginning_mrr_minor > 0
      AND ending_mrr_minor > beginning_mrr_minor
      THEN ending_mrr_minor - beginning_mrr_minor
    ELSE CAST(0 AS NUMERIC)
  END AS expansion_mrr_minor,
  CASE
    WHEN has_contiguous_prior_snapshot
      AND beginning_mrr_minor > ending_mrr_minor
      AND ending_mrr_minor > 0
      THEN beginning_mrr_minor - ending_mrr_minor
    ELSE CAST(0 AS NUMERIC)
  END AS contraction_mrr_minor,
  CASE
    WHEN has_contiguous_prior_snapshot
      AND beginning_mrr_minor > 0
      AND ending_mrr_minor = 0
      THEN beginning_mrr_minor
    ELSE CAST(0 AS NUMERIC)
  END AS churned_mrr_minor,
  CASE
    WHEN previous_observed_month IS NULL
      AND has_contiguous_global_prior_snapshot
      AND ending_mrr_minor > 0 THEN 'new'
    WHEN previous_observed_month IS NULL THEN 'first_observed_month'
    WHEN NOT has_contiguous_prior_snapshot THEN 'snapshot_gap'
    WHEN beginning_mrr_minor = 0
      AND ending_mrr_minor > 0
      AND prior_positive_mrr_month_count = 0 THEN 'new'
    WHEN beginning_mrr_minor = 0
      AND ending_mrr_minor > 0
      AND prior_positive_mrr_month_count > 0 THEN 'reactivation'
    WHEN beginning_mrr_minor > 0
      AND ending_mrr_minor > beginning_mrr_minor THEN 'expansion'
    WHEN beginning_mrr_minor > ending_mrr_minor
      AND ending_mrr_minor > 0 THEN 'contraction'
    WHEN beginning_mrr_minor > 0
      AND ending_mrr_minor = 0 THEN 'churn'
    ELSE 'no_change'
  END AS movement_type
FROM movement_base;
