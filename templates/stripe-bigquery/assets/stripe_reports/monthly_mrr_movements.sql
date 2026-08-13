/* @bruin
name: stripe_reports.monthly_mrr_movements
type: bq.sql
description: >
  Customer-level MRR movement ledger. Movements are classified after summing all
  subscriptions for a billing customer, preventing internal plan swaps from
  appearing as separate churn and new-business events.

materialization:
  type: table

depends:
  - stripe_reports.monthly_mrr_by_customer

tags:
  - stripe_reports
  - stripe
  - billing
  - mrr
  - movements

columns:
  - name: metric_month
    type: DATE
    description: First day of the reporting month.
    primary_key: true
    checks:
      - name: not_null
  - name: as_of_snapshot_date
    type: DATE
    description: Snapshot date the month's MRR was observed on.
  - name: has_contiguous_global_prior_snapshot
    type: BOOL
    description: Whether a snapshot exists for the immediately preceding month.
  - name: stripe_customer_id
    type: STRING
    description: Stripe billing customer.
    primary_key: true
    checks:
      - name: not_null
  - name: currency
    type: STRING
    description: >
      Native Stripe currency. Movements are classified per currency and are
      never converted.
    primary_key: true
    checks:
      - name: not_null
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
  - name: previous_observed_month
    type: DATE
    description: >
      Previous month in which this customer and currency were observed. It is
      null in the customer's first observed month.
  - name: has_contiguous_prior_snapshot
    type: BOOL
    description: >
      Whether the previous observed month is the immediately preceding month.
      Expansion, contraction, churn, and reactivation are only classified when
      it is true.
  - name: beginning_mrr_minor
    type: NUMERIC
    description: >
      MRR carried in from the previous observed month, in minor units. It is
      zero when no prior observation exists.
  - name: ending_mrr_minor
    type: NUMERIC
    description: Month-end MRR in native-currency minor units.
  - name: net_mrr_change_minor
    type: NUMERIC
    description: >
      Ending less beginning MRR in minor units. It is null when the month cannot
      be reconciled against a prior snapshot.
  - name: new_mrr_minor
    type: NUMERIC
    description: MRR from customers with no prior positive MRR month.
  - name: reactivation_mrr_minor
    type: NUMERIC
    description: MRR from customers returning after a zero-MRR month.
  - name: expansion_mrr_minor
    type: NUMERIC
    description: Increase in MRR for customers who already had positive MRR.
  - name: contraction_mrr_minor
    type: NUMERIC
    description: Decrease in MRR for customers who retained positive MRR.
  - name: churned_mrr_minor
    type: NUMERIC
    description: MRR lost by customers who fell to zero MRR.
  - name: movement_type
    type: STRING
    description: >
      Classified movement for the customer month, one of `new`, `reactivation`,
      `expansion`, `contraction`, `churn`, `no_change`, `first_observed_month`,
      or `snapshot_gap`. The last two mark months that cannot be reconciled.
    checks:
      - name: not_null

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
