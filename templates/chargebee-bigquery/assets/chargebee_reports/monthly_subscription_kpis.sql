/* @bruin
name: chargebee_reports.monthly_subscription_kpis
type: bq.sql
description: >
  Monthly recurring-revenue scorecard per native currency: MRR, ARR run rate,
  the new/expansion/contraction/churn/reactivation waterfall, gross and net
  revenue retention, logo churn, ARPA, and the SaaS quick ratio. Retention
  metrics use only contiguous monthly snapshots and exclude reactivation from
  NRR by policy. Every metric is per currency; no FX conversion is applied.

materialization:
  type: table

depends:
  - chargebee_reports.monthly_mrr_movements

tags:
  - chargebee_reports
  - chargebee
  - billing
  - kpis

columns:
  - name: metric_month
    type: DATE
    description: First day of the reporting month.
    primary_key: true
    checks:
      - name: not_null
  - name: currency_code
    type: STRING
    description: Native currency. Every metric is reported per currency.
    primary_key: true
    checks:
      - name: not_null
  - name: as_of_snapshot_date
    type: DATE
    description: Snapshot date the month's MRR was observed on.
  - name: beginning_mrr_minor
    type: NUMERIC
    description: >
      Sum of beginning MRR in minor units, counting only customer months with a
      contiguous prior snapshot. Denominator of the retention rates.
  - name: ending_mrr_minor
    type: NUMERIC
    description: Sum of month-end MRR in native-currency minor units.
  - name: ending_run_rate_arr_minor
    type: NUMERIC
    description: Ending MRR multiplied by 12, in minor units.
  - name: new_mrr_minor
    type: NUMERIC
    description: MRR added by customers with no prior positive MRR month.
  - name: reactivation_mrr_minor
    type: NUMERIC
    description: MRR added by customers returning after a zero-MRR month.
  - name: expansion_mrr_minor
    type: NUMERIC
    description: MRR added by customers who already had positive MRR.
  - name: contraction_mrr_minor
    type: NUMERIC
    description: MRR lost by customers who retained positive MRR.
  - name: churned_mrr_minor
    type: NUMERIC
    description: MRR lost by customers who fell to zero MRR.
  - name: beginning_active_customer_count
    type: INT64
    description: Customers with positive beginning MRR and a contiguous prior snapshot.
  - name: ending_active_customer_count
    type: INT64
    description: Customers with positive month-end MRR.
  - name: new_customer_count
    type: INT64
    description: Customers classified as new in the month.
  - name: reactivated_customer_count
    type: INT64
    description: Customers classified as reactivation in the month.
  - name: churned_customer_count
    type: INT64
    description: Customers classified as churn in the month.
  - name: average_revenue_per_active_customer_minor
    type: NUMERIC
    description: Ending MRR divided by the ending active customer count, in minor units.
  - name: gross_revenue_churn_rate
    type: NUMERIC
    description: Churned MRR over beginning MRR. Null when beginning MRR cannot be reconciled.
  - name: gross_revenue_retention_rate
    type: NUMERIC
    description: >
      Beginning MRR less contraction and churn, over beginning MRR. Excludes
      expansion by definition.
  - name: net_revenue_retention_rate_excluding_reactivation
    type: NUMERIC
    description: >
      Beginning MRR plus expansion less contraction and churn, over beginning
      MRR. Reactivation is excluded from NRR by policy.
  - name: logo_churn_rate
    type: FLOAT64
    description: Churned customer count over the beginning active customer count.
  - name: quick_ratio
    type: NUMERIC
    description: >
      (New + expansion + reactivation MRR) over (contraction + churned MRR). A
      SaaS growth-efficiency ratio; higher is better and a value at or above 4
      is strong. Null when there is no lost MRR to divide by.
@bruin */

SELECT
  metric_month,
  currency_code,
  MAX(as_of_snapshot_date) AS as_of_snapshot_date,
  SUM(IF(
    has_contiguous_prior_snapshot,
    beginning_mrr_minor,
    CAST(0 AS NUMERIC)
  )) AS beginning_mrr_minor,
  SUM(ending_mrr_minor) AS ending_mrr_minor,
  SUM(ending_mrr_minor) * 12 AS ending_run_rate_arr_minor,
  SUM(new_mrr_minor) AS new_mrr_minor,
  SUM(reactivation_mrr_minor) AS reactivation_mrr_minor,
  SUM(expansion_mrr_minor) AS expansion_mrr_minor,
  SUM(contraction_mrr_minor) AS contraction_mrr_minor,
  SUM(churned_mrr_minor) AS churned_mrr_minor,
  COUNT(DISTINCT IF(
    has_contiguous_prior_snapshot AND beginning_mrr_minor > 0,
    chargebee_customer_id,
    NULL
  )) AS beginning_active_customer_count,
  COUNT(DISTINCT IF(
    ending_mrr_minor > 0,
    chargebee_customer_id,
    NULL
  )) AS ending_active_customer_count,
  COUNT(DISTINCT IF(movement_type = 'new', chargebee_customer_id, NULL))
    AS new_customer_count,
  COUNT(DISTINCT IF(movement_type = 'reactivation', chargebee_customer_id, NULL))
    AS reactivated_customer_count,
  COUNT(DISTINCT IF(movement_type = 'churn', chargebee_customer_id, NULL))
    AS churned_customer_count,
  SAFE_DIVIDE(
    SUM(ending_mrr_minor),
    NULLIF(
      COUNT(DISTINCT IF(ending_mrr_minor > 0, chargebee_customer_id, NULL)),
      0
    )
  ) AS average_revenue_per_active_customer_minor,
  SAFE_DIVIDE(
    SUM(churned_mrr_minor),
    NULLIF(
      SUM(IF(
        has_contiguous_prior_snapshot,
        beginning_mrr_minor,
        CAST(0 AS NUMERIC)
      )),
      0
    )
  ) AS gross_revenue_churn_rate,
  SAFE_DIVIDE(
    SUM(IF(
      has_contiguous_prior_snapshot,
      beginning_mrr_minor,
      CAST(0 AS NUMERIC)
    ))
      - SUM(contraction_mrr_minor)
      - SUM(churned_mrr_minor),
    NULLIF(
      SUM(IF(
        has_contiguous_prior_snapshot,
        beginning_mrr_minor,
        CAST(0 AS NUMERIC)
      )),
      0
    )
  ) AS gross_revenue_retention_rate,
  SAFE_DIVIDE(
    SUM(IF(
      has_contiguous_prior_snapshot,
      beginning_mrr_minor,
      CAST(0 AS NUMERIC)
    ))
      + SUM(expansion_mrr_minor)
      - SUM(contraction_mrr_minor)
      - SUM(churned_mrr_minor),
    NULLIF(
      SUM(IF(
        has_contiguous_prior_snapshot,
        beginning_mrr_minor,
        CAST(0 AS NUMERIC)
      )),
      0
    )
  ) AS net_revenue_retention_rate_excluding_reactivation,
  SAFE_DIVIDE(
    COUNT(DISTINCT IF(movement_type = 'churn', chargebee_customer_id, NULL)),
    NULLIF(
      COUNT(DISTINCT IF(
        has_contiguous_prior_snapshot AND beginning_mrr_minor > 0,
        chargebee_customer_id,
        NULL
      )),
      0
    )
  ) AS logo_churn_rate,
  SAFE_DIVIDE(
    SUM(new_mrr_minor) + SUM(expansion_mrr_minor) + SUM(reactivation_mrr_minor),
    NULLIF(SUM(contraction_mrr_minor) + SUM(churned_mrr_minor), CAST(0 AS NUMERIC))
  ) AS quick_ratio
FROM chargebee_reports.monthly_mrr_movements
GROUP BY 1, 2;
