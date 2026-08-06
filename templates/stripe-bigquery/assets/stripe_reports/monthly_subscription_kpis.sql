/* @bruin
name: stripe_reports.monthly_subscription_kpis
type: bq.sql
description: >
  Monthly recurring-revenue scorecard. Retention metrics use only
  contiguous monthly snapshots and exclude reactivation from NRR by policy.

materialization:
  type: table
  strategy: truncate+insert

depends:
  - stripe_reports.monthly_mrr_movements

tags:
  - stripe_reports
  - stripe
  - billing
  - kpis
@bruin */

SELECT
  metric_month,
  currency,
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
    stripe_customer_id,
    NULL
  )) AS beginning_active_customer_count,
  COUNT(DISTINCT IF(
    ending_mrr_minor > 0,
    stripe_customer_id,
    NULL
  )) AS ending_active_customer_count,
  COUNT(DISTINCT IF(movement_type = 'new', stripe_customer_id, NULL))
    AS new_customer_count,
  COUNT(DISTINCT IF(movement_type = 'reactivation', stripe_customer_id, NULL))
    AS reactivated_customer_count,
  COUNT(DISTINCT IF(movement_type = 'churn', stripe_customer_id, NULL))
    AS churned_customer_count,
  SAFE_DIVIDE(
    SUM(ending_mrr_minor),
    NULLIF(
      COUNT(DISTINCT IF(ending_mrr_minor > 0, stripe_customer_id, NULL)),
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
    SUM(contraction_mrr_minor) + SUM(churned_mrr_minor),
    NULLIF(
      SUM(IF(
        has_contiguous_prior_snapshot,
        beginning_mrr_minor,
        CAST(0 AS NUMERIC)
      )),
      0
    )
  ) AS gross_revenue_retention_loss_rate,
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
    COUNT(DISTINCT IF(movement_type = 'churn', stripe_customer_id, NULL)),
    NULLIF(
      COUNT(DISTINCT IF(
        has_contiguous_prior_snapshot AND beginning_mrr_minor > 0,
        stripe_customer_id,
        NULL
      )),
      0
    )
  ) AS logo_churn_rate
FROM stripe_reports.monthly_mrr_movements
GROUP BY 1, 2;
