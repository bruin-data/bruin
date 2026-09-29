/* @bruin
name: chargebee_reports.revenue_concentration
type: bq.sql
description: >
  Customer revenue concentration at the latest MRR snapshot, per native
  currency: the top-N customers by MRR with each one's share of the currency's
  total, plus the top-1, top-5, and top-N shares. High concentration is a
  well-known risk that Chargebee's native reports do not surface. The number of
  ranked customers is set by the `concentration_top_n` pipeline variable
  (default 10); the top-1 and top-5 shares are always emitted. Amounts are
  native currency in minor units.

materialization:
  type: table

depends:
  - chargebee_stage.customer_currency_daily_mrr_snapshot
  - chargebee_stage.customers

tags:
  - chargebee_reports
  - chargebee
  - billing
  - concentration

columns:
  - name: as_of_snapshot_date
    type: DATE
    description: Snapshot date the concentration was measured on.
    checks:
      - name: not_null
  - name: currency_code
    type: STRING
    description: Native currency. Do not sum across currencies.
    primary_key: true
    checks:
      - name: not_null
  - name: customer_rank
    type: INT64
    description: Rank of the customer by MRR within the currency, 1 being the largest.
    primary_key: true
    checks:
      - name: not_null
  - name: chargebee_customer_id
    type: STRING
    description: The ranked customer.
  - name: customer_name
    type: STRING
    description: Customer's full or business name.
  - name: company_name
    type: STRING
    description: Company name carried from the customer record.
  - name: ending_mrr_minor
    type: NUMERIC
    description: The customer's MRR at the snapshot, in minor units.
  - name: pct_of_currency_mrr
    type: FLOAT64
    description: The customer's MRR as a fraction of the currency's total MRR.
  - name: cumulative_pct_of_currency_mrr
    type: FLOAT64
    description: Cumulative fraction of currency MRR down to and including this rank.
  - name: total_currency_mrr_minor
    type: NUMERIC
    description: Total MRR across all customers in this currency, in minor units.
  - name: active_customer_count
    type: INT64
    description: Number of customers with positive MRR in this currency.
  - name: top_1_share
    type: FLOAT64
    description: Fraction of currency MRR held by the single largest customer.
  - name: top_5_share
    type: FLOAT64
    description: Fraction of currency MRR held by the five largest customers.
  - name: top_n_share
    type: FLOAT64
    description: Fraction of currency MRR held by the top-N (concentration_top_n) customers.
@bruin */

WITH latest_snapshot AS (
  SELECT
    snapshot.chargebee_customer_id,
    snapshot.currency_code,
    snapshot.ending_mrr_minor,
    snapshot.snapshot_date AS as_of_snapshot_date
  FROM chargebee_stage.customer_currency_daily_mrr_snapshot AS snapshot
  INNER JOIN (
    SELECT MAX(snapshot_date) AS as_of_snapshot_date
    FROM chargebee_stage.customer_currency_daily_mrr_snapshot
  ) AS latest
    ON snapshot.snapshot_date = latest.as_of_snapshot_date
  WHERE snapshot.ending_mrr_minor > 0
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER currency_window AS customer_rank,
    SUM(ending_mrr_minor) OVER (PARTITION BY currency_code) AS total_currency_mrr_minor,
    COUNT(*) OVER (PARTITION BY currency_code) AS active_customer_count,
    SUM(ending_mrr_minor) OVER (
      currency_window ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_mrr_minor
  FROM latest_snapshot
  WINDOW currency_window AS (
    PARTITION BY currency_code ORDER BY ending_mrr_minor DESC, chargebee_customer_id
  )
),
concentrated AS (
  SELECT
    *,
    SUM(IF(customer_rank = 1, ending_mrr_minor, CAST(0 AS NUMERIC)))
      OVER (PARTITION BY currency_code) AS top_1_mrr_minor,
    SUM(IF(customer_rank <= 5, ending_mrr_minor, CAST(0 AS NUMERIC)))
      OVER (PARTITION BY currency_code) AS top_5_mrr_minor,
    SUM(IF(
      customer_rank <= {{ var.concentration_top_n }},
      ending_mrr_minor,
      CAST(0 AS NUMERIC)
    )) OVER (PARTITION BY currency_code) AS top_n_mrr_minor
  FROM ranked
)

SELECT
  concentrated.as_of_snapshot_date,
  concentrated.currency_code,
  concentrated.customer_rank,
  concentrated.chargebee_customer_id,
  customer.customer_name,
  customer.company_name,
  concentrated.ending_mrr_minor,
  SAFE_DIVIDE(concentrated.ending_mrr_minor, concentrated.total_currency_mrr_minor)
    AS pct_of_currency_mrr,
  SAFE_DIVIDE(concentrated.cumulative_mrr_minor, concentrated.total_currency_mrr_minor)
    AS cumulative_pct_of_currency_mrr,
  concentrated.total_currency_mrr_minor,
  concentrated.active_customer_count,
  SAFE_DIVIDE(concentrated.top_1_mrr_minor, concentrated.total_currency_mrr_minor) AS top_1_share,
  SAFE_DIVIDE(concentrated.top_5_mrr_minor, concentrated.total_currency_mrr_minor) AS top_5_share,
  SAFE_DIVIDE(concentrated.top_n_mrr_minor, concentrated.total_currency_mrr_minor) AS top_n_share
FROM concentrated
LEFT JOIN chargebee_stage.customers AS customer
  ON concentrated.chargebee_customer_id = customer.chargebee_customer_id
WHERE concentrated.customer_rank <= {{ var.concentration_top_n }};
