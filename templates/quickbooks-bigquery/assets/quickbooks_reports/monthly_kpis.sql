/* @bruin
name: quickbooks_reports.monthly_kpis
type: bq.sql
owner: finance@example.com
description: >
  One row per month with the headline SaaS and finance numbers: MRR and ARR,
  paying customers, the MRR bridge (new, expansion, contraction, churn,
  reactivation), revenue, gross margin, operating expenses, cash collected,
  and net burn. Revenue and costs are on an accrual basis from
  `quickbooks_reports.monthly_pnl`; net burn is total costs less cash
  collected and other income, an approximation of cash burn because card
  charges and bills count when incurred, not when paid. Only invoices,
  purchases, and bills are loaded, so payroll booked as journal entries and
  revenue booked as sales receipts or deposits are missing. Only complete
  months are included, and the latest month is flagged preliminary until
  `close_days` days after it ends.

materialization:
  type: table

depends:
  - quickbooks_reports.monthly_pnl
  - quickbooks_reports.customer_mrr_movements
  - quickbooks_reports.monthly_collections

tags:
  - quickbooks_reports
  - quickbooks
  - accounting
  - kpis

domains:
  - finance
  - revenue

meta:
  grain: one row per month
  answers: >
    What is our MRR, ARR, and growth? How many customers did we add or lose?
    What is our gross margin and burn?
  contains_pii: "false"
  agent_notes: >
    Start here for any month-level question. Rates are 0 to 1. Say when a
    month is_preliminary: its costs may still be coming in. The first month
    of data has opening MRR, so its growth and new-customer numbers are not
    meaningful. Journal entries, sales receipts, and deposits are not loaded;
    if payroll or revenue looks too low, say that may be why.

columns:
  - name: month
    type: DATE
    description: First day of the month.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: is_preliminary
    type: BOOL
    description: >
      Whether the month ended less than `close_days` days before the run's
      end date, so bills and card charges may still be missing.
    checks:
      - name: not_null
  - name: mrr
    type: NUMERIC
    description: Monthly recurring revenue invoiced in the month.
    checks:
      - name: not_null
      - name: non_negative
  - name: arr
    type: NUMERIC
    description: Annualized run rate, `mrr * 12`.
    checks:
      - name: not_null
  - name: paying_customers
    type: INT64
    description: Customers with MRR in the month.
    checks:
      - name: not_null
  - name: new_customers
    type: INT64
    description: Customers whose first month with MRR was this month.
    checks:
      - name: not_null
  - name: churned_customers
    type: INT64
    description: Customers with MRR last month and none this month.
    checks:
      - name: not_null
  - name: new_mrr
    type: NUMERIC
    description: MRR from new customers.
  - name: expansion_mrr
    type: NUMERIC
    description: MRR added by existing customers that increased.
  - name: contraction_mrr
    type: NUMERIC
    description: MRR lost by existing customers that decreased, as a negative number.
  - name: churned_mrr
    type: NUMERIC
    description: MRR lost from churned customers, as a negative number.
  - name: reactivation_mrr
    type: NUMERIC
    description: MRR from customers returning after a gap.
  - name: net_new_mrr
    type: NUMERIC
    description: Change in MRR from the month before.
    checks:
      - name: not_null
  - name: mrr_growth_rate
    type: FLOAT64
    description: '`net_new_mrr` over the previous month''s MRR.'
  - name: customer_churn_rate
    type: FLOAT64
    description: '`churned_customers` over the previous month''s paying customers.'
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: revenue
    type: NUMERIC
    description: Total invoiced revenue, recurring and one-time, net of discounts.
    checks:
      - name: not_null
  - name: cost_of_revenue
    type: NUMERIC
    description: Cost of goods sold, such as hosting and support.
    checks:
      - name: not_null
  - name: gross_profit
    type: NUMERIC
    description: '`revenue - cost_of_revenue`.'
    checks:
      - name: not_null
  - name: gross_margin
    type: FLOAT64
    description: '`gross_profit / revenue`.'
  - name: operating_expenses
    type: NUMERIC
    description: Operating expenses, such as payroll, software, and marketing.
    checks:
      - name: not_null
  - name: operating_income
    type: NUMERIC
    description: '`gross_profit - operating_expenses`.'
    checks:
      - name: not_null
  - name: net_income
    type: NUMERIC
    description: Operating income plus other income, less other expenses.
    checks:
      - name: not_null
  - name: cash_collected
    type: NUMERIC
    description: Customer payments applied to invoices in the month.
    checks:
      - name: not_null
  - name: net_burn
    type: NUMERIC
    description: >
      All costs less cash collected and other income; positive when the
      company spends more than it brings in.
    checks:
      - name: not_null

custom_checks:
  - name: mrr matches the customer movements
    query: |
      SELECT COUNTIF(ABS(kpi.mrr - movement.mrr) > 0.01)
      FROM {{ this }} AS kpi
      JOIN (
        SELECT month, SUM(mrr) AS mrr
        FROM quickbooks_reports.customer_mrr_movements
        GROUP BY month
      ) AS movement
        USING (month)
    value: 0
  - name: revenue months have MRR
    description: >
      Non-blocking: months with revenue but no MRR, which usually means the
      subscription income account is not marked is_recurring_revenue in
      account_mapping.csv.
    query: |
      SELECT COUNTIF(revenue > 0 AND mrr = 0)
      FROM {{ this }}
    value: 0
    blocking: false
  - name: gross margin is within bounds
    description: Non-blocking signal that a month's gross margin is negative or above 100%.
    query: |
      SELECT COUNTIF(gross_margin < 0 OR gross_margin > 1)
      FROM {{ this }}
    value: 0
    blocking: false
@bruin */

WITH pnl AS (
  SELECT
    month,
    SUM(IF(pnl_group = 'revenue', amount, 0)) AS revenue,
    SUM(IF(pnl_group = 'cogs', amount, 0)) AS cost_of_revenue,
    SUM(IF(pnl_group = 'operating_expense', amount, 0)) AS operating_expenses,
    SUM(IF(pnl_group = 'other_income', amount, 0)) AS other_income,
    SUM(IF(pnl_group = 'other_expense', amount, 0)) AS other_expenses
  FROM quickbooks_reports.monthly_pnl
  GROUP BY month
),

mrr AS (
  SELECT
    month,
    SUM(mrr) AS mrr,
    COUNTIF(mrr > 0) AS paying_customers,
    COUNTIF(movement = 'new') AS new_customers,
    COUNTIF(movement = 'churn') AS churned_customers,
    SUM(IF(movement = 'new', mrr_change, 0)) AS new_mrr,
    SUM(IF(movement = 'expansion', mrr_change, 0)) AS expansion_mrr,
    SUM(IF(movement = 'contraction', mrr_change, 0)) AS contraction_mrr,
    SUM(IF(movement = 'churn', mrr_change, 0)) AS churned_mrr,
    SUM(IF(movement = 'reactivation', mrr_change, 0)) AS reactivation_mrr,
    SUM(mrr_change) AS net_new_mrr
  FROM quickbooks_reports.customer_mrr_movements
  GROUP BY month
),

combined AS (
  SELECT
    month,
    IFNULL(mrr.mrr, 0) AS mrr,
    IFNULL(mrr.paying_customers, 0) AS paying_customers,
    IFNULL(mrr.new_customers, 0) AS new_customers,
    IFNULL(mrr.churned_customers, 0) AS churned_customers,
    mrr.new_mrr,
    mrr.expansion_mrr,
    mrr.contraction_mrr,
    mrr.churned_mrr,
    mrr.reactivation_mrr,
    IFNULL(mrr.net_new_mrr, 0) AS net_new_mrr,
    pnl.revenue,
    pnl.cost_of_revenue,
    pnl.operating_expenses,
    pnl.other_income,
    pnl.other_expenses,
    IFNULL(collections.collected_amount, 0) AS cash_collected
  FROM pnl
  LEFT JOIN mrr USING (month)
  LEFT JOIN quickbooks_reports.monthly_collections AS collections USING (month)
)

SELECT
  month,
  DATE_DIFF(DATE('{{ end_date }}'), LAST_DAY(month), DAY) < {{ var.close_days }} AS is_preliminary,
  ROUND(mrr, 2) AS mrr,
  ROUND(mrr * 12, 2) AS arr,
  paying_customers,
  new_customers,
  churned_customers,
  new_mrr,
  expansion_mrr,
  contraction_mrr,
  churned_mrr,
  reactivation_mrr,
  net_new_mrr,
  ROUND(SAFE_DIVIDE(
    CAST(net_new_mrr AS FLOAT64),
    CAST(LAG(mrr) OVER (ORDER BY month) AS FLOAT64)
  ), 4) AS mrr_growth_rate,
  ROUND(SAFE_DIVIDE(
    churned_customers,
    LAG(paying_customers) OVER (ORDER BY month)
  ), 4) AS customer_churn_rate,
  ROUND(revenue, 2) AS revenue,
  ROUND(cost_of_revenue, 2) AS cost_of_revenue,
  ROUND(revenue - cost_of_revenue, 2) AS gross_profit,
  ROUND(SAFE_DIVIDE(CAST(revenue - cost_of_revenue AS FLOAT64), CAST(revenue AS FLOAT64)), 4) AS gross_margin,
  ROUND(operating_expenses, 2) AS operating_expenses,
  ROUND(revenue - cost_of_revenue - operating_expenses, 2) AS operating_income,
  ROUND(revenue - cost_of_revenue - operating_expenses + other_income - other_expenses, 2) AS net_income,
  ROUND(cash_collected, 2) AS cash_collected,
  ROUND(cost_of_revenue + operating_expenses + other_expenses - cash_collected - other_income, 2) AS net_burn
FROM combined;
