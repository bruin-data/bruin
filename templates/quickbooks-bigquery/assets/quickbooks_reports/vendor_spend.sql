/* @bruin
name: quickbooks_reports.vendor_spend
type: bq.sql
owner: finance@example.com
description: >
  Spend per vendor across purchases and bills, with recency, how many months
  the vendor was paid, its main report line, an estimated annual cost, and a
  recurring flag for monthly and annual subscriptions. Use it to review tool sprawl and vendor costs.
  Only lines on income-statement accounts are counted, so card payoffs and
  other balance sheet postings are excluded. Lines without a payee are left
  out. Windows are measured back from the run's end date.

materialization:
  type: table

depends:
  - quickbooks_stage.expense_lines
  - quickbooks_stage.account_categories

tags:
  - quickbooks_reports
  - quickbooks
  - accounting
  - vendors
  - expenses

domains:
  - finance
  - procurement

meta:
  grain: one row per payee
  answers: >
    Who are our biggest vendors? Which SaaS tools do we pay for every month,
    and what do they cost per year? Which vendors are new?
  contains_pii: "true"
  pii_note: Payees can be people, such as contractors or employees.
  agent_notes: >
    Filter primary_category = 'Software' and is_recurring for SaaS tool
    sprawl; estimated_annual_cost uses the last three months for monthly
    vendors and the last 12 months otherwise. Category names come from
    account_mapping.csv, so list DISTINCT primary_category first.

columns:
  - name: payee_id
    type: STRING
    description: QuickBooks payee identifier; joins to `quickbooks_stage.vendors` for vendor payees.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: payee_name
    type: STRING
    description: Payee display name.
  - name: payee_type
    type: STRING
    description: '`Vendor`, `Customer`, or `Employee`.'
  - name: primary_category
    type: STRING
    description: Report line with the most spend for the payee, for example `Software`.
  - name: total_spend
    type: NUMERIC
    description: All-time spend with the payee.
    checks:
      - name: not_null
  - name: spend_last_90_days
    type: NUMERIC
    description: Spend in the 90 days up to the run's end date.
    checks:
      - name: not_null
  - name: spend_prior_90_days
    type: NUMERIC
    description: Spend in the 90 days before that, for trend comparison.
    checks:
      - name: not_null
  - name: trailing_12m_spend
    type: NUMERIC
    description: Spend in the 12 complete months before the run's end date.
    checks:
      - name: not_null
  - name: estimated_annual_cost
    type: NUMERIC
    description: >
      For vendors paid in each of the last three complete months, their
      average monthly spend over those months times 12; otherwise
      `trailing_12m_spend`, so annual subscriptions count once.
    checks:
      - name: not_null
  - name: first_spend_date
    type: DATE
    description: Date of the first spend line with the payee.
    checks:
      - name: not_null
  - name: last_spend_date
    type: DATE
    description: Date of the most recent spend line.
    checks:
      - name: not_null
  - name: active_months
    type: INT64
    description: Number of distinct months with spend.
    checks:
      - name: not_null
      - name: positive
  - name: line_count
    type: INT64
    description: Number of spend lines with the payee.
    checks:
      - name: not_null
      - name: positive
  - name: is_recurring
    type: BOOL
    description: >
      Whether the payee looks like a subscription: paid in each of the last
      three complete months, or paid twice 11 to 13 months apart.
    checks:
      - name: not_null
  - name: is_new_vendor
    type: BOOL
    description: Whether the first spend with the payee was in the last 90 days.
    checks:
      - name: not_null

custom_checks:
  - name: vendor spend reconciles to expense lines
    description: Total spend across payees equals expense lines that have a payee.
    query: |
      SELECT IF(
        ABS(
          (SELECT IFNULL(SUM(total_spend), 0) FROM {{ this }})
          - (
            SELECT IFNULL(SUM(amount), 0)
            FROM quickbooks_stage.expense_lines
            WHERE is_expense AND payee_id IS NOT NULL
              AND transaction_date <= DATE('{{ end_date }}')
          )
        ) > 0.01,
        1,
        0
      )
    value: 0
@bruin */

WITH params AS (
  SELECT
    DATE('{{ end_date }}') AS as_of_date,
    DATE_SUB(DATE_TRUNC(DATE_ADD(DATE('{{ end_date }}'), INTERVAL 1 DAY), MONTH), INTERVAL 3 MONTH) AS recent_months_start,
    DATE_SUB(DATE_TRUNC(DATE_ADD(DATE('{{ end_date }}'), INTERVAL 1 DAY), MONTH), INTERVAL 1 DAY) AS recent_months_end,
    DATE_SUB(DATE_TRUNC(DATE_ADD(DATE('{{ end_date }}'), INTERVAL 1 DAY), MONTH), INTERVAL 12 MONTH) AS trailing_year_start
),

lines AS (
  SELECT
    line.payee_id,
    line.payee_name,
    line.payee_type,
    line.transaction_date,
    line.amount,
    COALESCE(category.category, 'Unassigned') AS category
  FROM quickbooks_stage.expense_lines AS line
  LEFT JOIN quickbooks_stage.account_categories AS category
    ON line.account_id = category.account_id
  CROSS JOIN params
  WHERE line.is_expense
    AND line.payee_id IS NOT NULL
    AND line.transaction_date <= params.as_of_date
),

annual_payees AS (
  SELECT DISTINCT later.payee_id
  FROM lines AS later
  JOIN lines AS earlier
    ON earlier.payee_id = later.payee_id
    AND DATE_DIFF(later.transaction_date, earlier.transaction_date, DAY) BETWEEN 330 AND 400
),

primary_categories AS (
  SELECT payee_id, category AS primary_category
  FROM (
    SELECT payee_id, category, SUM(amount) AS spend
    FROM lines
    GROUP BY 1, 2
  )
  QUALIFY ROW_NUMBER() OVER (PARTITION BY payee_id ORDER BY spend DESC, category) = 1
),

payees AS (
  SELECT
    payee_id,
    ANY_VALUE(payee_name) AS payee_name,
    ANY_VALUE(payee_type) AS payee_type,
    SUM(amount) AS total_spend,
    SUM(IF(transaction_date > DATE_SUB(params.as_of_date, INTERVAL 90 DAY), amount, 0)) AS spend_last_90_days,
    SUM(IF(
      transaction_date > DATE_SUB(params.as_of_date, INTERVAL 180 DAY)
        AND transaction_date <= DATE_SUB(params.as_of_date, INTERVAL 90 DAY),
      amount,
      0
    )) AS spend_prior_90_days,
    SUM(IF(transaction_date BETWEEN params.recent_months_start AND params.recent_months_end, amount, 0)) AS recent_months_spend,
    SUM(IF(transaction_date BETWEEN params.trailing_year_start AND params.recent_months_end, amount, 0)) AS trailing_12m_spend,
    COUNT(DISTINCT IF(
      transaction_date BETWEEN params.recent_months_start AND params.recent_months_end,
      DATE_TRUNC(transaction_date, MONTH),
      NULL
    )) AS recent_active_months,
    MIN(transaction_date) AS first_spend_date,
    MAX(transaction_date) AS last_spend_date,
    COUNT(DISTINCT DATE_TRUNC(transaction_date, MONTH)) AS active_months,
    COUNT(*) AS line_count,
    ANY_VALUE(params.as_of_date) AS as_of_date
  FROM lines
  CROSS JOIN params
  GROUP BY payee_id
)

SELECT
  payees.payee_id,
  payees.payee_name,
  payees.payee_type,
  primary_categories.primary_category,
  ROUND(payees.total_spend, 2) AS total_spend,
  ROUND(payees.spend_last_90_days, 2) AS spend_last_90_days,
  ROUND(payees.spend_prior_90_days, 2) AS spend_prior_90_days,
  ROUND(payees.trailing_12m_spend, 2) AS trailing_12m_spend,
  ROUND(
    IF(payees.recent_active_months = 3, payees.recent_months_spend / 3 * 12, payees.trailing_12m_spend),
    2
  ) AS estimated_annual_cost,
  payees.first_spend_date,
  payees.last_spend_date,
  payees.active_months,
  payees.line_count,
  payees.recent_active_months = 3 OR annual_payees.payee_id IS NOT NULL AS is_recurring,
  payees.first_spend_date > DATE_SUB(payees.as_of_date, INTERVAL 90 DAY) AS is_new_vendor
FROM payees
LEFT JOIN primary_categories USING (payee_id)
LEFT JOIN annual_payees USING (payee_id);
