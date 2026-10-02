/* @bruin
name: quickbooks_reports.monthly_pnl
type: bq.sql
owner: finance@example.com
description: >
  Accrual profit and loss by month and report line, built from invoice lines
  (by invoice date) and purchase and bill lines (by transaction date, so
  bills count when billed). Lines are grouped with
  `quickbooks_stage.account_categories`, which you control through
  `account_mapping.csv`. Revenue and income are positive, and so are costs; a
  line that offsets its section, such as an invoice line coded to an expense
  account for a rebilled cost, is negative. Only invoices, purchases, and
  bills are loaded: journal entries (often payroll), sales receipts and
  deposits (often how Stripe syncs), credit memos, and vendor credits are
  missing, so check this against the QuickBooks P&L before relying on it.
  Only complete months up to the run's end date are included.

materialization:
  type: table

depends:
  - quickbooks_stage.invoice_lines
  - quickbooks_stage.expense_lines
  - quickbooks_stage.account_categories

tags:
  - quickbooks_reports
  - quickbooks
  - accounting
  - pnl

domains:
  - finance

meta:
  grain: one row per month, P&L section, report line, and department
  answers: >
    What did we spend on X last month? What is our gross margin? How do
    costs break down by department?
  contains_pii: "false"
  agent_notes: >
    Sum amount by pnl_group for a month to get revenue, cogs, and operating
    expenses. List DISTINCT category before filtering on one; the names come
    from account_mapping.csv. Drill into individual lines with
    quickbooks_stage.expense_lines or quickbooks_stage.invoice_lines. Journal
    entries, sales receipts, deposits, and credits are not loaded, so say so
    when payroll or revenue looks low.

columns:
  - name: month
    type: DATE
    description: First day of the month.
    primary_key: true
    checks:
      - name: not_null
  - name: pnl_group
    type: STRING
    description: '`revenue`, `cogs`, `operating_expense`, `other_income`, or `other_expense`.'
    primary_key: true
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - revenue
          - cogs
          - operating_expense
          - other_income
          - other_expense
  - name: category
    type: STRING
    description: Report line, for example `Subscription Revenue` or `Payroll`.
    primary_key: true
    checks:
      - name: not_null
  - name: department
    type: STRING
    description: Department the line belongs to; `Unassigned` when not mapped.
    primary_key: true
    checks:
      - name: not_null
  - name: amount
    type: NUMERIC
    description: >
      Total for the line in the month, positive in the section's normal
      direction. Discounts, card refunds, and offsetting lines reduce it.
    checks:
      - name: not_null
  - name: line_count
    type: INT64
    description: Number of transaction lines behind the amount.
    checks:
      - name: not_null
      - name: positive

custom_checks:
  - name: pnl keys are unique
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT month, pnl_group, category, department
        FROM {{ this }}
        GROUP BY 1, 2, 3, 4
        HAVING COUNT(*) > 1
      )
    value: 0
  - name: net income reconciles to source lines
    description: >
      Income less costs in the report equals invoice lines less expense
      lines in the same months, so no line is dropped or double counted by
      the account mapping.
    query: |
      SELECT COUNTIF(ABS(report.net - source.net) > 0.01)
      FROM (
        SELECT
          month,
          SUM(IF(pnl_group IN ('revenue', 'other_income'), amount, -amount)) AS net
        FROM {{ this }}
        GROUP BY month
      ) AS report
      JOIN (
        SELECT month, SUM(amount) AS net
        FROM (
          SELECT DATE_TRUNC(invoice_date, MONTH) AS month, amount
          FROM quickbooks_stage.invoice_lines
          UNION ALL
          SELECT DATE_TRUNC(transaction_date, MONTH), -amount
          FROM quickbooks_stage.expense_lines
          WHERE is_expense
        )
        GROUP BY month
      ) AS source
        USING (month)
    value: 0
  - name: every account with activity is mapped
    description: >
      Non-blocking: accounts with invoice or spend lines that are not listed
      in account_mapping.csv and fell back to a default report line. Add them
      to the CSV so they land on the right line.
    query: |
      SELECT COUNT(DISTINCT category.account_id)
      FROM (
        SELECT income_account_id AS account_id FROM quickbooks_stage.invoice_lines
        UNION ALL
        SELECT account_id FROM quickbooks_stage.expense_lines WHERE is_expense
      ) AS line
      JOIN quickbooks_stage.account_categories AS category
        USING (account_id)
      WHERE NOT category.is_mapped
    value: 0
    blocking: false
  - name: single currency
    description: >
      Non-blocking: the reports add amounts across transactions, so they
      assume one currency. A non-zero result means transactions in more than
      one currency were loaded, and totals mix currencies.
    query: |
      SELECT GREATEST(COUNT(DISTINCT currency) - 1, 0)
      FROM (
        SELECT currency FROM quickbooks_stage.invoice_lines
        UNION ALL
        SELECT currency FROM quickbooks_stage.expense_lines
      )
      WHERE currency IS NOT NULL
    value: 0
    blocking: false
@bruin */

WITH all_lines AS (
  SELECT
    DATE_TRUNC(invoice_date, MONTH) AS month,
    income_account_id AS account_id,
    'invoice' AS source,
    amount
  FROM quickbooks_stage.invoice_lines

  UNION ALL

  SELECT
    DATE_TRUNC(transaction_date, MONTH) AS month,
    account_id,
    'spend' AS source,
    amount
  FROM quickbooks_stage.expense_lines
  WHERE is_expense
),

grouped_lines AS (
  SELECT
    line.month,
    line.amount,
    line.source,
    COALESCE(
      category.pnl_group,
      IF(line.source = 'invoice', 'revenue', 'operating_expense')
    ) AS pnl_group,
    COALESCE(category.category, 'Unassigned') AS category,
    COALESCE(category.department, 'Unassigned') AS department
  FROM all_lines AS line
  LEFT JOIN quickbooks_stage.account_categories AS category
    ON line.account_id = category.account_id
  WHERE LAST_DAY(line.month) <= DATE('{{ end_date }}')
)

SELECT
  month,
  pnl_group,
  category,
  department,
  -- Invoice lines add to income and spend lines add to costs; a line that
  -- lands in the other kind of section offsets it.
  ROUND(SUM(
    IF(
      (source = 'invoice') = (pnl_group IN ('revenue', 'other_income')),
      amount,
      -amount
    )
  ), 2) AS amount,
  COUNT(*) AS line_count
FROM grouped_lines
GROUP BY 1, 2, 3, 4;
