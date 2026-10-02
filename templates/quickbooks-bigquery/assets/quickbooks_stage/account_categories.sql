/* @bruin
name: quickbooks_stage.account_categories
type: bq.sql
owner: finance@example.com
description: >
  Every income-statement account with the profit-and-loss section, report
  line, and department it rolls up to. Lines come from the editable
  `quickbooks_stage.account_mapping` seed; accounts the seed does not list
  fall back to a default based on their QuickBooks account type and are
  flagged with `is_mapped = false`.

materialization:
  type: table

depends:
  - quickbooks_stage.accounts
  - quickbooks_stage.account_mapping

tags:
  - quickbooks_stage
  - quickbooks
  - accounting
  - accounts

domains:
  - finance

meta:
  grain: one row per income or expense account
  contains_pii: "false"
  agent_notes: >
    Join transaction lines to this table on account_id to group them into
    P&L lines. Accounts with is_mapped = false use a default line; add them to
    account_mapping.csv to place them precisely.

columns:
  - name: account_id
    type: STRING
    description: QuickBooks account identifier; joins to `quickbooks_stage.accounts`.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: account_name
    type: STRING
    description: Account name.
    checks:
      - name: not_null
  - name: account_type
    type: STRING
    description: QuickBooks account type, for example `Expense` or `Income`.
  - name: pnl_group
    type: STRING
    description: >
      Profit-and-loss section: `revenue`, `cogs`, `operating_expense`,
      `other_income`, or `other_expense`.
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
    description: Report line within the section, for example `Payroll` or `Software`.
    checks:
      - name: not_null
  - name: department
    type: STRING
    description: Department the account's costs belong to, when mapped.
  - name: is_recurring_revenue
    type: BOOL
    description: Whether lines on the account count toward MRR.
    checks:
      - name: not_null
  - name: is_mapped
    type: BOOL
    description: >
      Whether the account is listed in `account_mapping.csv`. False means the
      line was defaulted from the account type.
    checks:
      - name: not_null

@bruin */

SELECT
  account.account_id,
  account.account_name,
  account.account_type,
  COALESCE(
    mapping.pnl_group,
    CASE account.account_type
      WHEN 'Income' THEN 'revenue'
      WHEN 'Other Income' THEN 'other_income'
      WHEN 'Cost of Goods Sold' THEN 'cogs'
      WHEN 'Other Expense' THEN 'other_expense'
      ELSE 'operating_expense'
    END
  ) AS pnl_group,
  COALESCE(
    mapping.category,
    CASE account.account_type
      WHEN 'Income' THEN 'Other Revenue'
      WHEN 'Other Income' THEN 'Other Income'
      WHEN 'Cost of Goods Sold' THEN 'Other Cost of Revenue'
      WHEN 'Other Expense' THEN 'Other Expenses'
      ELSE 'Other Operating Expenses'
    END
  ) AS category,
  mapping.department,
  IFNULL(mapping.is_recurring_revenue, FALSE) AS is_recurring_revenue,
  mapping.account_name IS NOT NULL AS is_mapped
FROM quickbooks_stage.accounts AS account
LEFT JOIN quickbooks_stage.account_mapping AS mapping
  ON account.account_name = mapping.account_name
WHERE account.financial_statement = 'income_statement';
