/* @bruin
name: quickbooks_reports.cash_runway
type: bq.sql
owner: finance@example.com
description: >
  A one-row snapshot of the company's cash position and runway as of the run's
  end date: bank balances, card balances owed, receivables and payables, the
  average net burn over the last three complete months, and how many months
  the cash lasts at that burn. Balances are what QuickBooks reported at the
  last load and assume one currency. Runway uses the accrual-based net burn
  from `quickbooks_reports.monthly_kpis`, which misses costs booked as
  journal entries (often payroll) and revenue booked as sales receipts or
  deposits, so treat it as an estimate and check it against the bank.

materialization:
  type: table

depends:
  - quickbooks_stage.accounts
  - quickbooks_stage.invoices
  - quickbooks_stage.vendors
  - quickbooks_reports.monthly_kpis

tags:
  - quickbooks_reports
  - quickbooks
  - accounting
  - cash
  - runway

domains:
  - finance

meta:
  grain: one row, the snapshot as of the run's end date
  answers: >
    What's our runway? How much cash do we have? How much do we owe on the
    card?
  contains_pii: "false"
  agent_notes: >
    runway_months is null when the company is not burning cash and 0 when
    cash is gone. Mention that it is an estimate based on the last three
    months, and that payroll run through journal entries is not counted.

columns:
  - name: as_of_date
    type: DATE
    description: The run's end date.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: cash_balance
    type: NUMERIC
    description: Sum of current balances on `Bank` accounts.
    checks:
      - name: not_null
  - name: credit_card_balance
    type: NUMERIC
    description: >
      Amount owed on credit card accounts. QuickBooks reports card balances
      owed as negative account balances, so the sign is flipped here.
    checks:
      - name: not_null
  - name: accounts_receivable
    type: NUMERIC
    description: Open invoice balances owed by customers.
    checks:
      - name: not_null
      - name: non_negative
  - name: accounts_payable
    type: NUMERIC
    description: Open balances owed to vendors.
    checks:
      - name: not_null
  - name: net_liquid_position
    type: NUMERIC
    description: '`cash_balance + accounts_receivable - credit_card_balance - accounts_payable`.'
    checks:
      - name: not_null
  - name: burn_months
    type: STRING
    description: The complete months averaged for burn, for example `2026-07 to 2026-09`.
  - name: avg_monthly_net_burn
    type: NUMERIC
    description: Average `net_burn` over the last three complete months.
  - name: avg_monthly_revenue
    type: NUMERIC
    description: Average revenue over the same months.
  - name: runway_months
    type: FLOAT64
    description: >
      `cash_balance / avg_monthly_net_burn`, 0 when cash is at or below 0;
      null when net burn is zero or negative.
    checks:
      - name: non_negative

custom_checks:
  - name: exactly one snapshot row
    query: SELECT COUNT(*) FROM {{ this }}
    value: 1
@bruin */

WITH balances AS (
  SELECT
    SUM(IF(account_type = 'Bank', current_balance, 0)) AS cash_balance,
    -SUM(IF(account_type = 'Credit Card', current_balance, 0)) AS credit_card_balance,
    (SELECT IFNULL(SUM(open_balance), 0) FROM quickbooks_stage.invoices) AS accounts_receivable,
    (SELECT IFNULL(SUM(open_balance), 0) FROM quickbooks_stage.vendors) AS accounts_payable
  FROM quickbooks_stage.accounts
),

burn AS (
  SELECT
    CONCAT(FORMAT_DATE('%Y-%m', MIN(month)), ' to ', FORMAT_DATE('%Y-%m', MAX(month))) AS burn_months,
    AVG(net_burn) AS avg_monthly_net_burn,
    AVG(revenue) AS avg_monthly_revenue
  FROM (
    SELECT month, net_burn, revenue
    FROM quickbooks_reports.monthly_kpis
    ORDER BY month DESC
    LIMIT 3
  )
)

SELECT
  DATE('{{ end_date }}') AS as_of_date,
  ROUND(IFNULL(balances.cash_balance, 0), 2) AS cash_balance,
  ROUND(IFNULL(balances.credit_card_balance, 0), 2) AS credit_card_balance,
  ROUND(balances.accounts_receivable, 2) AS accounts_receivable,
  ROUND(balances.accounts_payable, 2) AS accounts_payable,
  ROUND(
    IFNULL(balances.cash_balance, 0)
      + balances.accounts_receivable
      - IFNULL(balances.credit_card_balance, 0)
      - balances.accounts_payable,
    2
  ) AS net_liquid_position,
  burn.burn_months,
  ROUND(burn.avg_monthly_net_burn, 2) AS avg_monthly_net_burn,
  ROUND(burn.avg_monthly_revenue, 2) AS avg_monthly_revenue,
  IF(
    burn.avg_monthly_net_burn > 0,
    ROUND(CAST(GREATEST(IFNULL(balances.cash_balance, 0), 0) / burn.avg_monthly_net_burn AS FLOAT64), 1),
    NULL
  ) AS runway_months
FROM balances
CROSS JOIN burn;
