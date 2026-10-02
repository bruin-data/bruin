/* @bruin
name: quickbooks_reports.customer_mrr_movements
type: bq.sql
owner: finance@example.com
description: >
  Monthly recurring revenue per customer and how it moved from the month
  before: new, expansion, contraction, churn, or reactivation. MRR is the
  recurring revenue invoiced to the customer in the month, from invoice lines
  on accounts marked `is_recurring_revenue` in `account_mapping.csv`.
  Sub-customers (jobs) roll up one level to their parent customer. MRR
  assumes monthly billing: an annual or quarterly invoice shows as a spike
  followed by churn and later reactivation. Discount lines post to a
  non-recurring account by default, so MRR is before discounts, and sales
  receipts and credit memos are not loaded. Customers in the first month of
  data are labeled `opening`. A row is kept for the month a customer churns,
  and months with no MRR before and after are dropped. Only complete months
  up to the run's end date are included.

materialization:
  type: table

depends:
  - quickbooks_stage.invoice_lines
  - quickbooks_stage.account_categories
  - quickbooks_stage.customers

tags:
  - quickbooks_reports
  - quickbooks
  - accounting
  - mrr

domains:
  - finance
  - revenue

meta:
  grain: one row per customer per month with MRR in the month or the month before
  answers: >
    What is our MRR? Which customers expanded, contracted, or churned this
    month? Who are our newest customers?
  contains_pii: "true"
  pii_note: Includes customer names.
  agent_notes: >
    Sum mrr by month for total MRR. Sum mrr_change by movement for the MRR
    bridge; churn rows carry the lost MRR as a negative mrr_change. If the
    company bills annually, churn and reactivation rows can be billing gaps,
    not real churn; check the customer's invoices before calling it churn.

columns:
  - name: month
    type: DATE
    description: First day of the month.
    primary_key: true
    checks:
      - name: not_null
  - name: customer_id
    type: STRING
    description: >
      Billing customer, the parent for sub-customers; joins to
      `quickbooks_stage.customers`.
    primary_key: true
    checks:
      - name: not_null
  - name: customer_name
    type: STRING
    description: Customer display name.
  - name: mrr
    type: NUMERIC
    description: Recurring revenue invoiced to the customer in the month.
    checks:
      - name: not_null
      - name: non_negative
  - name: previous_mrr
    type: NUMERIC
    description: The customer's MRR in the month before; 0 when they had none.
    checks:
      - name: not_null
      - name: non_negative
  - name: mrr_change
    type: NUMERIC
    description: '`mrr - previous_mrr`.'
    checks:
      - name: not_null
  - name: movement
    type: STRING
    description: >
      `opening` for customers in the first month of data, `new` for a
      customer's first month after that, `expansion`, `contraction`,
      `churn` (MRR went to 0), `reactivation` (MRR back after a gap), or
      `retained` (no change).
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - opening
          - new
          - expansion
          - contraction
          - churn
          - reactivation
          - retained
  - name: customer_first_month
    type: DATE
    description: First month the customer had MRR.
    checks:
      - name: not_null

custom_checks:
  - name: customer month keys are unique
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT month, customer_id
        FROM {{ this }}
        GROUP BY 1, 2
        HAVING COUNT(*) > 1
      )
    value: 0
  - name: mrr bridge balances
    description: >
      Each month's MRR equals the previous month's MRR plus the month's
      movements.
    query: |
      WITH monthly AS (
        SELECT month, SUM(mrr) AS mrr, SUM(mrr_change) AS change
        FROM {{ this }}
        GROUP BY month
      )
      SELECT COUNTIF(ABS(mrr - IFNULL(previous_mrr, 0) - change) > 0.01)
      FROM (
        SELECT *, LAG(mrr) OVER (ORDER BY month) AS previous_mrr
        FROM monthly
      )
    value: 0
unit_tests:
  - name: classifies_mrr_movements
    description: >
      Opening, new, expansion, contraction, churn, and reactivation, with
      one-time revenue left out of MRR.
    inputs:
      - asset: quickbooks_stage.invoice_lines
        rows:
          - {customer_id: c1, invoice_date: "2025-01-05", income_account_id: sub, amount: 100}
          - {customer_id: c1, invoice_date: "2025-02-05", income_account_id: sub, amount: 150}
          - {customer_id: c1, invoice_date: "2025-03-05", income_account_id: sub, amount: 150}
          - {customer_id: c2, invoice_date: "2025-02-10", income_account_id: sub, amount: 200}
          - {customer_id: c2, invoice_date: "2025-02-10", income_account_id: services, amount: 999}
          - {customer_id: c2, invoice_date: "2025-03-10", income_account_id: sub, amount: 100}
          - {customer_id: c2, invoice_date: "2025-04-10", income_account_id: sub, amount: 100}
          - {customer_id: c3, invoice_date: "2025-01-20", income_account_id: sub, amount: 50}
          - {customer_id: c3, invoice_date: "2025-03-20", income_account_id: sub, amount: 50}
      - asset: quickbooks_stage.account_categories
        rows:
          - {account_id: sub, is_recurring_revenue: true}
          - {account_id: services, is_recurring_revenue: false}
      - asset: quickbooks_stage.customers
        rows:
          - {customer_id: c1, customer_name: Acme, parent_customer_id: null}
    expected:
      match: exact
      rows:
        - {month: "2025-01-01", customer_id: c1, customer_name: Acme, mrr: 100, movement: opening}
        - {month: "2025-02-01", customer_id: c1, mrr: 150, mrr_change: 50, movement: expansion}
        - {month: "2025-03-01", customer_id: c1, mrr: 150, mrr_change: 0, movement: retained}
        - {month: "2025-04-01", customer_id: c1, mrr: 0, previous_mrr: 150, mrr_change: -150, movement: churn}
        - {month: "2025-02-01", customer_id: c2, mrr: 200, movement: new}
        - {month: "2025-03-01", customer_id: c2, mrr: 100, mrr_change: -100, movement: contraction}
        - {month: "2025-04-01", customer_id: c2, mrr: 100, movement: retained}
        - {month: "2025-05-01", customer_id: c2, mrr: 0, mrr_change: -100, movement: churn}
        - {month: "2025-01-01", customer_id: c3, mrr: 50, movement: opening}
        - {month: "2025-02-01", customer_id: c3, mrr: 0, mrr_change: -50, movement: churn}
        - {month: "2025-03-01", customer_id: c3, mrr: 50, previous_mrr: 0, movement: reactivation}
        - {month: "2025-04-01", customer_id: c3, mrr: 0, mrr_change: -50, movement: churn}
@bruin */

WITH customer_months AS (
  SELECT
    COALESCE(customer.parent_customer_id, line.customer_id) AS customer_id,
    DATE_TRUNC(line.invoice_date, MONTH) AS month,
    -- A month holding only credits on a subscription account has no MRR.
    GREATEST(SUM(line.amount), 0) AS mrr
  FROM quickbooks_stage.invoice_lines AS line
  JOIN quickbooks_stage.account_categories AS category
    ON line.income_account_id = category.account_id
  LEFT JOIN quickbooks_stage.customers AS customer
    ON line.customer_id = customer.customer_id
  WHERE category.is_recurring_revenue
    AND LAST_DAY(DATE_TRUNC(line.invoice_date, MONTH)) <= DATE('{{ end_date }}')
  GROUP BY 1, 2
),

bounds AS (
  -- Run the spine to the last complete month, so customers who stop billing
  -- near the end still get a churn row.
  SELECT
    MIN(month) AS first_month,
    DATE_SUB(DATE_TRUNC(DATE_ADD(DATE('{{ end_date }}'), INTERVAL 1 DAY), MONTH), INTERVAL 1 MONTH) AS last_month
  FROM customer_months
),

customer_spans AS (
  SELECT customer_id, MIN(month) AS customer_first_month
  FROM customer_months
  GROUP BY customer_id
),

spine AS (
  SELECT span.customer_id, span.customer_first_month, month
  FROM customer_spans AS span
  CROSS JOIN bounds
  CROSS JOIN UNNEST(
    GENERATE_DATE_ARRAY(span.customer_first_month, bounds.last_month, INTERVAL 1 MONTH)
  ) AS month
),

filled AS (
  SELECT
    spine.customer_id,
    spine.customer_first_month,
    spine.month,
    IFNULL(customer_months.mrr, 0) AS mrr,
    LAG(IFNULL(customer_months.mrr, 0)) OVER (
      PARTITION BY spine.customer_id
      ORDER BY spine.month
    ) AS previous_mrr
  FROM spine
  LEFT JOIN customer_months
    USING (customer_id, month)
)

SELECT
  filled.month,
  filled.customer_id,
  customer.customer_name,
  ROUND(filled.mrr, 2) AS mrr,
  ROUND(IFNULL(filled.previous_mrr, 0), 2) AS previous_mrr,
  ROUND(filled.mrr - IFNULL(filled.previous_mrr, 0), 2) AS mrr_change,
  CASE
    WHEN filled.previous_mrr IS NULL AND filled.month = bounds.first_month THEN 'opening'
    WHEN filled.previous_mrr IS NULL THEN 'new'
    WHEN filled.previous_mrr = 0 THEN 'reactivation'
    WHEN filled.mrr = 0 THEN 'churn'
    WHEN filled.mrr > filled.previous_mrr THEN 'expansion'
    WHEN filled.mrr < filled.previous_mrr THEN 'contraction'
    ELSE 'retained'
  END AS movement,
  filled.customer_first_month
FROM filled
CROSS JOIN bounds
LEFT JOIN quickbooks_stage.customers AS customer
  ON filled.customer_id = customer.customer_id
WHERE filled.mrr > 0 OR IFNULL(filled.previous_mrr, 0) > 0;
