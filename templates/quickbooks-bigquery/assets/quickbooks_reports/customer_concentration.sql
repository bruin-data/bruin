/* @bruin
name: quickbooks_reports.customer_concentration
type: bq.sql
owner: finance@example.com
description: >
  How dependent revenue is on the largest customers, for the latest complete
  month: each paying customer's MRR, share of total MRR, rank, and the
  cumulative share of the customers ranked above and including them, plus
  their revenue over the last 12 months and what they owe now. Sub-customers
  roll up one level to their parent, as in
  `quickbooks_reports.customer_mrr_movements`.

materialization:
  type: table

depends:
  - quickbooks_reports.customer_mrr_movements
  - quickbooks_stage.invoice_lines
  - quickbooks_stage.invoices
  - quickbooks_stage.customers

tags:
  - quickbooks_reports
  - quickbooks
  - accounting
  - customers
  - mrr

domains:
  - finance
  - revenue

meta:
  grain: one row per customer with MRR in the latest complete month
  answers: >
    Who are our biggest customers? How much of MRR comes from the top 10? Is
    a large customer behind on payments?
  contains_pii: "true"
  pii_note: Includes customer names.
  agent_notes: >
    cumulative_share at mrr_rank 10 is the top-10 concentration. Ties in MRR
    are ranked by customer name.

columns:
  - name: month
    type: DATE
    description: The latest complete month with MRR.
    checks:
      - name: not_null
  - name: customer_id
    type: STRING
    description: Customer; joins to `quickbooks_stage.customers`.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: customer_name
    type: STRING
    description: Customer display name.
  - name: mrr
    type: NUMERIC
    description: The customer's MRR in the month.
    checks:
      - name: not_null
      - name: positive
  - name: share_of_mrr
    type: FLOAT64
    description: The customer's share of total MRR, from 0 to 1.
    checks:
      - name: not_null
      - name: min
        value: 0
      - name: max
        value: 1
  - name: mrr_rank
    type: INT64
    description: Rank by MRR, 1 for the largest customer.
    checks:
      - name: not_null
      - name: unique
  - name: cumulative_share
    type: FLOAT64
    description: Share of total MRR from customers ranked at or above this one.
    checks:
      - name: not_null
      - name: max
        value: 1.000001
  - name: customer_since
    type: DATE
    description: First month the customer had MRR.
  - name: trailing_12m_revenue
    type: NUMERIC
    description: All invoiced revenue from the customer in the 12 months up to and including the month.
    checks:
      - name: not_null
  - name: open_balance
    type: NUMERIC
    description: What the customer owes now across open invoices.
    checks:
      - name: not_null
      - name: non_negative
@bruin */

WITH latest AS (
  SELECT MAX(month) AS month
  FROM quickbooks_reports.customer_mrr_movements
  WHERE mrr > 0
),

paying AS (
  SELECT movement.*
  FROM quickbooks_reports.customer_mrr_movements AS movement
  JOIN latest USING (month)
  WHERE movement.mrr > 0
),

billing_customers AS (
  SELECT customer_id, COALESCE(parent_customer_id, customer_id) AS billing_customer_id
  FROM quickbooks_stage.customers
),

revenue AS (
  SELECT
    COALESCE(billing.billing_customer_id, line.customer_id) AS customer_id,
    SUM(line.amount) AS trailing_12m_revenue
  FROM quickbooks_stage.invoice_lines AS line
  CROSS JOIN latest
  LEFT JOIN billing_customers AS billing
    ON line.customer_id = billing.customer_id
  WHERE DATE_TRUNC(line.invoice_date, MONTH) BETWEEN DATE_SUB(latest.month, INTERVAL 11 MONTH) AND latest.month
  GROUP BY 1
),

balances AS (
  SELECT
    COALESCE(billing.billing_customer_id, invoice.customer_id) AS customer_id,
    SUM(invoice.open_balance) AS open_balance
  FROM quickbooks_stage.invoices AS invoice
  LEFT JOIN billing_customers AS billing
    ON invoice.customer_id = billing.customer_id
  GROUP BY 1
),

ranked AS (
  SELECT
    paying.*,
    ROW_NUMBER() OVER (ORDER BY paying.mrr DESC, paying.customer_name, paying.customer_id) AS mrr_rank,
    SUM(paying.mrr) OVER () AS total_mrr
  FROM paying
)

SELECT
  ranked.month,
  ranked.customer_id,
  ranked.customer_name,
  ranked.mrr,
  ROUND(CAST(ranked.mrr / ranked.total_mrr AS FLOAT64), 4) AS share_of_mrr,
  ranked.mrr_rank,
  ROUND(
    CAST(
      SUM(ranked.mrr) OVER (ORDER BY ranked.mrr_rank) / ranked.total_mrr
      AS FLOAT64
    ),
    4
  ) AS cumulative_share,
  ranked.customer_first_month AS customer_since,
  ROUND(IFNULL(revenue.trailing_12m_revenue, 0), 2) AS trailing_12m_revenue,
  ROUND(IFNULL(balances.open_balance, 0), 2) AS open_balance
FROM ranked
LEFT JOIN revenue USING (customer_id)
LEFT JOIN balances USING (customer_id);
