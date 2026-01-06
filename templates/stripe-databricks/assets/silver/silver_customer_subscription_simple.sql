/* @bruin
name: stripe_sandbox.silver_customer_subscription_simple
type: databricks.sql
materialization:
  type: table
depends:
  - stripe_sandbox.bronze_customer_data_raw
  - stripe_sandbox.bronze_subscription_data_raw
  - stripe_sandbox.bronze_charge_data_raw
  - stripe_sandbox.bronze_balance_transaction_data_raw

description: >
  A table joining customers to their subscriptions and most recent balance transaction.
  Links via: customer → charge → balance_transaction
@bruin */

SELECT
    c.id AS customer_id,
    c.name AS customer_name,
    c.email,
    c.created AS customer_created_at,
    s.id AS subscription_id,
    s.status AS subscription_status,
    s.current_period_start,
    s.current_period_end,
    bt.id AS last_transaction_id,
    bt.amount / 100.0 AS last_transaction_amount,
    bt.fee / 100.0 AS last_transaction_fee,
    bt.net / 100.0 AS last_transaction_net,
    bt.currency AS last_transaction_currency,
    bt.created AS last_transaction_created_at
FROM stripe_sandbox.bronze_customer_data_raw c
LEFT JOIN stripe_sandbox.bronze_subscription_data_raw s 
    ON c.id = s.customer
-- Get most recent charge per customer
LEFT JOIN (
    SELECT customer, MAX(created) AS max_created
    FROM stripe_sandbox.bronze_charge_data_raw
    GROUP BY customer
) ch_max ON c.id = ch_max.customer
-- Join to the actual charge record
LEFT JOIN stripe_sandbox.bronze_charge_data_raw ch 
    ON ch.customer = ch_max.customer 
    AND ch.created = ch_max.max_created
-- Join charge to its balance transaction
LEFT JOIN stripe_sandbox.bronze_balance_transaction_data_raw bt 
    ON ch.balance_transaction = bt.id
