/* @bruin
name: stripe_sandbox.silver_customer_subscription_simple
type: databricks.sql
materialization:
  type: table
depends:
  - customers_raw
  - subscriptions_raw
  - charges_raw

description: >
  Simplified view joining customers to their subscriptions and most recent charge.
  Provides a clean, analysis-ready view of customer activity.
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
    ch.id AS last_charge_id,
    ch.amount / 100.0 AS last_charge_amount,
    ch.currency AS last_charge_currency,
    ch.created AS last_charge_created_at
FROM customers_raw c
LEFT JOIN subscriptions_raw s ON c.id = s.customer
LEFT JOIN (
    SELECT
        customer,
        MAX(created) AS max_created
    FROM charges_raw
    GROUP BY customer
) ch_max ON c.id = ch_max.customer
LEFT JOIN charges_raw ch ON ch.customer = ch_max.customer AND ch.created = ch_max.max_created

