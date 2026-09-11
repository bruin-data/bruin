/* @bruin
name: chargebee_reports.mrr_by_plan
type: bq.sql
description: >
  Current MRR broken down by plan/add-on item price, per native currency, from
  the recurring line items of MRR-eligible subscriptions. This answers "which
  plans and add-ons make up our MRR and how concentrated is it by product",
  which Chargebee's native reports do not expose at item-price grain. It is a
  point-in-time view of the current subscription state, not a monthly trend.
  Amounts are native currency in minor units.

materialization:
  type: table

depends:
  - chargebee_stage.subscription_items
  - chargebee_stage.subscriptions

tags:
  - chargebee_reports
  - chargebee
  - billing
  - mrr

columns:
  - name: item_price_id
    type: STRING
    description: Chargebee item-price identifier (plan or add-on).
    primary_key: true
    checks:
      - name: not_null
  - name: currency_code
    type: STRING
    description: Native currency. Do not sum across currencies.
    primary_key: true
    checks:
      - name: not_null
  - name: item_type
    type: STRING
    description: Item type, one of plan or addon.
  - name: active_subscription_count
    type: INT64
    description: MRR-eligible subscriptions carrying this item price.
  - name: total_quantity
    type: INT64
    description: Total quantity of this item across eligible subscriptions.
  - name: monthly_mrr_minor
    type: NUMERIC
    description: Normalized monthly MRR contributed by this item, in minor units.
  - name: run_rate_arr_minor
    type: NUMERIC
    description: Annualized run rate, monthly MRR multiplied by 12, in minor units.
@bruin */

SELECT
  item.item_price_id,
  subscription.currency_code,
  ANY_VALUE(item.item_type) AS item_type,
  COUNT(DISTINCT item.chargebee_subscription_id) AS active_subscription_count,
  SUM(item.quantity) AS total_quantity,
  SUM(item.item_monthly_mrr_minor) AS monthly_mrr_minor,
  SUM(item.item_monthly_mrr_minor) * 12 AS run_rate_arr_minor
FROM chargebee_stage.subscription_items AS item
INNER JOIN chargebee_stage.subscriptions AS subscription
  ON item.chargebee_subscription_id = subscription.chargebee_subscription_id
WHERE COALESCE(subscription.is_mrr_eligible, FALSE)
  AND COALESCE(item.is_recurring, FALSE)
  AND item.item_price_id IS NOT NULL
  AND subscription.currency_code IS NOT NULL
GROUP BY 1, 2;
