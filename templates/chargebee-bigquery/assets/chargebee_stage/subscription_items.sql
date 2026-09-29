/* @bruin
name: chargebee_stage.subscription_items
type: bq.sql
description: >
  Subscription line items, one row per subscription and item price, unnested
  from the Chargebee subscription `subscription_items` array (Product Catalog
  2.0). Each recurring item's amount is normalized to a monthly run rate
  (`item_monthly_mrr_minor`) so MRR can be broken down by plan and add-on rather
  than only read as a single subscription total. Amounts stay in native-currency
  minor units. This holds current state only, mirroring the subscriptions model.

materialization:
  type: table

depends:
  - chargebee_raw.subscription

tags:
  - chargebee_stage
  - chargebee
  - billing
  - subscriptions

columns:
  - name: chargebee_subscription_id
    type: STRING
    description: Subscription the line item belongs to.
    primary_key: true
    checks:
      - name: not_null
  - name: item_price_id
    type: STRING
    description: Chargebee item-price identifier for the line, the plan/addon key.
    primary_key: true
    checks:
      - name: not_null
  - name: chargebee_customer_id
    type: STRING
    description: Customer the subscription belongs to.
  - name: item_type
    type: STRING
    description: Line item type, one of plan, addon, charge.
  - name: is_recurring
    type: BOOL
    description: Whether the item recurs (plan or addon) versus a one-time charge.
  - name: quantity
    type: INT64
    description: Quantity of the item on the subscription.
  - name: unit_price_minor
    type: NUMERIC
    description: Per-unit price in native-currency minor units.
  - name: item_amount_minor
    type: NUMERIC
    description: Line amount (unit price x quantity) in native-currency minor units.
  - name: billing_period
    type: INT64
    description: Number of billing-period units for this item.
  - name: billing_period_unit
    type: STRING
    description: Unit of the item billing period, one of day, week, month, year.
  - name: item_monthly_mrr_minor
    type: NUMERIC
    description: >
      Line amount normalized to a monthly run rate in minor units. Null for
      one-time charges. Annual and other cycles are converted to a monthly
      equivalent (e.g. a yearly amount divided by 12).
@bruin */

SELECT
  subscription.id AS chargebee_subscription_id,
  LAX_STRING(item.item_price_id) AS item_price_id,
  subscription.customer_id AS chargebee_customer_id,
  LAX_STRING(item.item_type) AS item_type,
  LAX_STRING(item.item_type) IN ('plan', 'addon') AS is_recurring,
  LAX_INT64(item.quantity) AS quantity,
  SAFE_CAST(LAX_INT64(item.unit_price) AS NUMERIC) AS unit_price_minor,
  SAFE_CAST(LAX_INT64(item.amount) AS NUMERIC) AS item_amount_minor,
  LAX_INT64(item.billing_period) AS billing_period,
  LAX_STRING(item.billing_period_unit) AS billing_period_unit,
  IF(
    LAX_STRING(item.item_type) IN ('plan', 'addon'),
    CASE LAX_STRING(item.billing_period_unit)
      WHEN 'month' THEN SAFE_DIVIDE(
        SAFE_CAST(LAX_INT64(item.amount) AS NUMERIC),
        NULLIF(SAFE_CAST(LAX_INT64(item.billing_period) AS NUMERIC), 0)
      )
      WHEN 'year' THEN SAFE_DIVIDE(
        SAFE_CAST(LAX_INT64(item.amount) AS NUMERIC),
        NULLIF(SAFE_CAST(LAX_INT64(item.billing_period) * 12 AS NUMERIC), 0)
      )
      WHEN 'week' THEN SAFE_DIVIDE(
        SAFE_CAST(LAX_INT64(item.amount) * 52 AS NUMERIC),
        NULLIF(SAFE_CAST(LAX_INT64(item.billing_period) * 12 AS NUMERIC), 0)
      )
      WHEN 'day' THEN SAFE_DIVIDE(
        SAFE_CAST(LAX_INT64(item.amount) * 365 AS NUMERIC),
        NULLIF(SAFE_CAST(LAX_INT64(item.billing_period) * 12 AS NUMERIC), 0)
      )
      ELSE NULL
    END,
    NULL
  ) AS item_monthly_mrr_minor
FROM chargebee_raw.subscription AS subscription,
  UNNEST(JSON_QUERY_ARRAY(subscription.subscription_items)) AS item
WHERE NOT COALESCE(subscription.deleted, FALSE);
