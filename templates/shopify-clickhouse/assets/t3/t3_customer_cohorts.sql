/* @bruin
name: shopify.t3_customer_cohorts
type: clickhouse.sql
description: "Monthly Shopify customer retention and revenue cohorts by first completed-order month and currency."

materialization:
  type: table
  strategy: create+replace

depends:
  - shopify.t2_orders

tags:
  - t3
  - mart
  - shopify
  - cohorts
  - retention
domains:
  - commerce
  - customer
  - finance
meta:
  grain: one row per cohort month, activity month, and shop currency
  source_system: shopify
  currency_scope: customers are assigned to independent cohorts per shop currency; no currency conversion
  customer_policy: guest orders without a Shopify customer identifier are excluded
  revenue_policy: completed non-test non-cancelled orders use Shopify current totals
  refresh_strategy: create-and-replace because cohort assignment depends on complete customer order history
  physical_design: compact aggregate rebuilt atomically; partitioning would add overhead at current scale
  data_classification: internal aggregated

custom_checks:
  - name: cohort chronology is valid
    description: "Activity month cannot precede the customer's cohort month."
    query: |
      SELECT count()
      FROM shopify.t3_customer_cohorts
      WHERE activity_month < cohort_month
    value: 0
    blocking: true
  - name: retained customers fit cohort
    description: "Retained customers cannot exceed the original cohort size."
    query: |
      SELECT count()
      FROM shopify.t3_customer_cohorts
      WHERE retained_customers > cohort_size
    value: 0
    blocking: true

columns:
  - name: cohort_key
    type: String
    description: "Stable key composed of cohort month, activity month, and currency."
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: cohort_month
    type: Date
    description: "First day of the month containing customers' first completed order."
  - name: activity_month
    type: Date
    description: "First day of the month containing the represented follow-on order activity."
  - name: months_since_first_order
    type: UInt32
    description: "Whole calendar months elapsed between cohort and activity month."
  - name: currency
    type: LowCardinality(String)
    description: "ISO shop-currency code for the cohort and every monetary value in the row."
  - name: cohort_size
    type: UInt64
    description: "Distinct customers whose first completed order in the currency occurred in the cohort month."
  - name: retained_customers
    type: UInt64
    description: "Distinct cohort customers with a completed order in the activity month."
  - name: completed_orders
    type: UInt64
    description: "Completed orders placed by retained customers in the activity month."
  - name: recognized_revenue_amount
    type: Decimal(18, 2)
    description: "Current recognized revenue from cohort customers in the activity month."
    checks:
      - name: non_negative
  - name: revenue_per_retained_customer
    type: Decimal(18, 2)
    description: "Recognized revenue divided by retained customer count."
    checks:
      - name: non_negative
  - name: retention_rate
    type: Float64
    description: "Retained customers divided by original cohort size."
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: source_max_updated_at
    type: DateTime64(6, 'UTC')
    description: "Latest Shopify order update timestamp contributing to the activity row."
@bruin */

WITH completed_orders AS (
    SELECT
        assumeNotNull(o.customer_id) AS customer_id,
        o.order_month,
        o.currency,
        o.recognized_revenue_amount,
        ifNull(
            o.order_updated_at,
            toDateTime64('1970-01-01 00:00:00', 6, 'UTC')
        ) AS order_updated_at
    FROM shopify.t2_orders AS o
    WHERE o.is_completed_order = 1
      AND o.customer_id IS NOT NULL
),
first_orders AS (
    SELECT
        orders.customer_id,
        orders.currency,
        min(orders.order_month) AS cohort_month
    FROM completed_orders AS orders
    GROUP BY
        orders.customer_id,
        orders.currency
),
cohort_sizes AS (
    SELECT
        first.cohort_month,
        first.currency,
        uniqExact(first.customer_id) AS cohort_size
    FROM first_orders AS first
    GROUP BY
        first.cohort_month,
        first.currency
),
activity AS (
    SELECT
        first.cohort_month,
        orders.order_month AS activity_month,
        orders.currency,
        uniqExact(orders.customer_id) AS retained_customers,
        count() AS completed_orders,
        toDecimal64(
            sum(orders.recognized_revenue_amount),
            2
        ) AS recognized_revenue_amount,
        max(orders.order_updated_at) AS source_max_updated_at
    FROM completed_orders AS orders
    INNER JOIN first_orders AS first
        ON orders.customer_id = first.customer_id
        AND orders.currency = first.currency
    GROUP BY
        first.cohort_month,
        orders.order_month,
        orders.currency
)
SELECT
    concat(
        toString(activity.cohort_month),
        '|',
        toString(activity.activity_month),
        '|',
        activity.currency
    ) AS cohort_key,
    activity.cohort_month,
    activity.activity_month,
    toUInt32(dateDiff('month', activity.cohort_month, activity.activity_month))
        AS months_since_first_order,
    activity.currency,
    sizes.cohort_size,
    activity.retained_customers,
    activity.completed_orders,
    activity.recognized_revenue_amount,
    toDecimal64(
        if(
            activity.retained_customers = 0,
            0,
            activity.recognized_revenue_amount / activity.retained_customers
        ),
        2
    ) AS revenue_per_retained_customer,
    round(
        if(
            sizes.cohort_size = 0,
            0,
            toFloat64(activity.retained_customers) / toFloat64(sizes.cohort_size)
        ),
        4
    ) AS retention_rate,
    activity.source_max_updated_at
FROM activity
INNER JOIN cohort_sizes AS sizes
    ON activity.cohort_month = sizes.cohort_month
    AND activity.currency = sizes.currency
