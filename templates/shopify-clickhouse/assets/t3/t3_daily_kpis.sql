/* @bruin
name: shopify.t3_daily_kpis
type: clickhouse.sql
description: "Shopify-only daily commerce scorecard with order, revenue, refund, and customer-mix KPIs."

materialization:
  type: table
  strategy: merge

depends:
  - shopify.t3_daily_revenue
  - shopify.t2_orders
  - shopify.t2_customers

tags:
  - t3
  - mart
  - shopify
  - kpi
domains:
  - commerce
  - finance
  - customer
meta:
  grain: one row per UTC order date and shop currency
  source_system: shopify
  currency_scope: no currency conversion; every row contains exactly one shop currency
  customer_policy: customer metrics exclude guest orders without a Shopify customer identifier
  metric_scope: Shopify-native commerce KPIs only; no web-session, advertising, fee, COGS, or profit estimates
  refresh_strategy: primary-key merge for date-currency groups touched by changed orders or customers
  physical_design: unpartitioned at current scale; date-prefixed primary key supports ordered date access
  data_classification: internal

custom_checks:
  - name: customer mix does not exceed identified customers
    description: "New plus returning customers cannot exceed the distinct identified customers on a row."
    query: |
      SELECT count()
      FROM shopify.t3_daily_kpis
      WHERE new_customers + returning_customers > unique_customers
    value: 0
    blocking: true

columns:
  - name: metric_key
    type: String
    description: "Date-prefixed stable key composed of metric date and currency."
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: metric_date
    type: Date
    description: "UTC calendar date represented by the scorecard row."
  - name: metric_month
    type: Date
    description: "First day of the UTC metric month."
  - name: currency
    type: LowCardinality(String)
    description: "ISO shop-currency code for every monetary value in the row."
  - name: order_attempts
    type: UInt64
    description: "Number of Shopify order records created on the date."
  - name: completed_orders
    type: UInt64
    description: "Number of paid or refunded, non-test, non-cancelled orders."
  - name: refunded_orders
    type: UInt64
    description: "Number of completed orders marked partially refunded or refunded."
  - name: unique_customers
    type: UInt64
    description: "Distinct identified customers with completed orders."
  - name: new_customers
    type: UInt64
    description: "Distinct customers whose first completed order was on the metric date."
  - name: returning_customers
    type: UInt64
    description: "Distinct customers whose first completed order preceded the metric date."
  - name: gross_sales_amount
    type: Decimal(18, 2)
    description: "Original line-item value before discounts across completed orders."
    checks:
      - name: non_negative
  - name: discount_amount
    type: Decimal(18, 2)
    description: "Original discounts across completed orders."
    checks:
      - name: non_negative
  - name: refunded_amount
    type: Decimal(18, 2)
    description: "Reduction from original to current totals across completed orders."
    checks:
      - name: non_negative
  - name: recognized_revenue_amount
    type: Decimal(18, 2)
    description: "Current total recognized for completed, non-test, non-cancelled orders."
    checks:
      - name: non_negative
  - name: average_order_value
    type: Decimal(18, 2)
    description: "Recognized revenue divided by completed order count."
    checks:
      - name: non_negative
  - name: revenue_per_customer
    type: Decimal(18, 2)
    description: "Recognized revenue divided by distinct identified customers."
    checks:
      - name: non_negative
  - name: orders_per_customer
    type: Float64
    description: "Completed order count divided by distinct identified customers."
    checks:
      - name: non_negative
  - name: completion_rate
    type: Float64
    description: "Completed orders divided by all order attempts."
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: refund_rate
    type: Float64
    description: "Refunded completed orders divided by completed orders."
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: returning_customer_share
    type: Float64
    description: "Returning customers divided by distinct identified customers."
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: source_max_updated_at
    type: DateTime64(6, 'UTC')
    description: "Latest Shopify order or customer update timestamp contributing to the row."
@bruin */

WITH changed_metric_keys AS (
    SELECT DISTINCT
        o.order_date AS metric_date,
        o.currency AS currency
    FROM shopify.t2_orders AS o
    WHERE o.order_updated_at BETWEEN
        parseDateTime64BestEffort('{{ start_timestamp }}', 6, 'UTC')
        AND parseDateTime64BestEffort('{{ end_timestamp }}', 6, 'UTC')
    UNION DISTINCT
    SELECT DISTINCT
        c.first_completed_order_date AS metric_date,
        o.currency AS currency
    FROM shopify.t2_customers AS c
    INNER JOIN shopify.t2_orders AS o
        ON c.customer_id = o.customer_id
        AND c.first_completed_order_date = o.order_date
        AND o.is_completed_order = 1
    WHERE c.first_completed_order_date IS NOT NULL
      AND c.customer_updated_at BETWEEN
        parseDateTime64BestEffort('{{ start_timestamp }}', 6, 'UTC')
        AND parseDateTime64BestEffort('{{ end_timestamp }}', 6, 'UTC')
),
customer_daily AS (
    SELECT
        o.order_date AS metric_date,
        o.currency AS currency,
        uniqExactIf(
            o.customer_id,
            c.first_completed_order_date = o.order_date
        ) AS new_customers,
        uniqExactIf(
            o.customer_id,
            c.first_completed_order_date < o.order_date
        ) AS returning_customers,
        max(
            ifNull(
                c.source_max_updated_at,
                toDateTime64('1970-01-01 00:00:00', 6, 'UTC')
            )
        ) AS customer_max_updated_at
    FROM shopify.t2_orders AS o
    INNER JOIN changed_metric_keys AS changed
        ON o.order_date = changed.metric_date
        AND o.currency = changed.currency
    LEFT JOIN shopify.t2_customers AS c
        ON o.customer_id = c.customer_id
    WHERE o.is_completed_order = 1
      AND o.customer_id IS NOT NULL
    GROUP BY
        o.order_date,
        o.currency
)
SELECT
    concat(toString(r.revenue_date), '|', r.currency) AS metric_key,
    r.revenue_date AS metric_date,
    r.revenue_month AS metric_month,
    r.currency AS currency,
    r.order_attempts,
    r.completed_orders,
    r.refunded_orders,
    r.unique_customers,
    toUInt64(ifNull(customers.new_customers, 0)) AS new_customers,
    toUInt64(ifNull(customers.returning_customers, 0)) AS returning_customers,
    r.gross_sales_amount,
    r.discount_amount,
    r.refunded_amount,
    r.recognized_revenue_amount,
    r.average_order_value,
    toDecimal64(
        if(
            r.unique_customers = 0,
            0,
            r.recognized_revenue_amount / r.unique_customers
        ),
        2
    ) AS revenue_per_customer,
    round(
        if(
            r.unique_customers = 0,
            0,
            toFloat64(r.completed_orders) / toFloat64(r.unique_customers)
        ),
        4
    ) AS orders_per_customer,
    r.completion_rate,
    r.refund_rate,
    round(
        if(
            r.unique_customers = 0,
            0,
            toFloat64(ifNull(customers.returning_customers, 0))
                / toFloat64(r.unique_customers)
        ),
        4
    ) AS returning_customer_share,
    greatest(
        r.source_max_updated_at,
        ifNull(
            customers.customer_max_updated_at,
            toDateTime64('1970-01-01 00:00:00', 6, 'UTC')
        )
    ) AS source_max_updated_at
FROM shopify.t3_daily_revenue AS r
INNER JOIN changed_metric_keys AS changed
    ON r.revenue_date = changed.metric_date
    AND r.currency = changed.currency
LEFT JOIN customer_daily AS customers
    ON r.revenue_date = customers.metric_date
    AND r.currency = customers.currency
