/* @bruin
name: shopify.t3_daily_revenue
type: clickhouse.sql
description: "Daily Shopify order and revenue mart at UTC date and shop-currency grain."

materialization:
  type: table
  strategy: merge

depends:
  - shopify.t2_orders

tags:
  - t3
  - mart
  - shopify
  - revenue
domains:
  - commerce
  - finance
meta:
  grain: one row per UTC order date and shop currency
  source_system: shopify
  currency_scope: no currency conversion; every row contains exactly one shop currency
  revenue_policy: completed non-test non-cancelled orders use Shopify current totals
  refresh_strategy: primary-key merge that recomputes complete date-currency groups touched by changed orders
  physical_design: unpartitioned at current scale; date-prefixed primary key supports ordered date access
  data_classification: internal

custom_checks:
  - name: recognized revenue reconciles
    description: "Recognized revenue must equal current order value for the completed orders in each row."
    query: |
      SELECT count()
      FROM shopify.t3_daily_revenue
      WHERE recognized_revenue_amount != current_order_amount
    value: 0
    blocking: true

columns:
  - name: revenue_key
    type: String
    description: "Date-prefixed stable key composed of revenue date and currency."
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: revenue_date
    type: Date
    description: "UTC calendar date when the represented orders were created."
    checks:
      - name: not_null
  - name: revenue_month
    type: Date
    description: "First day of the UTC revenue month."
  - name: currency
    type: LowCardinality(String)
    description: "ISO shop-currency code for every monetary value in the row."
    checks:
      - name: not_null
  - name: order_attempts
    type: UInt64
    description: "Number of Shopify orders created on the date, including incomplete, cancelled, and test orders."
  - name: completed_orders
    type: UInt64
    description: "Number of paid or refunded, non-test, non-cancelled orders."
  - name: cancelled_orders
    type: UInt64
    description: "Number of orders with a Shopify cancellation timestamp."
  - name: test_orders
    type: UInt64
    description: "Number of orders Shopify marks as tests."
  - name: refunded_orders
    type: UInt64
    description: "Number of completed orders currently marked partially refunded or refunded."
  - name: unique_customers
    type: UInt64
    description: "Distinct identified customers with completed orders; guest orders are excluded."
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
  - name: subtotal_amount
    type: Decimal(18, 2)
    description: "Original post-discount subtotal across completed orders."
    checks:
      - name: non_negative
  - name: shipping_amount
    type: Decimal(18, 2)
    description: "Original shipping amount across completed orders."
    checks:
      - name: non_negative
  - name: tax_amount
    type: Decimal(18, 2)
    description: "Original tax amount across completed orders."
    checks:
      - name: non_negative
  - name: original_order_amount
    type: Decimal(18, 2)
    description: "Original total value across completed orders."
    checks:
      - name: non_negative
  - name: current_order_amount
    type: Decimal(18, 2)
    description: "Current total value across completed orders after returns, refunds, and edits."
    checks:
      - name: non_negative
  - name: refunded_amount
    type: Decimal(18, 2)
    description: "Non-negative reduction from original to current totals across completed orders."
    checks:
      - name: non_negative
  - name: outstanding_amount
    type: Decimal(18, 2)
    description: "Amount Shopify reports as still owed across non-test, non-cancelled order attempts."
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
  - name: source_max_updated_at
    type: DateTime64(6, 'UTC')
    description: "Latest Shopify order update timestamp contributing to the row."
@bruin */

WITH changed_daily_keys AS (
    SELECT DISTINCT
        o.order_date,
        o.currency
    FROM shopify.t2_orders AS o
    WHERE o.order_updated_at BETWEEN
        parseDateTime64BestEffort('{{ start_timestamp }}', 6, 'UTC')
        AND parseDateTime64BestEffort('{{ end_timestamp }}', 6, 'UTC')
),
daily AS (
    SELECT
        o.order_date AS revenue_date,
        o.currency AS currency,
        count() AS order_attempts,
        countIf(o.is_completed_order = 1) AS completed_orders,
        countIf(o.is_cancelled_order = 1) AS cancelled_orders,
        countIf(o.is_test_order = 1) AS test_orders,
        countIf(o.is_completed_order = 1 AND o.is_refunded_order = 1) AS refunded_orders,
        uniqExactIf(o.customer_id, o.is_completed_order = 1 AND o.customer_id IS NOT NULL) AS unique_customers,
        toDecimal64(
            sumIf(o.gross_sales_amount, o.is_completed_order = 1),
            2
        ) AS gross_sales_amount,
        toDecimal64(
            sumIf(o.discount_amount, o.is_completed_order = 1),
            2
        ) AS discount_amount,
        toDecimal64(
            sumIf(o.subtotal_amount, o.is_completed_order = 1),
            2
        ) AS subtotal_amount,
        toDecimal64(
            sumIf(o.shipping_amount, o.is_completed_order = 1),
            2
        ) AS shipping_amount,
        toDecimal64(
            sumIf(o.tax_amount, o.is_completed_order = 1),
            2
        ) AS tax_amount,
        toDecimal64(
            sumIf(o.original_total_amount, o.is_completed_order = 1),
            2
        ) AS original_order_amount,
        toDecimal64(
            sumIf(o.current_total_amount, o.is_completed_order = 1),
            2
        ) AS current_order_amount,
        toDecimal64(
            sumIf(o.refunded_amount, o.is_completed_order = 1),
            2
        ) AS refunded_amount,
        toDecimal64(
            sumIf(
                o.outstanding_amount,
                o.is_test_order = 0 AND o.is_cancelled_order = 0
            ),
            2
        ) AS outstanding_amount,
        toDecimal64(
            sum(o.recognized_revenue_amount),
            2
        ) AS recognized_revenue_amount,
        max(
            ifNull(
                o.order_updated_at,
                toDateTime64('1970-01-01 00:00:00', 6, 'UTC')
            )
        ) AS source_max_updated_at
    FROM shopify.t2_orders AS o
    INNER JOIN changed_daily_keys AS changed
        ON o.order_date = changed.order_date
        AND o.currency = changed.currency
    GROUP BY
        o.order_date,
        o.currency
)
SELECT
    concat(toString(d.revenue_date), '|', d.currency) AS revenue_key,
    d.revenue_date,
    toStartOfMonth(d.revenue_date) AS revenue_month,
    d.currency,
    d.order_attempts,
    d.completed_orders,
    d.cancelled_orders,
    d.test_orders,
    d.refunded_orders,
    d.unique_customers,
    d.gross_sales_amount,
    d.discount_amount,
    d.subtotal_amount,
    d.shipping_amount,
    d.tax_amount,
    d.original_order_amount,
    d.current_order_amount,
    d.refunded_amount,
    d.outstanding_amount,
    d.recognized_revenue_amount,
    toDecimal64(
        if(
            d.completed_orders = 0,
            0,
            d.recognized_revenue_amount / d.completed_orders
        ),
        2
    ) AS average_order_value,
    round(
        if(
            d.order_attempts = 0,
            0,
            toFloat64(d.completed_orders) / toFloat64(d.order_attempts)
        ),
        4
    ) AS completion_rate,
    round(
        if(
            d.completed_orders = 0,
            0,
            toFloat64(d.refunded_orders) / toFloat64(d.completed_orders)
        ),
        4
    ) AS refund_rate,
    d.source_max_updated_at
FROM daily AS d
