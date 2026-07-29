/* @bruin
name: shopify.t2_customers
type: clickhouse.sql
description: "Conformed Shopify customer profiles with consent, geography, and completed-order lifecycle measures."

materialization:
  type: table
  strategy: merge

depends:
  - shopify.t1_customers
  - shopify.t2_orders

tags:
  - t2
  - conformed
  - shopify
  - customers
  - restricted
domains:
  - commerce
  - customer
  - governance
meta:
  grain: one row per Shopify customer
  source_system: shopify
  currency_scope: customer and order values remain in their Shopify currency
  pii_policy: direct identifiers and postal addresses remain in t1; this model retains identifiers and coarse geography only
  revenue_policy: order measures include completed non-test non-cancelled orders
  refresh_strategy: primary-key merge when either the customer or an associated order changes
  data_classification: restricted

custom_checks:
  - name: customer catalog is preserved
    description: "Ensures every raw Shopify customer appears in the conformed customer table."
    query: |
      SELECT
        (SELECT count() FROM shopify.t2_customers)
        =
        (SELECT count() FROM shopify.t1_customers)
    value: 1
    blocking: true

columns:
  - name: customer_id
    type: Int64
    description: "Shopify numeric identifier for the customer."
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: customer_created_at
    type: Nullable(DateTime64(6, 'UTC'))
    description: "Timestamp when Shopify created the customer record."
  - name: customer_updated_at
    type: Nullable(DateTime64(6, 'UTC'))
    description: "Timestamp when Shopify last updated the customer record."
  - name: account_state
    type: LowCardinality(String)
    description: "Current Shopify customer-account state, or unknown when absent."
  - name: locale
    type: LowCardinality(String)
    description: "Customer locale recorded by Shopify, or blank when absent."
  - name: currency
    type: LowCardinality(String)
    description: "ISO currency code associated with Shopify's customer spending total."
  - name: country_code
    type: LowCardinality(String)
    description: "ISO country code from the customer's default Shopify address, or blank when absent."
  - name: province_code
    type: LowCardinality(String)
    description: "Province or state code from the customer's default Shopify address, or blank when absent."
  - name: accepts_marketing
    type: Bool
    description: "Whether the customer currently accepts marketing according to Shopify's legacy consent flag."
  - name: marketing_opt_in_level
    type: LowCardinality(String)
    description: "Marketing opt-in level recorded by Shopify, or unspecified when absent."
  - name: is_tax_exempt
    type: Bool
    description: "Whether Shopify marks the customer as exempt from tax."
  - name: has_verified_email
    type: Bool
    description: "Whether Shopify considers the customer's email address verified; no email value is exposed here."
  - name: shopify_orders_count
    type: Int64
    description: "Lifetime order count reported on the Shopify customer record."
    checks:
      - name: non_negative
  - name: shopify_total_spent
    type: Decimal(18, 2)
    description: "Lifetime amount spent reported on the Shopify customer record."
    checks:
      - name: non_negative
  - name: first_completed_order_date
    type: Nullable(Date)
    description: "UTC date of the customer's first completed, non-test, non-cancelled order."
  - name: last_completed_order_date
    type: Nullable(Date)
    description: "UTC date of the customer's most recent completed, non-test, non-cancelled order."
  - name: completed_order_count
    type: UInt64
    description: "Count of completed, non-test, non-cancelled orders in the conformed order table."
  - name: refunded_order_count
    type: UInt64
    description: "Count of the customer's completed orders currently marked partially refunded or refunded."
  - name: gross_sales_amount
    type: Decimal(18, 2)
    description: "Original gross line-item value across the customer's completed orders."
  - name: original_order_amount
    type: Decimal(18, 2)
    description: "Original total value across the customer's completed orders."
  - name: recognized_revenue_amount
    type: Decimal(18, 2)
    description: "Current recognized revenue across the customer's completed orders."
  - name: average_order_value
    type: Decimal(18, 2)
    description: "Recognized revenue divided by completed order count."
    checks:
      - name: non_negative
  - name: customer_status
    type: LowCardinality(String)
    description: "Lifecycle band derived from completed orders: prospect, one_time, or repeat."
    checks:
      - name: accepted_values
        value: ["prospect", "one_time", "repeat"]
  - name: source_max_updated_at
    type: DateTime64(6, 'UTC')
    description: "Latest customer or associated order update timestamp contributing to the row."
@bruin */

WITH changed_customers AS (
    SELECT id AS customer_id
    FROM shopify.t1_customers
    WHERE updated_at BETWEEN
        parseDateTime64BestEffort('{{ start_timestamp }}', 6, 'UTC')
        AND parseDateTime64BestEffort('{{ end_timestamp }}', 6, 'UTC')
    UNION DISTINCT
    SELECT assumeNotNull(customer_id) AS customer_id
    FROM shopify.t2_orders
    WHERE customer_id IS NOT NULL
      AND order_updated_at BETWEEN
        parseDateTime64BestEffort('{{ start_timestamp }}', 6, 'UTC')
        AND parseDateTime64BestEffort('{{ end_timestamp }}', 6, 'UTC')
),
order_summary AS (
    SELECT
        assumeNotNull(o.customer_id) AS customer_id,
        min(
            toNullable(
                if(o.is_completed_order = 1, o.order_date, NULL)
            )
        ) AS first_completed_order_date,
        max(
            toNullable(
                if(o.is_completed_order = 1, o.order_date, NULL)
            )
        ) AS last_completed_order_date,
        countIf(o.is_completed_order = 1) AS completed_order_count,
        countIf(o.is_completed_order = 1 AND o.is_refunded_order = 1) AS refunded_order_count,
        toDecimal64(
            sumIf(o.gross_sales_amount, o.is_completed_order = 1),
            2
        ) AS gross_sales_amount,
        toDecimal64(
            sumIf(o.original_total_amount, o.is_completed_order = 1),
            2
        ) AS original_order_amount,
        toDecimal64(sum(o.recognized_revenue_amount), 2) AS recognized_revenue_amount,
        max(o.order_updated_at) AS orders_max_updated_at
    FROM shopify.t2_orders AS o
    INNER JOIN changed_customers AS changed
        ON o.customer_id = changed.customer_id
    WHERE o.customer_id IS NOT NULL
    GROUP BY assumeNotNull(o.customer_id)
)
SELECT
    c.id AS customer_id,
    c.created_at AS customer_created_at,
    c.updated_at AS customer_updated_at,
    toLowCardinality(lower(ifNull(c.state, 'unknown'))) AS account_state,
    toLowCardinality(ifNull(c.locale, '')) AS locale,
    toLowCardinality(ifNull(c.currency, '')) AS currency,
    toLowCardinality(
        ifNull(JSONExtractString(c.default_address, 'country_code'), '')
    ) AS country_code,
    toLowCardinality(
        ifNull(JSONExtractString(c.default_address, 'province_code'), '')
    ) AS province_code,
    toBool(ifNull(c.accepts_marketing, false)) AS accepts_marketing,
    toLowCardinality(ifNull(c.marketing_opt_in_level, 'unspecified')) AS marketing_opt_in_level,
    toBool(ifNull(c.tax_exempt, false)) AS is_tax_exempt,
    toBool(ifNull(c.verified_email, false)) AS has_verified_email,
    greatest(toInt64(ifNull(c.orders_count, 0)), toInt64(0)) AS shopify_orders_count,
    greatest(
        toDecimal64OrZero(ifNull(c.total_spent, ''), 2),
        toDecimal64(0, 2)
    ) AS shopify_total_spent,
    orders.first_completed_order_date,
    orders.last_completed_order_date,
    toUInt64(ifNull(orders.completed_order_count, 0)) AS completed_order_count,
    toUInt64(ifNull(orders.refunded_order_count, 0)) AS refunded_order_count,
    toDecimal64(
        ifNull(orders.gross_sales_amount, toDecimal64(0, 2)),
        2
    ) AS gross_sales_amount,
    toDecimal64(
        ifNull(orders.original_order_amount, toDecimal64(0, 2)),
        2
    ) AS original_order_amount,
    toDecimal64(
        ifNull(orders.recognized_revenue_amount, toDecimal64(0, 2)),
        2
    ) AS recognized_revenue_amount,
    toDecimal64(
        if(
            ifNull(orders.completed_order_count, 0) = 0,
            0,
            ifNull(orders.recognized_revenue_amount, toDecimal64(0, 2))
                / orders.completed_order_count
        ),
        2
    ) AS average_order_value,
    toLowCardinality(
        multiIf(
            ifNull(orders.completed_order_count, 0) = 0, 'prospect',
            orders.completed_order_count = 1, 'one_time',
            'repeat'
        )
    ) AS customer_status,
    greatest(
        ifNull(c.updated_at, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')),
        ifNull(orders.orders_max_updated_at, toDateTime64('1970-01-01 00:00:00', 6, 'UTC'))
    ) AS source_max_updated_at
FROM shopify.t1_customers AS c
INNER JOIN changed_customers AS changed
    ON c.id = changed.customer_id
LEFT JOIN order_summary AS orders
    ON c.id = orders.customer_id
