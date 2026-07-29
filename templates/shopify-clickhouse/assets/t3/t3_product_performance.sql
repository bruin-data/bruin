/* @bruin
name: shopify.t3_product_performance
type: clickhouse.sql
description: "Current Shopify product catalog enriched with lifetime completed-order demand and refund-adjusted merchandise revenue."

materialization:
  type: table
  strategy: create+replace

depends:
  - shopify.t2_products
  - shopify.t2_order_line_items

tags:
  - t3
  - mart
  - shopify
  - products
  - merchandising
domains:
  - commerce
  - merchandising
  - fulfillment
  - finance
meta:
  grain: one row per current Shopify product
  source_system: shopify
  currency_scope: catalog and order-line amounts use the Shopify shop currency; mismatches are blocked
  revenue_policy: current order merchandise subtotals are allocated proportionally to completed order lines
  cost_policy: no historical margin is estimated because Shopify provides current inventory cost, not order-time COGS
  refresh_strategy: create-and-replace because this compact mart combines the full current catalog with lifetime sales history
  physical_design: product identifier is the lookup key; partitioning would add overhead at current scale
  data_classification: internal

custom_checks:
  - name: product catalog is preserved
    description: "Ensures every conformed Shopify product appears exactly once in the performance mart."
    query: |
      SELECT
        (SELECT count() FROM shopify.t3_product_performance)
        =
        (SELECT count() FROM shopify.t2_products)
    value: 1
    blocking: true
  - name: catalog and order currency agree
    description: "Prevents product sales from being aggregated into a catalog currency without conversion."
    query: |
      SELECT count()
      FROM shopify.t2_order_line_items AS lines
      INNER JOIN shopify.t2_products AS products
        ON lines.product_id = products.product_id
      WHERE lines.is_completed_order = 1
        AND lines.currency != products.catalog_currency
    value: 0
    blocking: true

columns:
  - name: product_id
    type: String
    description: "Shopify Admin GraphQL global identifier for the product."
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: product_legacy_id
    type: UInt64
    description: "Legacy numeric Shopify identifier for the product."
  - name: product_title
    type: String
    description: "Current merchant-facing Shopify product title."
  - name: product_type
    type: LowCardinality(String)
    description: "Current merchant-defined product type."
  - name: vendor
    type: LowCardinality(String)
    description: "Current product vendor."
  - name: status
    type: LowCardinality(String)
    description: "Current Shopify product status."
  - name: is_active
    type: UInt8
    description: "One when the current Shopify product status is active."
  - name: currency
    type: LowCardinality(String)
    description: "ISO Shopify shop-currency code for catalog and sales amounts."
  - name: minimum_variant_price
    type: Decimal(18, 2)
    description: "Minimum current variant price."
    checks:
      - name: non_negative
  - name: maximum_variant_price
    type: Decimal(18, 2)
    description: "Maximum current variant price."
    checks:
      - name: non_negative
  - name: inventory_quantity
    type: Int64
    description: "Current tracked inventory quantity across associated inventory items."
  - name: sellable_online_quantity
    type: Int64
    description: "Current sellable-online quantity across associated inventory items."
  - name: inventory_status
    type: LowCardinality(String)
    description: "Current stock band derived in the product model."
  - name: has_completed_sales
    type: Bool
    description: "Whether the product appears on at least one completed Shopify order."
  - name: first_completed_order_date
    type: Nullable(Date)
    description: "UTC date of the product's earliest completed order in the ingested history."
  - name: last_completed_order_date
    type: Nullable(Date)
    description: "UTC date of the product's most recent completed order in the ingested history."
  - name: completed_order_count
    type: UInt64
    description: "Distinct completed orders containing the product."
  - name: refunded_order_count
    type: UInt64
    description: "Distinct completed orders containing the product that are currently partially refunded or refunded."
  - name: unique_customer_count
    type: UInt64
    description: "Distinct identified customers who completed an order containing the product."
  - name: units_ordered
    type: UInt64
    description: "Original product units on completed orders."
  - name: gross_sales_amount
    type: Decimal(18, 2)
    description: "Original line value before discounts on completed orders."
    checks:
      - name: non_negative
  - name: discount_amount
    type: Decimal(18, 2)
    description: "Shopify line discounts on completed orders."
    checks:
      - name: non_negative
  - name: recognized_product_revenue_amount
    type: Decimal(18, 2)
    description: "Refund-adjusted current merchandise subtotal allocated to the product's completed order lines."
    checks:
      - name: non_negative
  - name: realized_revenue_per_unit
    type: Decimal(18, 2)
    description: "Recognized product revenue divided by original units ordered."
    checks:
      - name: non_negative
  - name: discount_rate
    type: Float64
    description: "Line discounts divided by original gross line value on completed orders."
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: revenue_share
    type: Float64
    description: "Product recognized revenue divided by recognized revenue across catalog-matched products."
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: source_max_updated_at
    type: DateTime64(6, 'UTC')
    description: "Latest Shopify catalog, inventory, or completed-order update timestamp contributing to the row."
@bruin */

WITH product_sales AS (
    SELECT
        lines.product_id,
        min(lines.order_date) AS first_completed_order_date,
        max(lines.order_date) AS last_completed_order_date,
        uniqExact(lines.order_id) AS completed_order_count,
        uniqExactIf(lines.order_id, lines.is_refunded_order = 1) AS refunded_order_count,
        uniqExactIf(lines.customer_id, lines.customer_id IS NOT NULL) AS unique_customer_count,
        toUInt64(sum(lines.quantity)) AS units_ordered,
        toDecimal64(sum(lines.gross_sales_amount), 2) AS gross_sales_amount,
        toDecimal64(sum(lines.discount_amount), 2) AS discount_amount,
        toDecimal64(
            sum(lines.recognized_line_revenue_amount),
            2
        ) AS recognized_product_revenue_amount,
        max(
            ifNull(
                lines.order_updated_at,
                toDateTime64('1970-01-01 00:00:00', 6, 'UTC')
            )
        ) AS sales_max_updated_at
    FROM shopify.t2_order_line_items AS lines
    WHERE lines.is_completed_order = 1
      AND lines.product_id != ''
    GROUP BY lines.product_id
),
sales_total AS (
    SELECT
        sum(sales.recognized_product_revenue_amount) AS recognized_product_revenue_amount
    FROM product_sales AS sales
)
SELECT
    products.product_id AS product_id,
    products.product_legacy_id,
    products.product_title,
    products.product_type,
    products.vendor,
    products.status,
    products.is_active,
    products.catalog_currency AS currency,
    products.minimum_variant_price,
    products.maximum_variant_price,
    products.inventory_quantity,
    products.sellable_online_quantity,
    products.inventory_status,
    toBool(ifNull(sales.completed_order_count, 0) > 0) AS has_completed_sales,
    if(
        ifNull(sales.completed_order_count, 0) = 0,
        CAST(NULL, 'Nullable(Date)'),
        toNullable(sales.first_completed_order_date)
    ) AS first_completed_order_date,
    if(
        ifNull(sales.completed_order_count, 0) = 0,
        CAST(NULL, 'Nullable(Date)'),
        toNullable(sales.last_completed_order_date)
    ) AS last_completed_order_date,
    toUInt64(ifNull(sales.completed_order_count, 0)) AS completed_order_count,
    toUInt64(ifNull(sales.refunded_order_count, 0)) AS refunded_order_count,
    toUInt64(ifNull(sales.unique_customer_count, 0)) AS unique_customer_count,
    toUInt64(ifNull(sales.units_ordered, 0)) AS units_ordered,
    ifNull(sales.gross_sales_amount, toDecimal64(0, 2)) AS gross_sales_amount,
    ifNull(sales.discount_amount, toDecimal64(0, 2)) AS discount_amount,
    ifNull(
        sales.recognized_product_revenue_amount,
        toDecimal64(0, 2)
    ) AS recognized_product_revenue_amount,
    toDecimal64(
        if(
            ifNull(sales.units_ordered, 0) = 0,
            0,
            sales.recognized_product_revenue_amount / sales.units_ordered
        ),
        2
    ) AS realized_revenue_per_unit,
    round(
        if(
            ifNull(sales.gross_sales_amount, toDecimal64(0, 2)) = 0,
            0,
            toFloat64(sales.discount_amount) / toFloat64(sales.gross_sales_amount)
        ),
        4
    ) AS discount_rate,
    round(
        if(
            ifNull(total.recognized_product_revenue_amount, toDecimal64(0, 2)) = 0,
            0,
            toFloat64(ifNull(sales.recognized_product_revenue_amount, toDecimal64(0, 2)))
                / toFloat64(total.recognized_product_revenue_amount)
        ),
        6
    ) AS revenue_share,
    greatest(
        products.source_max_updated_at,
        ifNull(
            sales.sales_max_updated_at,
            toDateTime64('1970-01-01 00:00:00', 6, 'UTC')
        )
    ) AS source_max_updated_at
FROM shopify.t2_products AS products
LEFT JOIN product_sales AS sales
    ON products.product_id = sales.product_id
CROSS JOIN sales_total AS total
