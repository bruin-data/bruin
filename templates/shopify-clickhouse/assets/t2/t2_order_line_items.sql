/* @bruin
name: shopify.t2_order_line_items
type: clickhouse.sql
description: "Conformed Shopify order lines parsed from the nested order payload."

materialization:
  type: table
  strategy: merge

depends:
  - shopify.t1_orders
  - shopify.t2_orders

tags:
  - t2
  - conformed
  - shopify
  - order-lines
domains:
  - commerce
  - finance
  - merchandising
meta:
  grain: one row per Shopify order line item
  source_system: shopify
  currency_scope: monetary values remain in each order's shop currency
  refund_policy: recognized line revenue allocates the order's current subtotal proportionally across original net line values
  refresh_strategy: primary-key merge for lines belonging to orders updated in the run interval
  data_classification: internal

custom_checks:
  - name: excluded orders have no recognized line revenue
    description: "Ensures lines from cancelled, test, and incomplete orders do not contribute recognized merchandise revenue."
    query: |
      SELECT count()
      FROM shopify.t2_order_line_items
      WHERE is_completed_order = 0
        AND recognized_line_revenue_amount != 0
    value: 0
    blocking: true
  - name: line totals reconcile to order headers
    description: "Ensures line gross sales and discounts reconcile to Shopify order-header totals."
    query: |
      WITH line_totals AS (
          SELECT
              order_id,
              sum(gross_sales_amount) AS gross_sales_amount,
              sum(discount_amount) AS discount_amount
          FROM shopify.t2_order_line_items
          GROUP BY order_id
      )
      SELECT count()
      FROM shopify.t2_orders AS o
      INNER JOIN line_totals AS l USING (order_id)
      WHERE abs(o.gross_sales_amount - l.gross_sales_amount) > 0.01
         OR abs(o.discount_amount - l.discount_amount) > 0.01
    value: 0
    blocking: true
  - name: recognized line revenue reconciles to current merchandise subtotal
    description: "Ensures proportional line allocations reconcile to completed orders' current subtotals within cent-rounding tolerance."
    query: |
      WITH line_totals AS (
          SELECT
              order_id,
              sum(recognized_line_revenue_amount) AS recognized_line_revenue_amount,
              count() AS line_count
          FROM shopify.t2_order_line_items
          GROUP BY order_id
      )
      SELECT count()
      FROM shopify.t2_orders AS o
      INNER JOIN line_totals AS l USING (order_id)
      WHERE o.is_completed_order = 1
        AND abs(o.current_subtotal_amount - l.recognized_line_revenue_amount)
            > greatest(toDecimal64(0.01, 2), toDecimal64(l.line_count * 0.01, 2))
    value: 0
    blocking: true

columns:
  - name: line_item_key
    type: String
    description: "Date-prefixed stable key used as the ClickHouse primary and sorting key."
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: line_item_id
    type: Int64
    description: "Shopify numeric identifier for the order line item."
    checks:
      - name: not_null
      - name: unique
  - name: order_id
    type: Int64
    description: "Shopify numeric identifier for the parent order."
  - name: order_date
    type: Date
    description: "UTC calendar date when the parent order was created."
  - name: order_month
    type: Date
    description: "First day of the UTC order month."
  - name: order_updated_at
    type: Nullable(DateTime64(6, 'UTC'))
    description: "Timestamp when Shopify last updated the parent order."
  - name: customer_id
    type: Nullable(Int64)
    description: "Shopify numeric customer identifier from the parent order."
  - name: currency
    type: LowCardinality(String)
    description: "ISO shop-currency code for monetary measures."
  - name: source_name
    type: LowCardinality(String)
    description: "Shopify order source or sales channel."
  - name: product_id
    type: String
    description: "Shopify Admin GraphQL global identifier for the product, or blank for deleted/custom products."
  - name: product_legacy_id
    type: UInt64
    description: "Legacy numeric Shopify product identifier."
  - name: variant_legacy_id
    type: UInt64
    description: "Legacy numeric Shopify product variant identifier."
  - name: sku
    type: String
    description: "Stock-keeping unit recorded on the order line."
  - name: product_title
    type: String
    description: "Product title recorded when the order was placed."
  - name: variant_title
    type: String
    description: "Variant title recorded when the order was placed."
  - name: vendor
    type: LowCardinality(String)
    description: "Product vendor recorded on the order line."
  - name: quantity
    type: Int64
    description: "Original quantity ordered."
    checks:
      - name: positive
  - name: current_quantity
    type: Int64
    description: "Current quantity after order edits and removals."
  - name: unit_price
    type: Decimal(18, 2)
    description: "Unit price recorded on the line in shop currency."
    checks:
      - name: non_negative
  - name: gross_sales_amount
    type: Decimal(18, 2)
    description: "Unit price multiplied by original quantity."
    checks:
      - name: non_negative
  - name: discount_amount
    type: Decimal(18, 2)
    description: "Total discount allocated by Shopify to the line."
    checks:
      - name: non_negative
  - name: net_line_amount
    type: Decimal(18, 2)
    description: "Gross line sales less Shopify line discounts."
  - name: recognized_line_revenue_amount
    type: Decimal(18, 2)
    description: "Completed order's current subtotal allocated to the line in proportion to its original net line value."
  - name: is_completed_order
    type: UInt8
    description: "One when the parent order qualifies as completed."
  - name: is_refunded_order
    type: UInt8
    description: "One when Shopify reports the parent order as partially refunded or refunded."
  - name: requires_shipping
    type: Bool
    description: "Whether the order line requires shipping."
  - name: taxable
    type: Bool
    description: "Whether the order line is taxable."
  - name: is_gift_card
    type: Bool
    description: "Whether the order line represents a gift card."
@bruin */

WITH raw_lines AS (
    SELECT
        o.id AS order_id,
        o.updated_at AS order_updated_at,
        item
    FROM shopify.t1_orders AS o
    ARRAY JOIN JSONExtractArrayRaw(ifNull(o.line_items, '[]')) AS item
    WHERE o.updated_at BETWEEN
        parseDateTime64BestEffort('{{ start_timestamp }}', 6, 'UTC')
        AND parseDateTime64BestEffort('{{ end_timestamp }}', 6, 'UTC')
),
parsed AS (
    SELECT
        r.order_id,
        r.order_updated_at,
        JSONExtractInt(r.item, 'id') AS line_item_id,
        toUInt64(JSONExtractInt(r.item, 'product_id')) AS product_legacy_id,
        toUInt64(JSONExtractInt(r.item, 'variant_id')) AS variant_legacy_id,
        JSONExtractString(r.item, 'sku') AS sku,
        JSONExtractString(r.item, 'title') AS product_title,
        JSONExtractString(r.item, 'variant_title') AS variant_title,
        toLowCardinality(JSONExtractString(r.item, 'vendor')) AS vendor,
        toInt64(JSONExtractInt(r.item, 'quantity')) AS quantity,
        toInt64(JSONExtractInt(r.item, 'current_quantity')) AS current_quantity,
        toDecimal64OrZero(JSONExtractString(r.item, 'price'), 2) AS unit_price,
        toDecimal64OrZero(JSONExtractString(r.item, 'total_discount'), 2) AS discount_amount,
        toBool(JSONExtractBool(r.item, 'requires_shipping')) AS requires_shipping,
        toBool(JSONExtractBool(r.item, 'taxable')) AS taxable,
        toBool(JSONExtractBool(r.item, 'gift_card')) AS is_gift_card
    FROM raw_lines AS r
),
line_base AS (
    SELECT
        concat(toString(o.order_date), '|', toString(p.line_item_id)) AS line_item_key,
        p.line_item_id,
        p.order_id,
        o.order_date,
        o.order_month,
        p.order_updated_at,
        o.customer_id,
        o.currency,
        o.source_name,
        if(
            p.product_legacy_id = 0,
            '',
            concat('gid://shopify/Product/', toString(p.product_legacy_id))
        ) AS product_id,
        p.product_legacy_id,
        p.variant_legacy_id,
        p.sku,
        p.product_title,
        p.variant_title,
        p.vendor,
        p.quantity,
        p.current_quantity,
        p.unit_price,
        toDecimal64(p.unit_price * p.quantity, 2) AS gross_sales_amount,
        p.discount_amount,
        toDecimal64((p.unit_price * p.quantity) - p.discount_amount, 2) AS net_line_amount,
        o.current_subtotal_amount,
        o.is_completed_order,
        o.is_refunded_order,
        p.requires_shipping,
        p.taxable,
        p.is_gift_card
    FROM parsed AS p
    INNER JOIN shopify.t2_orders AS o
        ON p.order_id = o.order_id
),
with_order_totals AS (
    SELECT
        lines.*,
        sum(lines.net_line_amount) OVER (PARTITION BY lines.order_id) AS order_net_line_amount
    FROM line_base AS lines
)
SELECT
    lines.line_item_key,
    lines.line_item_id,
    lines.order_id,
    lines.order_date,
    lines.order_month,
    lines.order_updated_at,
    lines.customer_id,
    lines.currency,
    lines.source_name,
    lines.product_id,
    lines.product_legacy_id,
    lines.variant_legacy_id,
    lines.sku,
    lines.product_title,
    lines.variant_title,
    lines.vendor,
    lines.quantity,
    lines.current_quantity,
    lines.unit_price,
    lines.gross_sales_amount,
    lines.discount_amount,
    lines.net_line_amount,
    if(
        lines.is_completed_order = 1 AND lines.order_net_line_amount != 0,
        toDecimal64(
            toFloat64(lines.current_subtotal_amount)
                * toFloat64(lines.net_line_amount)
                / toFloat64(lines.order_net_line_amount),
            2
        ),
        toDecimal64(0, 2)
    ) AS recognized_line_revenue_amount,
    lines.is_completed_order,
    lines.is_refunded_order,
    lines.requires_shipping,
    lines.taxable,
    lines.is_gift_card
FROM with_order_totals AS lines
