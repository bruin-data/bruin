/* @bruin
name: shopify.t2_orders
type: clickhouse.sql
description: "Conformed Shopify order headers with customer, channel, lifecycle, and current financial measures."

materialization:
  type: table
  strategy: merge

depends:
  - shopify.t1_orders

tags:
  - t2
  - conformed
  - shopify
  - orders
domains:
  - commerce
  - finance
  - fulfillment
meta:
  grain: one row per Shopify order
  source_system: shopify
  currency_scope: monetary values remain in each order's shop currency
  revenue_policy: completed non-test non-cancelled orders use Shopify current totals
  refresh_strategy: primary-key merge for source records updated in the run interval
  data_classification: internal

custom_checks:
  - name: excluded orders have no recognized revenue
    description: "Ensures cancelled, test, and incomplete orders do not contribute recognized revenue."
    query: |
      SELECT count()
      FROM shopify.t2_orders
      WHERE is_completed_order = 0
        AND recognized_revenue_amount != 0
    value: 0
    blocking: true

columns:
  - name: order_key
    type: String
    description: "Date-prefixed stable key used as the ClickHouse primary and sorting key."
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: order_id
    type: Int64
    description: "Shopify numeric identifier for the order."
    checks:
      - name: not_null
      - name: unique
  - name: order_name
    type: String
    description: "Merchant-facing Shopify order name."
  - name: order_date
    type: Date
    description: "UTC calendar date when the order was created."
  - name: order_month
    type: Date
    description: "First day of the UTC order month for grouping and retention analysis."
  - name: created_at
    type: Nullable(DateTime64(6, 'UTC'))
    description: "Timestamp when Shopify created the order."
  - name: processed_at
    type: Nullable(DateTime64(6, 'UTC'))
    description: "Timestamp when Shopify processed the order."
  - name: order_updated_at
    type: Nullable(DateTime64(6, 'UTC'))
    description: "Timestamp when Shopify last updated the order."
  - name: customer_id
    type: Nullable(Int64)
    description: "Shopify numeric customer identifier, null for guest orders without a customer object."
  - name: currency
    type: LowCardinality(String)
    description: "ISO shop-currency code for monetary measures."
  - name: presentment_currency
    type: LowCardinality(String)
    description: "ISO currency code presented to the buyer."
  - name: source_name
    type: LowCardinality(String)
    description: "Shopify order source or sales channel."
  - name: financial_status
    type: LowCardinality(String)
    description: "Current Shopify financial status."
  - name: fulfillment_status
    type: LowCardinality(String)
    description: "Current Shopify fulfillment status, or `unfulfilled` when absent."
  - name: shipping_country_code
    type: LowCardinality(String)
    description: "ISO country code from the shipping address; blank when unavailable."
  - name: shipping_province_code
    type: LowCardinality(String)
    description: "Province or state code from the shipping address; blank when unavailable."
  - name: discount_code
    type: String
    description: "First discount code recorded on the order, or blank when none was used."
  - name: is_test_order
    type: UInt8
    description: "One when Shopify marks the order as a test."
  - name: is_cancelled_order
    type: UInt8
    description: "One when the order has a cancellation timestamp."
  - name: is_completed_order
    type: UInt8
    description: "One for paid or refunded, non-test, non-cancelled orders."
  - name: is_refunded_order
    type: UInt8
    description: "One when Shopify reports a partially refunded or refunded financial status."
  - name: gross_sales_amount
    type: Decimal(18, 2)
    description: "Original line-item value before order-level discounts."
  - name: discount_amount
    type: Decimal(18, 2)
    description: "Original total discounts applied to the order."
  - name: subtotal_amount
    type: Decimal(18, 2)
    description: "Original line-item subtotal after discounts and before shipping."
  - name: shipping_amount
    type: Decimal(18, 2)
    description: "Original shipping amount in shop currency."
  - name: tax_amount
    type: Decimal(18, 2)
    description: "Original tax amount in shop currency."
  - name: original_total_amount
    type: Decimal(18, 2)
    description: "Original Shopify order total."
  - name: current_subtotal_amount
    type: Decimal(18, 2)
    description: "Current subtotal after returns, refunds, edits, and cancellations."
  - name: current_discount_amount
    type: Decimal(18, 2)
    description: "Current discount total after returns and refunds."
  - name: current_tax_amount
    type: Decimal(18, 2)
    description: "Current tax total after returns and refunds."
  - name: current_total_amount
    type: Decimal(18, 2)
    description: "Current Shopify order total after returns, refunds, edits, and cancellations."
  - name: refunded_amount
    type: Decimal(18, 2)
    description: "Non-negative difference between original and current order totals."
  - name: outstanding_amount
    type: Decimal(18, 2)
    description: "Amount Shopify reports as still owed."
  - name: recognized_revenue_amount
    type: Decimal(18, 2)
    description: "Current total recognized only for completed, non-test, non-cancelled orders."
  - name: line_item_count
    type: UInt64
    description: "Number of serialized line items on the order."
  - name: refund_record_count
    type: UInt64
    description: "Number of serialized Shopify refund records on the order."
  - name: cancelled_at
    type: Nullable(DateTime64(6, 'UTC'))
    description: "Timestamp when the order was cancelled."
  - name: closed_at
    type: Nullable(DateTime64(6, 'UTC'))
    description: "Timestamp when the order was closed."
  - name: _ingestr_loaded_at
    type: Nullable(DateTime64(6, 'UTC'))
    description: "Timestamp when ingestr loaded the raw order used for this record."
@bruin */

WITH base AS (
    SELECT
        concat(
            toString(toDate(ifNull(created_at, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')))),
            '|',
            toString(id)
        ) AS order_key,
        id AS order_id,
        ifNull(name, '') AS order_name,
        toDate(ifNull(created_at, toDateTime64('1970-01-01 00:00:00', 6, 'UTC'))) AS order_date,
        toStartOfMonth(
            toDate(ifNull(created_at, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')))
        ) AS order_month,
        created_at,
        processed_at,
        updated_at AS order_updated_at,
        nullIf(JSONExtractInt(customer, 'id'), 0) AS customer_id,
        toLowCardinality(ifNull(currency, '')) AS currency,
        toLowCardinality(ifNull(presentment_currency, ifNull(currency, ''))) AS presentment_currency,
        toLowCardinality(ifNull(source_name, 'unknown')) AS source_name,
        toLowCardinality(ifNull(o.financial_status, 'unknown')) AS financial_status,
        toLowCardinality(ifNull(o.fulfillment_status, 'unfulfilled')) AS fulfillment_status,
        toLowCardinality(
            JSONExtractString(ifNull(shipping_address, '{}'), 'country_code')
        ) AS shipping_country_code,
        toLowCardinality(
            JSONExtractString(ifNull(shipping_address, '{}'), 'province_code')
        ) AS shipping_province_code,
        JSONExtractString(
            arrayElement(JSONExtractArrayRaw(ifNull(discount_codes, '[]')), 1),
            'code'
        ) AS discount_code,
        toUInt8(ifNull(o.test, false)) AS is_test_order,
        toUInt8(o.cancelled_at IS NOT NULL) AS is_cancelled_order,
        toUInt8(
            materialize(ifNull(o.financial_status, ''))
            IN ('paid', 'partially_refunded', 'refunded')
        ) AS has_completed_financial_status,
        toUInt8(
            materialize(ifNull(o.financial_status, ''))
            IN ('partially_refunded', 'refunded')
        ) AS is_refunded_order,
        toDecimal64OrZero(ifNull(total_line_items_price, ''), 2) AS gross_sales_amount,
        toDecimal64OrZero(ifNull(total_discounts, ''), 2) AS discount_amount,
        toDecimal64OrZero(ifNull(subtotal_price, ''), 2) AS subtotal_amount,
        toDecimal64OrZero(
            JSONExtractString(
                ifNull(total_shipping_price_set, '{}'),
                'shop_money',
                'amount'
            ),
            2
        ) AS shipping_amount,
        toDecimal64OrZero(ifNull(total_tax, ''), 2) AS tax_amount,
        toDecimal64OrZero(ifNull(total_price, ''), 2) AS original_total_amount,
        toDecimal64OrZero(ifNull(current_subtotal_price, ''), 2) AS current_subtotal_amount,
        toDecimal64OrZero(ifNull(current_total_discounts, ''), 2) AS current_discount_amount,
        toDecimal64OrZero(ifNull(current_total_tax, ''), 2) AS current_tax_amount,
        toDecimal64OrZero(ifNull(current_total_price, ''), 2) AS current_total_amount,
        toDecimal64OrZero(ifNull(total_outstanding, ''), 2) AS outstanding_amount,
        toUInt64(JSONLength(ifNull(line_items, '[]'))) AS line_item_count,
        toUInt64(JSONLength(ifNull(refunds, '[]'))) AS refund_record_count,
        cancelled_at,
        closed_at,
        _ingestr_loaded_at
    FROM shopify.t1_orders AS o
    WHERE updated_at BETWEEN
        parseDateTime64BestEffort('{{ start_timestamp }}', 6, 'UTC')
        AND parseDateTime64BestEffort('{{ end_timestamp }}', 6, 'UTC')
)
SELECT
    order_key,
    order_id,
    order_name,
    order_date,
    order_month,
    created_at,
    processed_at,
    order_updated_at,
    customer_id,
    currency,
    presentment_currency,
    source_name,
    financial_status,
    fulfillment_status,
    shipping_country_code,
    shipping_province_code,
    discount_code,
    is_test_order,
    is_cancelled_order,
    toUInt8(
        has_completed_financial_status = 1
        AND is_test_order = 0
        AND is_cancelled_order = 0
    ) AS is_completed_order,
    is_refunded_order,
    gross_sales_amount,
    discount_amount,
    subtotal_amount,
    shipping_amount,
    tax_amount,
    original_total_amount,
    current_subtotal_amount,
    current_discount_amount,
    current_tax_amount,
    current_total_amount,
    greatest(
        toDecimal64(0, 2),
        toDecimal64(original_total_amount - current_total_amount, 2)
    ) AS refunded_amount,
    outstanding_amount,
    if(
        has_completed_financial_status = 1
        AND is_test_order = 0
        AND is_cancelled_order = 0,
        current_total_amount,
        toDecimal64(0, 2)
    ) AS recognized_revenue_amount,
    line_item_count,
    refund_record_count,
    cancelled_at,
    closed_at,
    _ingestr_loaded_at
FROM base
