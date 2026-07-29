/* @bruin
name: shopify.t3_payment_reconciliation
type: clickhouse.sql
description: "Daily Shopify order financial-status and amount reconciliation at UTC date and currency grain."

materialization:
  type: table
  strategy: merge

depends:
  - shopify.t2_orders

tags:
  - t3
  - mart
  - shopify
  - payments
  - reconciliation
domains:
  - commerce
  - finance
meta:
  grain: one row per UTC order date and shop currency
  source_system: shopify orders
  currency_scope: no currency conversion; every row contains exactly one shop currency
  reconciliation_scope: Shopify order financial statuses and order-header amounts
  transaction_scope: does not include a gateway settlement or balance transaction ledger
  refresh_strategy: primary-key merge that recomputes complete date-currency groups touched by changed orders
  physical_design: unpartitioned at current scale; date-prefixed primary key supports ordered date access
  data_classification: internal

custom_checks:
  - name: financial status buckets reconcile
    description: "Every order must fall into exactly one declared financial-status bucket."
    query: |
      SELECT count()
      FROM shopify.t3_payment_reconciliation
      WHERE order_attempts !=
        pending_orders
        + authorized_orders
        + partially_paid_orders
        + paid_orders
        + partially_refunded_orders
        + refunded_orders
        + voided_orders
        + other_status_orders
    value: 0
    blocking: true
  - name: recognized amount reconciles
    description: "Recognized revenue must equal current totals for completed non-test non-cancelled orders."
    query: |
      SELECT count()
      FROM shopify.t3_payment_reconciliation
      WHERE recognized_revenue_amount != completed_current_amount
    value: 0
    blocking: true

columns:
  - name: reconciliation_key
    type: String
    description: "Date-prefixed stable key composed of reconciliation date and currency."
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: reconciliation_date
    type: Date
    description: "UTC calendar date when the represented orders were created."
  - name: reconciliation_month
    type: Date
    description: "First day of the UTC reconciliation month."
  - name: currency
    type: LowCardinality(String)
    description: "ISO shop-currency code for every monetary value in the row."
  - name: order_attempts
    type: UInt64
    description: "Number of Shopify order records created on the date."
  - name: pending_orders
    type: UInt64
    description: "Orders whose current Shopify financial status is pending."
  - name: authorized_orders
    type: UInt64
    description: "Orders whose current Shopify financial status is authorized."
  - name: partially_paid_orders
    type: UInt64
    description: "Orders whose current Shopify financial status is partially paid."
  - name: paid_orders
    type: UInt64
    description: "Orders whose current Shopify financial status is paid."
  - name: partially_refunded_orders
    type: UInt64
    description: "Orders whose current Shopify financial status is partially refunded."
  - name: refunded_orders
    type: UInt64
    description: "Orders whose current Shopify financial status is refunded."
  - name: voided_orders
    type: UInt64
    description: "Orders whose current Shopify financial status is voided."
  - name: other_status_orders
    type: UInt64
    description: "Orders with a missing or non-standard Shopify financial status."
  - name: test_orders
    type: UInt64
    description: "Orders Shopify marks as tests, independently of financial status."
  - name: cancelled_orders
    type: UInt64
    description: "Orders with a Shopify cancellation timestamp, independently of financial status."
  - name: original_order_amount
    type: Decimal(18, 2)
    description: "Original total value across all represented order attempts."
    checks:
      - name: non_negative
  - name: current_order_amount
    type: Decimal(18, 2)
    description: "Current total value across all represented order attempts."
    checks:
      - name: non_negative
  - name: completed_current_amount
    type: Decimal(18, 2)
    description: "Current total value across completed, non-test, non-cancelled orders."
    checks:
      - name: non_negative
  - name: refunded_amount
    type: Decimal(18, 2)
    description: "Non-negative reduction from original to current totals across completed orders."
    checks:
      - name: non_negative
  - name: outstanding_amount
    type: Decimal(18, 2)
    description: "Amount Shopify reports as still owed across non-test, non-cancelled orders."
    checks:
      - name: non_negative
  - name: recognized_revenue_amount
    type: Decimal(18, 2)
    description: "Current total recognized for completed, non-test, non-cancelled orders."
    checks:
      - name: non_negative
  - name: source_max_updated_at
    type: DateTime64(6, 'UTC')
    description: "Latest Shopify order update timestamp contributing to the row."
@bruin */

WITH changed_reconciliation_keys AS (
    SELECT DISTINCT
        o.order_date,
        o.currency
    FROM shopify.t2_orders AS o
    WHERE o.order_updated_at BETWEEN
        parseDateTime64BestEffort('{{ start_timestamp }}', 6, 'UTC')
        AND parseDateTime64BestEffort('{{ end_timestamp }}', 6, 'UTC')
)
SELECT
    concat(toString(o.order_date), '|', o.currency) AS reconciliation_key,
    o.order_date AS reconciliation_date,
    toStartOfMonth(o.order_date) AS reconciliation_month,
    o.currency,
    count() AS order_attempts,
    countIf(o.financial_status = 'pending') AS pending_orders,
    countIf(o.financial_status = 'authorized') AS authorized_orders,
    countIf(o.financial_status = 'partially_paid') AS partially_paid_orders,
    countIf(o.financial_status = 'paid') AS paid_orders,
    countIf(o.financial_status = 'partially_refunded') AS partially_refunded_orders,
    countIf(o.financial_status = 'refunded') AS refunded_orders,
    countIf(o.financial_status = 'voided') AS voided_orders,
    countIf(
        o.financial_status NOT IN (
            'pending',
            'authorized',
            'partially_paid',
            'paid',
            'partially_refunded',
            'refunded',
            'voided'
        )
    ) AS other_status_orders,
    countIf(o.is_test_order = 1) AS test_orders,
    countIf(o.is_cancelled_order = 1) AS cancelled_orders,
    toDecimal64(sum(o.original_total_amount), 2) AS original_order_amount,
    toDecimal64(sum(o.current_total_amount), 2) AS current_order_amount,
    toDecimal64(
        sumIf(o.current_total_amount, o.is_completed_order = 1),
        2
    ) AS completed_current_amount,
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
INNER JOIN changed_reconciliation_keys AS changed
    ON o.order_date = changed.order_date
    AND o.currency = changed.currency
GROUP BY
    o.order_date,
    o.currency
