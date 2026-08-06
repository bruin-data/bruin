/* @bruin
name: shopify.t2_products
type: clickhouse.sql
description: "Conformed Shopify product catalog enriched with variant pricing and current inventory."

materialization:
  type: table
  strategy: merge

depends:
  - shopify.t1_products
  - shopify.t2_inventory_items

tags:
  - t2
  - conformed
  - shopify
  - products
domains:
  - commerce
  - merchandising
  - fulfillment
meta:
  grain: one row per Shopify product
  source_system: shopify
  currency_scope: catalog price and cost values use shop currency
  cost_policy: average unit cost and inventory value include only inventory items with a Shopify cost
  refresh_strategy: primary-key merge for changed products and products with changed inventory
  data_classification: internal

custom_checks:
  - name: product catalog is preserved
    description: "Ensures every raw Shopify product appears in the conformed catalog."
    query: |
      SELECT
        (SELECT count() FROM shopify.t2_products)
        =
        (SELECT count() FROM shopify.t1_products)
    value: 1
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
    description: "Legacy numeric Shopify identifier parsed from the global identifier."
  - name: product_title
    type: String
    description: "Merchant-facing product title."
  - name: handle
    type: String
    description: "URL-safe Shopify product handle."
  - name: product_type
    type: LowCardinality(String)
    description: "Merchant-defined product type."
  - name: vendor
    type: LowCardinality(String)
    description: "Vendor assigned to the product."
  - name: status
    type: LowCardinality(String)
    description: "Current Shopify product status."
  - name: is_active
    type: UInt8
    description: "One when the Shopify product status is active."
  - name: catalog_currency
    type: LowCardinality(String)
    description: "ISO shop-currency code attached to Shopify's product price range."
  - name: minimum_variant_price
    type: Decimal(18, 2)
    description: "Minimum current variant price in shop currency."
    checks:
      - name: non_negative
  - name: maximum_variant_price
    type: Decimal(18, 2)
    description: "Maximum current variant price in shop currency."
    checks:
      - name: non_negative
  - name: variant_count
    type: UInt64
    description: "Number of variants reported by Shopify."
  - name: inventory_item_count
    type: UInt64
    description: "Number of ingested inventory items associated with the product."
  - name: tracked_inventory_item_count
    type: UInt64
    description: "Number of associated inventory items with tracking enabled."
  - name: costed_inventory_item_count
    type: UInt64
    description: "Number of associated inventory items with a non-null Shopify unit cost."
  - name: inventory_quantity
    type: Int64
    description: "Current tracked inventory quantity summed across product inventory items."
  - name: sellable_online_quantity
    type: Int64
    description: "Current sellable-online quantity summed across product inventory items."
  - name: average_unit_cost
    type: Nullable(Float64)
    description: "Average Shopify unit cost across inventory items where cost is available."
  - name: known_cost_inventory_value
    type: Decimal(18, 2)
    description: "Inventory quantity multiplied by unit cost for costed inventory items only."
  - name: inventory_status
    type: LowCardinality(String)
    description: "Derived stock band: out_of_stock, low_stock, or in_stock."
    checks:
      - name: accepted_values
        value: ["out_of_stock", "low_stock", "in_stock"]
  - name: tracks_inventory
    type: Bool
    description: "Whether Shopify reports inventory tracking for the product."
  - name: created_at
    type: Nullable(DateTime64(6, 'UTC'))
    description: "Timestamp when the product was created in Shopify."
  - name: published_at
    type: Nullable(DateTime64(6, 'UTC'))
    description: "Timestamp when the product was first published."
  - name: product_updated_at
    type: Nullable(DateTime64(6, 'UTC'))
    description: "Timestamp when Shopify last updated the product."
  - name: source_max_updated_at
    type: DateTime64(6, 'UTC')
    description: "Latest product or inventory update timestamp contributing to the row."
@bruin */

WITH changed_products AS (
    SELECT id AS product_id
    FROM shopify.t1_products
    WHERE updated_at BETWEEN
        parseDateTime64BestEffort('{{ start_timestamp }}', 6, 'UTC')
        AND parseDateTime64BestEffort('{{ end_timestamp }}', 6, 'UTC')
    UNION DISTINCT
    SELECT product_id
    FROM shopify.t2_inventory_items
    WHERE updated_at BETWEEN
        parseDateTime64BestEffort('{{ start_timestamp }}', 6, 'UTC')
        AND parseDateTime64BestEffort('{{ end_timestamp }}', 6, 'UTC')
),
inventory_summary AS (
    SELECT
        inv.product_id,
        count() AS inventory_item_count,
        countIf(inv.tracked) AS tracked_inventory_item_count,
        countIf(inv.unit_cost IS NOT NULL) AS costed_inventory_item_count,
        sum(inv.inventory_quantity) AS inventory_quantity,
        sum(inv.sellable_online_quantity) AS sellable_online_quantity,
        avgOrNull(toFloat64(inv.unit_cost)) AS average_unit_cost,
        toDecimal64(
            sum(ifNull(toFloat64(inv.unit_cost), 0) * toFloat64(inv.inventory_quantity)),
            2
        ) AS known_cost_inventory_value,
        max(inv.updated_at) AS inventory_updated_at
    FROM shopify.t2_inventory_items AS inv
    GROUP BY inv.product_id
)
SELECT
    p.id AS product_id,
    toUInt64OrZero(arrayElement(splitByChar('/', p.id), -1)) AS product_legacy_id,
    ifNull(p.title, '') AS product_title,
    ifNull(p.handle, '') AS handle,
    toLowCardinality(ifNull(p.product_type, '')) AS product_type,
    toLowCardinality(ifNull(p.vendor, '')) AS vendor,
    toLowCardinality(lower(ifNull(p.status, 'unknown'))) AS status,
    toUInt8(upper(ifNull(p.status, '')) = 'ACTIVE') AS is_active,
    toLowCardinality(
        JSONExtractString(
            ifNull(p.price_range_v2, '{}'),
            'minVariantPrice',
            'currencyCode'
        )
    ) AS catalog_currency,
    toDecimal64OrZero(
        JSONExtractString(
            ifNull(p.price_range_v2, '{}'),
            'minVariantPrice',
            'amount'
        ),
        2
    ) AS minimum_variant_price,
    toDecimal64OrZero(
        JSONExtractString(
            ifNull(p.price_range_v2, '{}'),
            'maxVariantPrice',
            'amount'
        ),
        2
    ) AS maximum_variant_price,
    toUInt64(
        JSONExtractInt(ifNull(p.variants_count, '{}'), 'count')
    ) AS variant_count,
    toUInt64(ifNull(i.inventory_item_count, 0)) AS inventory_item_count,
    toUInt64(ifNull(i.tracked_inventory_item_count, 0)) AS tracked_inventory_item_count,
    toUInt64(ifNull(i.costed_inventory_item_count, 0)) AS costed_inventory_item_count,
    toInt64(ifNull(i.inventory_quantity, 0)) AS inventory_quantity,
    toInt64(ifNull(i.sellable_online_quantity, 0)) AS sellable_online_quantity,
    i.average_unit_cost,
    ifNull(i.known_cost_inventory_value, toDecimal64(0, 2)) AS known_cost_inventory_value,
    toLowCardinality(
        multiIf(
            ifNull(i.sellable_online_quantity, 0) <= 0, 'out_of_stock',
            ifNull(i.sellable_online_quantity, 0) <= 10, 'low_stock',
            'in_stock'
        )
    ) AS inventory_status,
    toBool(ifNull(p.tracks_inventory, false)) AS tracks_inventory,
    p.created_at,
    p.published_at,
    p.updated_at AS product_updated_at,
    greatest(
        ifNull(p.updated_at, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')),
        ifNull(i.inventory_updated_at, toDateTime64('1970-01-01 00:00:00', 6, 'UTC'))
    ) AS source_max_updated_at
FROM shopify.t1_products AS p
INNER JOIN changed_products AS c
    ON p.id = c.product_id
LEFT JOIN inventory_summary AS i
    ON p.id = i.product_id
