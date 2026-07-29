/* @bruin
name: shopify.t2_inventory_items
type: clickhouse.sql
description: "Conformed Shopify inventory items with product, variant, price, cost, and sellable quantity attributes."

materialization:
  type: table
  strategy: merge

depends:
  - shopify.t1_inventory_items

tags:
  - t2
  - conformed
  - shopify
  - inventory
domains:
  - commerce
  - merchandising
  - fulfillment
meta:
  grain: one row per Shopify inventory item
  source_system: shopify
  refresh_strategy: primary-key merge for source records updated in the run interval
  data_classification: internal

columns:
  - name: inventory_item_id
    type: String
    description: "Shopify Admin GraphQL global identifier for the inventory item."
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: inventory_item_legacy_id
    type: UInt64
    description: "Legacy numeric Shopify identifier for the inventory item."
  - name: product_id
    type: String
    description: "Shopify Admin GraphQL global identifier for the parent product."
  - name: variant_id
    type: String
    description: "Shopify Admin GraphQL global identifier for the product variant."
  - name: variant_legacy_id
    type: UInt64
    description: "Legacy numeric Shopify identifier for the product variant."
  - name: sku
    type: String
    description: "Merchant-defined stock-keeping unit."
  - name: variant_title
    type: String
    description: "Merchant-facing product variant title."
  - name: variant_price
    type: Decimal(18, 2)
    description: "Current variant price in shop currency."
    checks:
      - name: non_negative
  - name: unit_cost
    type: Nullable(Decimal(18, 2))
    description: "Unit cost recorded in Shopify, when available."
  - name: inventory_quantity
    type: Int64
    description: "Current tracked inventory quantity across locations."
  - name: sellable_online_quantity
    type: Int64
    description: "Quantity Shopify reports as sellable online."
  - name: available_for_sale
    type: Bool
    description: "Whether Shopify reports the variant as available for sale."
  - name: tracked
    type: Bool
    description: "Whether Shopify tracks inventory for the item."
  - name: requires_shipping
    type: Bool
    description: "Whether the inventory item represents a physical item requiring shipping."
  - name: location_count
    type: UInt64
    description: "Number of inventory locations returned for the item."
  - name: created_at
    type: Nullable(DateTime64(6, 'UTC'))
    description: "Timestamp when the inventory item was created in Shopify."
  - name: updated_at
    type: Nullable(DateTime64(6, 'UTC'))
    description: "Timestamp when the inventory item was last updated in Shopify."
@bruin */

SELECT
    id AS inventory_item_id,
    toUInt64OrZero(ifNull(legacy_resource_id, '')) AS inventory_item_legacy_id,
    JSONExtractString(ifNull(variant, '{}'), 'product', 'id') AS product_id,
    JSONExtractString(ifNull(variant, '{}'), 'id') AS variant_id,
    toUInt64OrZero(
        JSONExtractString(ifNull(variant, '{}'), 'legacyResourceId')
    ) AS variant_legacy_id,
    ifNull(sku, '') AS sku,
    JSONExtractString(ifNull(variant, '{}'), 'title') AS variant_title,
    toDecimal64OrZero(
        JSONExtractString(ifNull(variant, '{}'), 'price'),
        2
    ) AS variant_price,
    toDecimal64OrNull(toString(cost), 2) AS unit_cost,
    toInt64(
        JSONExtractInt(ifNull(variant, '{}'), 'inventoryQuantity')
    ) AS inventory_quantity,
    toInt64(
        JSONExtractInt(ifNull(variant, '{}'), 'sellableOnlineQuantity')
    ) AS sellable_online_quantity,
    toBool(
        JSONExtractBool(ifNull(variant, '{}'), 'availableForSale')
    ) AS available_for_sale,
    toBool(ifNull(tracked, false)) AS tracked,
    toBool(ifNull(requires_shipping, false)) AS requires_shipping,
    toUInt64(JSONLength(ifNull(inventory_levels, '[]'))) AS location_count,
    created_at,
    updated_at
FROM shopify.t1_inventory_items
WHERE updated_at BETWEEN
    parseDateTime64BestEffort('{{ start_timestamp }}', 6, 'UTC')
    AND parseDateTime64BestEffort('{{ end_timestamp }}', 6, 'UTC')
