/* @bruin

name: inventory_items
type: bq.sql

materialization:
   type: table

description: |
  # Products table
  This asset represents the Shopify products data.

  You can write Markdown here, it supports `inline codeblocks` or larger blocks of code. It supports **bold** and *italic* text.

depends:
  - shopify_raw.inventory_items

columns:
  - name: id
    type: string
    description: "Unique identifier for the inventory item"
    primary_key: true
    checks:
        - name: not_null

  - name: created_at
    type: timestamp
    description: "When the inventory item was created"

  - name: duplicate_sku_count
    type: integer
    description: "Count of duplicate SKUs"

  - name: legacy_resource_id
    type: string
    description: "Legacy identifier for the resource"

  - name: measurement
    type: json
    description: "Measurement details of the inventory item"

  - name: requires_shipping
    type: boolean
    description: "Whether the item requires shipping"

  - name: sku
    type: string
    description: "Stock Keeping Unit identifier"

  - name: tracked
    type: boolean
    description: "Whether inventory tracking is enabled for this item"

  - name: tracked_editable
    type: json
    description: "Tracking edit settings"

  - name: updated_at
    type: timestamp
    description: "When the inventory item was last updated"

  - name: variant
    type: json
    description: "Product variant details"
@bruin */

SELECT * from `shopify_raw.inventory_items`;
