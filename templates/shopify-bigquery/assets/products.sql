/* @bruin

name: products
type: bq.sql

materialization:
   type: table

description: |
  # Products table
  This asset represents the Shopify products data.

  You can write Markdown here, it supports `inline codeblocks` or larger blocks of code. It supports **bold** and *italic* text.

depends:
  - shopify_raw.products
columns:
  - name: id
    type: integer
    description: "Product unique identifier"
    primary_key: true
    checks:
        - name: not_null
        - name: positive

  - name: available_publications_count
    type: json
    description: "Count of available publications"
    checks:
        - name: not_null

  - name: category
    type: json
    description: "Product category information"

  - name: combined_listing
    type: json
    description: "Combined listing information"

  - name: combined_listing_role
    type: json
    description: "Combined listing role information"

  - name: compare_at_price_range
    type: json
    description: "Product compare at price range"

  - name: created_at
    type: timestamp
    description: "Timestamp when product was created"
    checks:
        - name: not_null

  - name: default_cursor
    type: varchar
    description: "Default cursor for the product"
    checks:
        - name: not_null

  - name: description
    type: varchar
    description: "Product description"
    checks:
        - name: not_null

  - name: description_html
    type: varchar
    description: "HTML formatted product description"
    checks:
        - name: not_null

  - name: handle
    type: varchar
    description: "Product handle/slug"
    checks:
        - name: not_null

  - name: metafields
    type: json
    description: "Product metafields"

  - name: options
    type: json
    description: "Product options"

  - name: price_range_v2
    type: json
    description: "Product price range information"
    checks:
        - name: not_null

  - name: product_type
    type: varchar
    description: "Type of product"
    checks:
        - name: not_null

  - name: published_at
    type: timestamp
    description: "Timestamp when product was published"

  - name: requires_selling_plan
    type: boolean
    description: "Whether product requires a selling plan"

  - name: status
    type: varchar
    description: "Product status"
    checks:
        - name: not_null

  - name: tags
    type: varchar
    description: "Product tags"

  - name: template_suffix
    type: varchar
    description: "Product template suffix"

  - name: title
    type: varchar
    description: "Product title"
    checks:
        - name: not_null

  - name: total_inventory
    type: integer
    description: "Total product inventory"
    checks:
        - name: not_null

  - name: tracks_inventory
    type: boolean
    description: "Whether product tracks inventory"
    checks:
        - name: not_null

  - name: updated_at
    type: timestamp
    description: "Timestamp when product was last updated"
    checks:
        - name: not_null

  - name: variants_first250
    type: json
    description: "First 250 product variants"
    checks:
        - name: not_null

  - name: variants_count
    type: json
    description: "Count of product variants"
    checks:

@bruin */

SELECT * from shopify_raw.products;
