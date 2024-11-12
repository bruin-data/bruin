/* @bruin

name: discounts
type: bq.sql

materialization:
   type: table

description: |
  # Products table
  This asset represents the Shopify products data.

  You can write Markdown here, it supports `inline codeblocks` or larger blocks of code. It supports **bold** and *italic* text.

depends:
  - shopify_raw.discounts

columns:
  - name: id
    type: integer
    description: "Product unique identifier"
    primary_key: true
    checks:
        - name: not_null
        - name: positive

  - name: discount
    type: json
    description: "Discount information"

  - name: metafields_first250
    type: json
    description: "First 250 metafields"

@bruin */

SELECT * from shopify_raw.discounts;
