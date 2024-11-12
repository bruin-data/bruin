/* @bruin

name: price_rules
type: bq.sql

materialization:
   type: table

description: |
  # Products table
  This asset represents the Shopify products data.

  You can write Markdown here, it supports `inline codeblocks` or larger blocks of code. It supports **bold** and *italic* text.

depends:
  - shopify_raw.price_rules

columns:
  - name: id
    type: integer
    description: "Unique identifier for the price rule"
    primary_key: true
    checks:
        - name: not_null

  - name: value_type
    type: string
    description: "Type of value for the price rule"

  - name: value
    type: string
    description: "Value of the price rule"

  - name: customer_selection
    type: string
    description: "Customer selection criteria"

  - name: target_type
    type: string
    description: "Type of target for the price rule"

  - name: target_selection
    type: string
    description: "Target selection criteria"

  - name: allocation_method
    type: string
    description: "Method of allocation for the price rule"

  - name: once_per_customer
    type: boolean
    description: "Whether the price rule can be used once per customer"

  - name: starts_at
    type: timestamp
    description: "Start time of the price rule"

  - name: ends_at
    type: timestamp
    description: "End time of the price rule"

  - name: created_at
    type: timestamp
    description: "When the price rule was created"

  - name: updated_at
    type: timestamp
    description: "When the price rule was last updated"

  - name: entitled_product_ids
    type: json
    description: "List of entitled product IDs"

  - name: entitled_variant_ids
    type: json
    description: "List of entitled variant IDs"

  - name: entitled_collection_ids
    type: json
    description: "List of entitled collection IDs"

  - name: entitled_country_ids
    type: json
    description: "List of entitled country IDs"

@bruin */

SELECT * from shopify_raw.price_rules;
