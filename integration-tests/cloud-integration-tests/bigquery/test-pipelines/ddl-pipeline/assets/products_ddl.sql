/* @bruin
name: test.products_ddl
type: bq.sql

materialization:
  type: table
  strategy: ddl

columns:
  - name: product_id
    type: INT64
    description: "Unique identifier for the product"
    primary_key: true
  - name: product_name
    type: STRING
    description: "Name of the product"
  - name: price
    type: INT64
    description: "Price of the product in cents"
  - name: stock
    type: INT64
    description: "Number of units in stock"
  - name: category
    type: STRING
    description: "Product category"
@bruin */

