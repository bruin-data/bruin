/* @bruin

name: test.products_ddl
type: databricks.sql

materialization:
    type: table
    strategy: ddl

columns:
  - name: product_id
    type: INTEGER
    description: "Unique identifier for the product"
    primary_key: true
  - name: product_name
    type: VARCHAR
    description: "Name of the product"
  - name: price
    type: INTEGER
    description: "Price of the product in cents"
  - name: stock
    type: INTEGER
    description: "Number of units in stock"
  - name: category
    type: VARCHAR
    description: "Product category"

@bruin */

