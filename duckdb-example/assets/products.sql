/* @bruin
name: test.products_02
type: sf.sql

materialization:
  type: table
  strategy: ddl
  cluster_by:
    - product_id

columns:
  - name: product_id
    type: INTEGER
    description: "Unique identifier for the product"
    primary_key: true
  - name: product_name
    type: VARCHAR
    description: "Name of the product"
  - name: price
    type: FLOAT
    description: "Price of the product in USD"
    checks:
      - name: positive
  - name: stock
    type: INTEGER
    description: "Number of units in stock"
@bruin */

