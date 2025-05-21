/* @bruin
name: my_schema.products
type: duckdb.sql

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
    primary_key: true
  - name: price
    type: FLOAT
    description: "Price of the product in USD"
  - name: stock
    type: INTEGER
    description: "Number of units in stock"

@bruin */
