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
    checks:
      - name: positive
  - name: stock
    type: INTEGER
    description: "Number of units in stock"

custom_checks:
    - name: check_for_cols
      value: 4
      query: SELECT COUNT(*) AS column_count
             FROM information_schema.columns
             WHERE table_name = 'products'


@bruin */