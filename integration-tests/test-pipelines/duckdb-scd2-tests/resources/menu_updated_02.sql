/* @bruin
name: test.menu
type: duckdb.sql
materialization:
  type: table
  strategy: scd2_by_column

columns:
  - name: ID
    type: INTEGER
    description: "Unique identifier for Menu Item"
    primary_key: true
    checks:
      - name: not_null
      - name: positive
  - name: Name
    type: VARCHAR
    description: "Name of the Menu Item"
    checks:
      - name: not_null
    primary_key: true
  - name: Price
    type: FLOAT
    description: "Price of the Menu Item"
@bruin */


SELECT 1 AS ID, 'Cola' AS Name, 0.99 AS Price

