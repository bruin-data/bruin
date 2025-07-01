/* @bruin
name: test.menu
type: sf.sql
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
  - name: Price
    type: FLOAT
    description: "Price of the Menu Item"
@bruin */



SELECT 1 AS ID, 'Cola' AS Name, 3.99 AS Price
UNION ALL
SELECT 2 AS ID, 'Tea' AS Name, 4.99 AS Price
UNION ALL
SELECT 3 AS ID, 'Coffee' AS Name, 5.99 AS Price

