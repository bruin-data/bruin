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
    primary_key: true
    checks:
      - name: not_null
  - name: Price
    type: FLOAT
    description: "Price of the Menu Item"
@bruin */


SELECT 1 AS ID, 'Cola' AS Name, 7.99 AS Price
UNION ALL
SELECT 2 AS ID, 'Tea' AS Name, 4.99 AS Price
UNION ALL
SELECT 4 AS ID, 'Fanta' AS Name, 1.99 AS Price
