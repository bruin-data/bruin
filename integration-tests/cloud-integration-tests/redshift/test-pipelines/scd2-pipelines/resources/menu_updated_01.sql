/* @bruin
name: test.menu
type: rs.sql
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
    type: INTEGER
    description: "Price of the Menu Item in cents"
@bruin */


SELECT 1 AS ID, 'Cola' AS Name, 799 AS Price
UNION ALL
SELECT 2 AS ID, 'Tea' AS Name, 499 AS Price
UNION ALL
SELECT 4 AS ID, 'Fanta' AS Name, 1.99 AS Price
