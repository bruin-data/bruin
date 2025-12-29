/* @bruin
name: test.datatable
type: databricks.sql

materialization:
  type: table

columns:
  - name: ID
    type: INTEGER
    description: "Unique identifier for Employee"
    primary_key: true
  - name: Name
    type: VARCHAR
    description: "Name of the Employee"
    primary_key: true
@bruin */

SELECT 1 AS ID, 'Colin' AS Name
UNION ALL
SELECT 2 AS ID, 'John' AS Name
UNION ALL
SELECT 4 AS ID, 'Jane' AS Name
