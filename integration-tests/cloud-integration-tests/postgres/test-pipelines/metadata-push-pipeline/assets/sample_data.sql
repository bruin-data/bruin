/* @bruin
name: test_metadata.sample_data
type: pg.sql
description: "A sample table"

materialization:
  type: table

columns:
  - name: id
    type: INTEGER
    description: "Unique identifier"
    primary_key: true
  - name: name
    type: VARCHAR
    description: "Sample name field"
  - name: value
    type: NUMERIC
    description: "Sample numeric value"
  - name: category
    type: VARCHAR
    description: "Sample category field"
@bruin */

SELECT
    1 AS id, 'Alice' AS name, 100 AS value, 'A' AS category
UNION ALL
SELECT
    2 AS id, 'Bob' AS name, 200 AS value, 'B' AS category