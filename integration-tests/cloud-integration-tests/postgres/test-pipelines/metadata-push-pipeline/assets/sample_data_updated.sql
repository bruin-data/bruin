/* @bruin
name: test_metadata.sample_data
type: pg.sql

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
    type: FLOAT
    description: "Sample numeric value"
  - name: category
    type: VARCHAR
    description: "Sample category field"
@bruin */

SELECT
    3 AS id, 'Charlie' AS name, 150.25 AS value, 'A' AS category
UNION ALL
SELECT
    4 AS id, 'Diana' AS name, 300.00 AS value, 'C' AS category