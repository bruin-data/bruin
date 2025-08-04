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
    type: NUMERIC
    description: "Sample numeric value"
  - name: category
    type: VARCHAR
    description: "Sample category field"
@bruin */


-- If metadata is pushed correctly, this SELECT statement will not be included in the output.
SELECT
    3 AS id, 'Charlie' AS name, 150 AS value, 'A' AS category
