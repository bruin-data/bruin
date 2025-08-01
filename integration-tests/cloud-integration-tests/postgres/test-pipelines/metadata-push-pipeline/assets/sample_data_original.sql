/* @bruin
name: test_metadata.sample_data
type: pg.sql

materialization:
  type: table

columns:
  - name: id
    type: INTEGER
    description: "empty description"
    primary_key: true
  - name: name
    type: VARCHAR
    description: "empty description"
  - name: value
    type: FLOAT
    description: "empty description"
  - name: category
    type: VARCHAR
    description: "empty description"
@bruin */

SELECT
    1 AS id, 'Alice' AS name, 100.50 AS value, 'A' AS category
UNION ALL
SELECT
    2 AS id, 'Bob' AS name, 200.75 AS value, 'B' AS category
UNION ALL
SELECT
    3 AS id, 'Charlie' AS name, 150.25 AS value, 'A' AS category
UNION ALL
SELECT
    4 AS id, 'Diana' AS name, 300.00 AS value, 'C' AS category