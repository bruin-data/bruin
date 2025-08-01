/* @bruin
name: dataset.sample_data
type: duckdb.sql

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
UNION ALL
SELECT
    5 AS id, 'Eve' AS name, 250.80 AS value, 'B' AS category
UNION ALL
SELECT
    6 AS id, 'Frank' AS name, 175.90 AS value, 'A' AS category
UNION ALL
SELECT
    7 AS id, 'Grace' AS name, 400.00 AS value, 'C' AS category
UNION ALL
SELECT
    8 AS id, 'Henry' AS name, 125.60 AS value, 'B' AS category
UNION ALL
SELECT
    9 AS id, 'Ivy' AS name, 350.45 AS value, 'A' AS category
UNION ALL
SELECT
    10 AS id, 'Jack' AS name, 275.30 AS value, 'C' AS category; 