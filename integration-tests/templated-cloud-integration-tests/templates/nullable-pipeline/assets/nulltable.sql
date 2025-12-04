/* @bruin
name: dataset.nulltable
type: bq.sql

materialization:
  type: table

columns:
  - name: id
    type: INTEGER
    primary_key: true
  - name: value
    type: varchar

@bruin */

SELECT
    1 AS id, 'x' AS value
UNION ALL
SELECT
    2 AS id, 'y' AS value
UNION ALL
SELECT
    NULL AS id, 'z' AS value 