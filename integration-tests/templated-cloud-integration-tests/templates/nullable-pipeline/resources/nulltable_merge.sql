/* @bruin
name: dataset.nulltable
type: bq.sql

materialization:
  type: table
  strategy: merge

columns:
  - name: id
    type: INTEGER
    primary_key: true
  - name: value
    type: varchar
    update_on_merge: true
@bruin */

SELECT
    1 AS id, 'k' AS value
UNION ALL
SELECT
    3 AS id, 'l' AS value
UNION ALL
SELECT
    NULL AS id, 'm' AS value
