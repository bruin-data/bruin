/* @bruin
name: bq_test.ts
type: bq.sql
materialization:
  type: table
  strategy: create+replace
  partition_by: TIMESTAMP_TRUNC(ts, DAY)

columns:
  - name: ts
    type: TIMESTAMP
  - name: created_at
    type: DATE

@bruin */

SELECT current_timestamp() as ts,
DATE(TIMESTAMP_SECONDS({{start_date}})) as created_at