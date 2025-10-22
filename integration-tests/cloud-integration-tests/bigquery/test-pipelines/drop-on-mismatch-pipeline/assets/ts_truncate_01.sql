/* @bruin
name: bq_test.ts_truncate_01
type: bq.sql
materialization:
  type: table
  strategy: create+replace
  partition_by: TIMESTAMP_TRUNC(date_timestamp, HOUR)

columns:
  - name: date_timestamp
    type: TIMESTAMP
  - name: created_at
    type: DATE


@bruin */

SELECT current_timestamp() as date_timestamp,
DATE(TIMESTAMP_SECONDS({{start_date}})) as created_at