/* @bruin
name: bq_test.date_01
type: bq.sql
materialization:
  type: table
  strategy: create+replace
  partition_by: date_timestamp

columns:
  - name: date_timestamp
    type: DATE
  - name: created_at
    type: DATE


@bruin */

SELECT current_date() as date_timestamp,
DATE(TIMESTAMP_SECONDS({{start_date}})) as created_at