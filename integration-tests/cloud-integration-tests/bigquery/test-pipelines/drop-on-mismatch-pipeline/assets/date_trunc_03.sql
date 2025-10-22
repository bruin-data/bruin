/* @bruin
name: bq_test.date_trunc_03
type: bq.sql
materialization:
  type: table
  strategy: create+replace
  partition_by: DATETIME_TRUNC(date_timestamp, YEAR)

columns:
  - name: date_timestamp
    type: DATETIME
  - name: created_at
    type: DATE

@bruin */

SELECT CAST(current_timestamp() as DATETIME) as date_timestamp,
DATE(TIMESTAMP_SECONDS({{start_date}})) as created_at