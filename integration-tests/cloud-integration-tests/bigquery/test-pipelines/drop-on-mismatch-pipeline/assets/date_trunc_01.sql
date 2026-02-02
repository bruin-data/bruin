/* @bruin
name: cloud_integration_test.drop_on_mismatch_date_trunc_01
type: bq.sql
materialization:
  type: table
  strategy: create+replace
  partition_by: DATETIME_TRUNC(date_timestamp, DAY)

columns:
  - name: date_timestamp
    type: DATETIME
  - name: created_at
    type: DATE

@bruin */

SELECT CAST(current_timestamp() as DATETIME) as date_timestamp,
DATE(TIMESTAMP_SECONDS({{start_date}})) as created_at