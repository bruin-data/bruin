/* @bruin

name: bq_test.date_trunc_02
type: bq.sql

materialization:
  type: table
  strategy: create+replace
  partition_by: DATETIME_TRUNC(date_timestamp, MONTH)

columns:
  - name: date_timestamp
    type: DATETIME
  - name: created_at
    type: DATE

@bruin */

SELECT
    CAST(CURRENT_TIMESTAMP() AS DATETIME) AS date_timestamp,
    DATE(TIMESTAMP_SECONDS({{ start_date }})) AS created_at
