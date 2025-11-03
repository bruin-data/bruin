/* @bruin

name: bq_test.compose
type: bq.sql

materialization:
  type: table
  strategy: create+replace
  partition_by: date_Trunc(date_timestamp, YEAR)

columns:
  - name: date_timestamp
    type: DATE
  - name: created_at
    type: DATE

@bruin */

SELECT
    current_date() AS date_timestamp,
    date(timestamp_seconds({{ start_date }})) AS created_at
