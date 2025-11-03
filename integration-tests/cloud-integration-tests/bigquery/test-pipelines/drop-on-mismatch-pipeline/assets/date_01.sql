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

SELECT
    current_date() AS date_timestamp,
    date(timestamp_seconds({{ start_date }})) AS created_at
