/* @bruin
name: default.trips_summary_monthly
type: databricks.sql
description: |
  Monthly aggregation of trips data.
  Summarizes total number of trips and total fare amount by month.
  Reads from samples.nyctaxi.trips and stores results in catalog "bruin" and schema "default".

materialization:
  type: table

columns:
  - name: month
    type: DATE
    description: First day of the month (YYYY-MM-01)
  - name: total_trips
    type: BIGINT
    description: Total number of trips in the month
  - name: total_fare_amount
    type: DOUBLE
    description: Sum of all fare amounts in the month
@bruin */

SELECT
    DATE_TRUNC('month', tpep_pickup_datetime) AS month,
    COUNT(*) AS total_trips,
    SUM(fare_amount) AS total_fare_amount
FROM samples.nyctaxi.trips
GROUP BY DATE_TRUNC('month', tpep_pickup_datetime)
ORDER BY month DESC
