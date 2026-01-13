/* @bruin
name: reports.report_trips_monthly
type: duckdb.sql
description: |
  Monthly summary report of NYC taxi trips aggregated by taxi type and month.
  Calculates average and total metrics for trip duration, total amount, and tip amount,
  as well as total trip count.
  Aggregation Level: Monthly aggregates by taxi type (one row per taxi_type per month).

  Sample query:
  ```sql
  SELECT *
  FROM reports.report_trips_monthly
  WHERE 1=1
    AND taxi_type = 'yellow'
    AND month_date >= '2022-01-01'
  ORDER BY month_date DESC
  ```

depends:
  - staging.trips_summary

materialization:
  type: table
  strategy: time_interval
  incremental_key: month_date
  time_granularity: timestamp

columns:
  - name: taxi_type
    type: VARCHAR
    description: Type of taxi (yellow or green)
    primary_key: true
    nullable: false
  - name: month_date
    type: DATE
    description: First day of the month for which the report is generated
    primary_key: true
    nullable: false
  - name: trip_duration_avg
    type: DOUBLE
    description: Average trip duration in seconds for the month
  - name: trip_duration_total
    type: DOUBLE
    description: Total trip duration in seconds for the month
  - name: total_amount_avg
    type: DOUBLE
    description: Average total amount charged to passengers for the month
  - name: total_amount_total
    type: DOUBLE
    description: Total amount charged to passengers for the month
    checks:
      - name: non_negative
  - name: tip_amount_avg
    type: DOUBLE
    description: Average tip amount for the month
  - name: tip_amount_total
    type: DOUBLE
    description: Total tip amount for the month
  - name: total_trips
    type: BIGINT
    description: Total number of trips for the month
    checks:
      - name: positive
  - name: extracted_at
    type: TIMESTAMP
    description: Maximum timestamp when the source data was extracted (latest extraction time for the month)
  - name: updated_at
    type: TIMESTAMP
    description: Timestamp when the data was last updated in reports

custom_checks:
  - name: positive_trip_count
    description: Validates total_trips count is positive for each month
    query: SELECT COUNT(*) FROM reports.report_trips_monthly WHERE total_trips <= 0
    value: 0
  - name: non_negative_revenue
    description: Ensures aggregated total_amount_total is non-negative
    query: SELECT COUNT(*) FROM reports.report_trips_monthly WHERE total_amount_total < 0
    value: 0

@bruin */

WITH

trips_by_month AS ( -- Step 1: Extract month from pickup_time and prepare data for aggregation, filtering for only charged trips
  SELECT
    taxi_type,
    DATE_TRUNC('month', pickup_time) AS month_date,
    trip_duration_seconds,
    total_amount,
    tip_amount,
    extracted_at,
  FROM staging.trips_summary
  WHERE 1=1
    AND DATE_TRUNC('month', pickup_time) BETWEEN DATE_TRUNC('month', CAST('{{ start_datetime }}' AS TIMESTAMP)) AND DATE_TRUNC('month', CAST('{{ end_datetime }}' AS TIMESTAMP))
    AND trip_duration_seconds IS NOT NULL
    AND total_amount IS NOT NULL
    AND tip_amount IS NOT NULL
    AND dropoff_time > pickup_time
)

, monthly_aggregates AS ( -- Step 2: Aggregate metrics by taxi type and month
  SELECT
    taxi_type,
    month_date,
    AVG(trip_duration_seconds) AS trip_duration_avg,
    SUM(trip_duration_seconds) AS trip_duration_total,
    AVG(total_amount) AS total_amount_avg,
    SUM(total_amount) AS total_amount_total,
    AVG(tip_amount) AS tip_amount_avg,
    SUM(tip_amount) AS tip_amount_total,
    COUNT(*) AS total_trips,
    MAX(extracted_at) AS extracted_at,
  FROM trips_by_month
  GROUP BY
    taxi_type,
    month_date
)

, final AS ( -- Step 3: Final select with all required columns
  SELECT
    taxi_type,
    month_date,
    trip_duration_avg,
    trip_duration_total,
    total_amount_avg,
    total_amount_total,
    tip_amount_avg,
    tip_amount_total,
    total_trips,
    extracted_at,
    CURRENT_TIMESTAMP AS updated_at,
  FROM monthly_aggregates
)

SELECT
  taxi_type,
  month_date,
  trip_duration_avg,
  trip_duration_total,
  total_amount_avg,
  total_amount_total,
  tip_amount_avg,
  tip_amount_total,
  total_trips,
  extracted_at,
  updated_at,
FROM final
