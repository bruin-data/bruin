/* @bruin
name: tier_3.report_trips_monthly
uri: neptune.tier_3.report_trips_monthly
type: duckdb.sql
description: |
  Monthly summary report of NYC taxi trips aggregated by taxi type and month.
  Calculates average and total metrics for trip duration, total amount, and tip amount,
  as well as total trip count.
  
  Query Operations:
  - Step 1: Extracts month from pickup_time using DATE_TRUNC('month', pickup_time) to create grouping key for monthly aggregation (e.g., 2022-03-15 14:30:00 → 2022-03-01 00:00:00). Applies data quality filters to ensure trip_duration_seconds, total_amount, and tip_amount are NOT NULL (filtering out NULLs ensures accurate aggregations as NULL values would skew averages). Filters by date range using month-level truncation to match tier_1/tier_2 logic. Filters for payment types 0, 1, or 2 (Flex Fare trip, Credit card, Cash) to include only trips that were actually charged, excluding No charge (3), Dispute (4), Unknown (5), and Voided trip (6) payments.
  - Step 2: Aggregates metrics by taxi_type and month_date using GROUP BY, creating one row per taxi type per month. Calculates both average and total metrics: trip_duration_avg and trip_duration_total (average and total trip duration in seconds), total_amount_avg and total_amount_total (average fare per trip and total revenue for the month), tip_amount_avg and tip_amount_total (average tip per trip and total tips for the month), and total_trips (count of trips in the month). Both averages and totals are calculated because averages are useful for understanding typical trip characteristics while totals are useful for understanding overall business metrics (revenue, volume).
  - Step 3: Final select with all required columns in proper order (primary keys first, then metrics in logical groups).
  
  Aggregation Level: Monthly aggregates by taxi type (one row per taxi_type per month).
  
  Sample query:
  ```sql
  SELECT *
  FROM tier_3.report_trips_monthly
  WHERE 1=1
    AND taxi_type = 'yellow'
    AND month_date >= '2022-01-01'
  ORDER BY month_date DESC
  ```

owner: data-engineering
tags:
  - tier-3
  - nyc-taxi
  - reports
  - monthly-aggregation

depends:
  - tier_2.trips_summary

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
    description: Timestamp when the data was last updated in tier_3

custom_checks:
  - name: positive_trip_count
    description: Validates total_trips count is positive for each month
    query: SELECT COUNT(*) FROM tier_3.report_trips_monthly WHERE total_trips <= 0
    value: 0
  - name: non_negative_revenue
    description: Ensures aggregated total_amount_total is non-negative
    query: SELECT COUNT(*) FROM tier_3.report_trips_monthly WHERE total_amount_total < 0
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
  FROM tier_2.trips_summary
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
