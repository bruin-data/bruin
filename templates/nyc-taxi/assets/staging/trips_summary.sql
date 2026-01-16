/* @bruin
name: staging.trips_summary
type: duckdb.sql
description: |
  Transforms and cleans raw trip data from raw.
  Normalizes column names (cast, coalesce, rename), selects necessary columns,
  and joins with the taxi zone lookup table to enrich data with borough and zone names.
  Aggregation Level: Individual trip records with location enrichment.

depends:
  - raw.taxi_zone_lookup
  - raw.trips_raw
  - raw.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_time
  time_granularity: timestamp

columns:
  - name: pickup_time
    type: TIMESTAMP
    description: The date and time when the meter was engaged
    primary_key: true
    nullable: false
  - name: dropoff_time
    type: TIMESTAMP
    description: The date and time when the meter was disengaged
    primary_key: true
    nullable: false
  - name: pickup_location_id
    type: INTEGER
    description: TLC Taxi Zone in which the taximeter was engaged
    primary_key: true
    nullable: false
  - name: dropoff_location_id
    type: INTEGER
    description: TLC Taxi Zone in which the taximeter was disengaged
    primary_key: true
    nullable: false
  - name: taxi_type
    type: VARCHAR
    description: Type of taxi (yellow or green)
    primary_key: true
    nullable: false
  - name: trip_distance
    type: DOUBLE
    description: The elapsed trip distance in miles reported by the taximeter
  - name: passenger_count
    type: DOUBLE
    description: The number of passengers in the vehicle
  - name: fare_amount
    type: DOUBLE
    description: The time-and-distance fare calculated by the meter
  - name: tip_amount
    type: DOUBLE
    description: Tip amount (automatically populated for credit card tips, manually entered for cash tips)
  - name: total_amount
    type: DOUBLE
    description: The total amount charged to passengers (does not include cash tips)
    checks:
      - name: non_negative
  - name: pickup_borough
    type: VARCHAR
    description: Borough name where the pickup occurred
  - name: pickup_zone
    type: VARCHAR
    description: Zone name where the pickup occurred
  - name: dropoff_borough
    type: VARCHAR
    description: Borough name where the dropoff occurred
  - name: dropoff_zone
    type: VARCHAR
    description: Zone name where the dropoff occurred
  - name: trip_duration_seconds
    type: DOUBLE
    description: Calculated trip duration in seconds (dropoff time - pickup time)
    checks:
      - name: positive
      - name: max
        value: 86400
  - name: payment_type
    type: DOUBLE
    description: Numeric code signifying how the passenger paid for the trip (0=Flex Fare trip, 1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided trip)
  - name: payment_description
    type: VARCHAR
    description: Human-readable description of the payment type
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when the data was extracted from the source
  - name: updated_at
    type: TIMESTAMP
    description: Timestamp when the data was last updated in staging

custom_checks:
  - name: all_rows_unique
    description: Ensures that each row is unique based on the primary key columns (pickup_time, dropoff_time, pickup_location_id, dropoff_location_id, taxi_type)
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT
          pickup_time,
          dropoff_time,
          pickup_location_id,
          dropoff_location_id,
          taxi_type,
          trip_distance,
          passenger_count,
          fare_amount,
          tip_amount,
          total_amount,
          payment_type
        FROM staging.trips_summary
        GROUP BY ALL
        HAVING COUNT(*) > 1
      )
    value: 0

@bruin */

WITH

normalized_trips AS ( -- Normalize column names from raw data (cast, coalesce, rename)
  SELECT
    vendorid,
    CAST(COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) AS TIMESTAMP) AS pickup_time,
    CAST(COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime) AS TIMESTAMP) AS dropoff_time,
    passenger_count,
    trip_distance,
    store_and_fwd_flag,
    pulocationid AS pickup_location_id,
    dolocationid AS dropoff_location_id,
    CAST(payment_type AS INTEGER) AS payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    taxi_type,
    extracted_at,
  FROM raw.trips_raw
  WHERE 1=1
    AND DATE_TRUNC('month', CAST(COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) AS TIMESTAMP)) BETWEEN DATE_TRUNC('month', CAST('{{ start_datetime }}' AS TIMESTAMP)) AND DATE_TRUNC('month', CAST('{{ end_datetime }}' AS TIMESTAMP))
    AND COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) IS NOT NULL
    AND COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime) IS NOT NULL
    AND pulocationid IS NOT NULL
    AND dolocationid IS NOT NULL
    AND taxi_type IS NOT NULL
)

, enriched_trips AS ( -- Enrich trips with location and payment information using LEFT JOINs
  SELECT
    ct.pickup_time,
    ct.dropoff_time,
    EXTRACT(EPOCH FROM (ct.dropoff_time - ct.pickup_time)) AS trip_duration_seconds,
    ct.pickup_location_id,
    ct.dropoff_location_id,
    ct.taxi_type,
    ct.trip_distance,
    ct.passenger_count,
    ct.fare_amount,
    ct.tip_amount,
    ct.total_amount,
    pl.borough AS pickup_borough,
    pl.zone AS pickup_zone,
    dl.borough AS dropoff_borough,
    dl.zone AS dropoff_zone,
    ct.payment_type,
    pmt.payment_description,
    ct.extracted_at,
    CURRENT_TIMESTAMP AS updated_at,
  FROM normalized_trips AS ct
  LEFT JOIN raw.taxi_zone_lookup AS pl
    ON ct.pickup_location_id = pl.location_id
  LEFT JOIN raw.taxi_zone_lookup AS dl
    ON ct.dropoff_location_id = dl.location_id
  LEFT JOIN raw.payment_lookup AS pmt
    ON ct.payment_type = pmt.payment_type_id
  WHERE 1=1
    -- filter out zero durations (trip cannot end at the same time it starts or before it starts)
    AND EXTRACT(EPOCH FROM (ct.dropoff_time - ct.pickup_time)) > 0
    -- filter out outlier durations that are too long, 8 hours (28800 seconds)
    AND EXTRACT(EPOCH FROM (ct.dropoff_time - ct.pickup_time)) < 28800
    -- filter out negative total amounts
    AND ct.total_amount >= 0
    -- Only include trips that were actually charged
    AND pmt.payment_description IN ('flex_fare', 'credit_card', 'cash')
    -- filter out negative trip distances as they are data quality issues (trip distance cannot be negative)
    AND ct.trip_distance >= 0
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      ct.pickup_time,
      ct.dropoff_time,
      ct.pickup_location_id,
      ct.dropoff_location_id,
      ct.taxi_type,
      ct.trip_distance,
      ct.passenger_count,
      ct.fare_amount,
      ct.tip_amount,
      ct.total_amount,
      ct.payment_type
    ORDER BY ct.extracted_at DESC
  ) = 1
)

SELECT
  pickup_time,
  dropoff_time,
  pickup_location_id,
  dropoff_location_id,
  taxi_type,
  trip_distance,
  passenger_count,
  fare_amount,
  tip_amount,
  total_amount,
  pickup_borough,
  pickup_zone,
  dropoff_borough,
  dropoff_zone,
  trip_duration_seconds,
  payment_type,
  payment_description,
  extracted_at,
  updated_at,
FROM enriched_trips
