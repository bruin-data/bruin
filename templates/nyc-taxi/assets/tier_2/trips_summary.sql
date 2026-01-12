/* @bruin
name: tier_2.trips_summary
type: duckdb.sql
description: |
  Transforms and cleans raw trip data from tier_1.
  Deduplicates trips, selects necessary columns, and joins with the taxi zone lookup table
  to enrich data with borough and zone names.
  Aggregation Level: Individual trip records with location enrichment and deduplication applied.

  Sample query:
  ```sql
  SELECT *
  FROM tier_2.trips_summary
  WHERE 1=1
    AND pickup_borough = 'Manhattan'
  LIMIT 10
  ```

depends:
  - tier_1.taxi_zone_lookup
  - tier_1.trips_historic
  - tier_1.payment_lookup

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
    description: Timestamp when the data was last updated in tier_2

@bruin */

WITH

raw_trips AS ( -- Step 1: Select necessary columns from tier_1 and apply data quality filters
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
    payment_type,
    extracted_at,
  FROM tier_1.trips_historic
  WHERE 1=1
    AND DATE_TRUNC('month', pickup_time) BETWEEN DATE_TRUNC('month', CAST('{{ start_datetime }}' AS TIMESTAMP)) AND DATE_TRUNC('month', CAST('{{ end_datetime }}' AS TIMESTAMP))
    AND pickup_time IS NOT NULL
    AND dropoff_time IS NOT NULL
    AND pickup_location_id IS NOT NULL
    AND dropoff_location_id IS NOT NULL
    AND taxi_type IS NOT NULL
)

, cleaned_trips AS ( -- Step 2: Deduplicate trips using QUALIFY and calculate trip duration in seconds
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
    payment_type,
    extracted_at,
    EXTRACT(EPOCH FROM (dropoff_time - pickup_time)) AS trip_duration_seconds,
  FROM raw_trips
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY pickup_time, dropoff_time, pickup_location_id, dropoff_location_id, taxi_type
    ORDER BY pickup_time DESC
  ) = 1
)

, trips_with_lookup AS ( -- Step 3: Enriches trips with pickup location information using LEFT JOIN with taxi_zone_lookup table
  SELECT
    ct.*,
    pickup_lookup.borough AS pickup_borough,
    pickup_lookup.zone AS pickup_zone,
  FROM cleaned_trips AS ct
  LEFT JOIN tier_1.taxi_zone_lookup AS pickup_lookup
    ON ct.pickup_location_id = pickup_lookup.location_id
)

, trips_with_payment AS ( -- Step 4: Enriches trips with payment type information using LEFT JOIN with payment_lookup table
  SELECT
    twl.pickup_time,
    twl.dropoff_time,
    twl.pickup_location_id,
    twl.dropoff_location_id,
    twl.taxi_type,
    twl.trip_distance,
    twl.passenger_count,
    twl.fare_amount,
    twl.tip_amount,
    twl.total_amount,
    twl.pickup_borough,
    twl.pickup_zone,
    dropoff_lookup.borough AS dropoff_borough,
    dropoff_lookup.zone AS dropoff_zone,
    twl.trip_duration_seconds,
    twl.payment_type,
    payment_lookup.payment_description,
    twl.extracted_at,
  FROM trips_with_lookup AS twl
  LEFT JOIN tier_1.taxi_zone_lookup AS dropoff_lookup
    ON twl.dropoff_location_id = dropoff_lookup.location_id
  LEFT JOIN tier_1.payment_lookup AS payment_lookup
    ON CAST(twl.payment_type AS INTEGER) = payment_lookup.payment_type_id
  WHERE 1=1
    -- filter out zero durations (trip cannot end at the same time it starts or before it starts)
    AND trip_duration_seconds > 0
    -- filter out outlier durations that are too long, 8 hours (28800 seconds)
    AND trip_duration_seconds < 28800
    -- filter out negative total amounts
    AND total_amount >= 0
    -- Only include trips that were actually charged
    AND payment_lookup.payment_description IN ('flex_fare', 'credit_card', 'cash')
    -- filter out negative trip distances as they are data quality issues (trip distance cannot be negative)
    AND trip_distance >= 0
)

, final AS ( -- Step 5: Final select with all required columns
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
    CURRENT_TIMESTAMP AS updated_at,
  FROM trips_with_payment
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
FROM final
