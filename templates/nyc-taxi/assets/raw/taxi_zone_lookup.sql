/* @bruin
name: raw.taxi_zone_lookup
type: duckdb.sql
description: |
  Loads the NYC taxi zone lookup table from HTTP CSV source.
  This table contains zone information including LocationID, Borough, Zone, and service_zone.
  The lookup table is replaced every time the pipeline runs to ensure it's up to date.

  Design Choices:
  - Why read from HTTP each time:
    - Lookup table may be updated by NYC TLC (new zones, renamed zones, etc.)
    - Refreshing ensures we always have the latest zone information
    - Strategy is truncate+insert, so old data is replaced completely

  - DuckDB read_csv() parameters:
    - header=true: First row contains column names
    - auto_detect=true: Automatically detect column types from data

  - Data Quality Filter:
    - LocationID IS NOT NULL: Ensures we only load valid zones
    - LocationID is the primary key, so NULL values would break referential integrity
  
  - Materialization:
    - No specified strategy, so it will be replaced every time the pipeline runs

materialization:
  type: table

columns:
  - name: location_id
    type: INTEGER
    description: Unique identifier for the taxi zone location
    primary_key: true
    nullable: false
  - name: borough
    type: VARCHAR
    description: Borough name where the taxi zone is located
  - name: zone
    type: VARCHAR
    description: Zone name within the borough
  - name: service_zone
    type: VARCHAR
    description: Service zone classification (Airports, Boro Zone, Yellow Zone, etc.)

@bruin */


  SELECT
    LocationID    AS location_id,
    Borough       AS borough,
    Zone          AS zone,
    Service_zone  AS service_zone,
  FROM read_csv(
    'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv',
    header=true,
    auto_detect=true
  )
  WHERE 1=1
    AND LocationID IS NOT NULL
