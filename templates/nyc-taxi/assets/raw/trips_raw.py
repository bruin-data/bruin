"""@bruin
name: tier_1.trips_raw
type: python
image: python:3.11
connection: duckdb-default
description: |
  Ingests NYC taxi trip data from HTTP parquet files using Python requests library.
  Loops through all months between interval start/end dates and combines the data.
  Uses Bruin Python materialization with merge strategy - returns a Pandas DataFrame and Bruin automatically
  handles insertion into DuckDB, updating existing records and inserting new ones based on primary key.

  This approach:
  - Downloads parquet files from HTTP URLs for all months in the date range
  - Combines data from multiple months into a single DataFrame
  - Adds taxi_type column to track which taxi type each record represents
  - Keeps data as raw as possible - preserves original column names from parquet files
  - Column normalization (tpep_pickup_datetime -> pickup_time, etc.) is handled in tier_2 transformation layer
  - Uses merge strategy to handle incremental updates - updates existing records and inserts new ones
  - Returns DataFrame for Bruin to materialize into DuckDB table

materialization:
  type: table
  strategy: merge

columns:
  - name: tpep_pickup_datetime
    type: TIMESTAMP
    description: The date and time when the meter was engaged (yellow taxis only)
    primary_key: true
  - name: lpep_pickup_datetime
    type: TIMESTAMP
    description: The date and time when the meter was engaged (green taxis only)
    primary_key: true
  - name: tpep_dropoff_datetime
    type: TIMESTAMP
    description: The date and time when the meter was disengaged (yellow taxis only)
    primary_key: true
  - name: lpep_dropoff_datetime
    type: TIMESTAMP
    description: The date and time when the meter was disengaged (green taxis only)
    primary_key: true
  - name: pulocationid
    type: INTEGER
    description: TLC Taxi Zone in which the taximeter was engaged
    primary_key: true
    nullable: false
  - name: dolocationid
    type: INTEGER
    description: TLC Taxi Zone in which the taximeter was disengaged
    primary_key: true
    nullable: false
  - name: taxi_type
    type: VARCHAR
    description: Type of taxi (yellow or green)
    primary_key: true
    nullable: false
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when the data was extracted from the source
    update_on_merge: true
  - name: vendorid
    type: DOUBLE
    description: A code indicating the TPEP provider that provided the record
    update_on_merge: true
  - name: passenger_count
    type: DOUBLE
    description: The number of passengers in the vehicle (entered by the driver)
    update_on_merge: true
  - name: trip_distance
    type: DOUBLE
    description: The elapsed trip distance in miles reported by the taximeter
    update_on_merge: true
  - name: store_and_fwd_flag
    type: VARCHAR
    description: This flag indicates whether the trip record was held in vehicle memory before sending to the vendor
    update_on_merge: true
  - name: payment_type
    type: DOUBLE
    description: A numeric code signifying how the passenger paid for the trip
    update_on_merge: true
  - name: fare_amount
    type: DOUBLE
    description: The time-and-distance fare calculated by the meter
    update_on_merge: true
  - name: extra
    type: DOUBLE
    description: Miscellaneous extras and surcharges
    update_on_merge: true
  - name: mta_tax
    type: DOUBLE
    description: $0.50 MTA tax that is automatically triggered based on the metered rate in use
    update_on_merge: true
  - name: tip_amount
    type: DOUBLE
    description: Tip amount (automatically populated for credit card tips, manually entered for cash tips)
    update_on_merge: true
  - name: tolls_amount
    type: DOUBLE
    description: Total amount of all tolls paid in trip
    update_on_merge: true
  - name: improvement_surcharge
    type: DOUBLE
    description: $0.30 improvement surcharge assessed on hailed trips at the flag drop
    update_on_merge: true
  - name: total_amount
    type: DOUBLE
    description: The total amount charged to passengers (does not include cash tips)
    update_on_merge: true
  - name: congestion_surcharge
    type: DOUBLE
    description: Congestion surcharge for trips that start, end or pass through the Manhattan Central Business District
    update_on_merge: true
  - name: airport_fee
    type: DOUBLE
    description: Airport fee for trips that start or end at an airport
    update_on_merge: true

@bruin"""

import pandas as pd
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
import io
import os
import json


def generate_month_range(start_date: str, end_date: str) -> list[tuple[int, int]]:
    """
    Generate list of (year, month) tuples for all months between start and end dates (inclusive).

    Args:
      start_date: Start date in 'YYYY-MM-DD' format
      end_date: End date in 'YYYY-MM-DD' format

    Returns:
      List of (year, month) tuples
    """
    start_month = datetime.strptime(start_date, '%Y-%m-%d').replace(day=1)
    end_month = datetime.strptime(end_date, '%Y-%m-%d').replace(day=1)

    print(f"Generating months between {start_month} and {end_month}")
    months = []
    current = start_month
    while current <= end_month:
      months.append((current.year, current.month))
      current += relativedelta(months=1)

    print(f"Total months to ingest: {len(months)}")

    return months


def materialize():
  """
  Materialize function that returns a Pandas DataFrame.
  Bruin will automatically insert this DataFrame into DuckDB based on materialization strategy.
  """

  # Get start and end dates from environment variables
  start_date = os.environ.get('BRUIN_START_DATE')
  end_date = os.environ.get('BRUIN_END_DATE')

  # Get taxi_type
  bruin_vars = json.loads(os.environ["BRUIN_VARS"])
  taxi_types = bruin_vars.get('taxi_types')
  print(f"Taxi types: {taxi_types}")

  # Generate list of months to process
  months = generate_month_range(start_date, end_date)

  # Download and combine parquet files
  all_dataframes = []
  errors = []
  base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
  extracted_at = datetime.now()
  
  for taxi_type in taxi_types:
    for year, month in months:
      print(f"Downloading {year}-{month:02d}: {taxi_type}")
      url = f'{base_url}/{taxi_type}_tripdata_{year}-{month:02d}.parquet'

      try:
        response = requests.get(url, timeout=300)
        response.raise_for_status()

        df = pd.read_parquet(io.BytesIO(response.content))

        # Normalize column names to lowercase with underscores to avoid collisions
        # e.g., 'Airport_fee' and 'airport_fee' both become 'airport_fee'
        df.columns = df.columns.str.lower().str.replace(' ', '_')

        df['taxi_type'] = taxi_type
        df['extracted_at'] = extracted_at

        all_dataframes.append(df)
        print(f"Successfully downloaded {year}-{month:02d}: {len(df)} rows")

      except requests.exceptions.RequestException as e:
        error_msg = f"Error downloading {taxi_type} {year}-{month:02d}: {e}"
        print(error_msg)
        errors.append(error_msg)
      except Exception as e:
        error_msg = f"Error processing {taxi_type} {year}-{month:02d}: {e}"
        print(error_msg)
        errors.append(error_msg)

  if not all_dataframes:
    error_summary = "\n".join(errors) if errors else "No errors recorded"
    raise ValueError(f"No dataframes to combine. Failed to download all files.\nErrors:\n{error_summary}")
  
  if errors:
    print(f"\nWarning: {len(errors)} file(s) failed to download, but continuing with {len(all_dataframes)} successful download(s)")

  combined_df = pd.concat(all_dataframes, ignore_index=True)
  print(f"Total rows combined: {len(combined_df)}")
  return combined_df

