"""@bruin
name: ingestion.ingest_trips_python
uri: neptune.ingestion.ingest_trips_python
type: python
image: python:3.11
connection: duckdb-default
description: |
  Ingests NYC taxi trip data from HTTP parquet files using Python requests library.
  Loops through all months between interval start/end dates and combines the data.
  Uses Bruin Python materialization - returns a Pandas DataFrame and Bruin automatically
  handles insertion into DuckDB based on the materialization strategy.

  This approach:
  - Downloads parquet files from HTTP URLs for all months in the date range
  - Combines data from multiple months into a single DataFrame
  - Adds taxi_type column to track which taxi type each record represents
  - Keeps data as raw as possible - preserves original column names from parquet files
  - Column normalization (vendor_id -> vendorid, etc.) is handled in tier_1 transformation layer
  - Returns DataFrame for Bruin to materialize into DuckDB table

owner: data-engineering
tags:
  - ingestion
  - nyc-taxi
  - raw-data
  - python-ingestion

materialization:
  type: table
  strategy: create+replace

@bruin"""

import pandas as pd
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
import io
import os
import json


def generate_month_range() -> list[tuple[int, int]]:
    """
    Generate list of (year, month) tuples for all months between start and end dates (inclusive).

    Returns:
        List of (year, month) tuples
    """
    start_month = datetime.strptime(os.environ.get('BRUIN_START_DATE'), '%Y-%m-%d').replace(day=1)
    end_month = datetime.strptime(os.environ.get('BRUIN_END_DATE'), '%Y-%m-%d').replace(day=1)

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

    # Get taxi_type
    bruin_vars = json.loads(os.environ["BRUIN_VARS"])
    taxi_types = bruin_vars.get('taxi_types')
    print(f"Taxi types: {taxi_types}")

    # Generate list of months to process
    months = generate_month_range()

    # Download and combine parquet files
    all_dataframes = []
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
              print(f"Error downloading {year}-{month:02d}: {e}")
              continue
          except Exception as e:
              print(f"Error processing {year}-{month:02d}: {e}")
              continue

    if not all_dataframes:
        print("No dataframes to combine")
        raise ValueError("No dataframes to combine")

    combined_df = pd.concat(all_dataframes, ignore_index=True)
    print(f"Total rows combined: {len(combined_df)}")
    return combined_df
