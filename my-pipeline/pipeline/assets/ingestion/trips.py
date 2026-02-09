"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# Connection to use for materialization.
# This can be omitted if a default connection is configured in pipeline.yml,
# but some validations require an explicit connection.
connection: duckdb-default

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: create+replace

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: trip_id
    type: string
    description: Unique trip identifier.
  - name: taxi_type
    type: string
    description: Taxi type (e.g., yellow, green).
  - name: pickup_datetime
    type: timestamp
    description: Trip pickup timestamp.
  - name: dropoff_datetime
    type: timestamp
    description: Trip dropoff timestamp.
  - name: payment_type_id
    type: integer
    description: Payment type identifier.
  - name: extracted_at
    type: timestamp
    description: Ingestion timestamp.

@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python
import json
import os
from datetime import datetime, timezone

import pandas as pd


# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
def materialize():
    """
    TODO: Implement ingestion using Bruin runtime context.

    Required Bruin concepts to use here:
    - Built-in date window variables:
      - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
      - BRUIN_START_DATETIME / BRUIN_END_DATETIME (ISO datetime)
      Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
    - Pipeline variables:
      - Read JSON from BRUIN_VARS, e.g. `taxi_types`
      Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

    Design TODOs (keep logic minimal, focus on architecture):
    - Use start/end dates + `taxi_types` to generate a list of source endpoints for the run window.
    - Fetch data for each endpoint, parse into DataFrames, and concatenate.
    - Add a column like `extracted_at` for lineage/debugging (timestamp of extraction).
    - Prefer append-only in ingestion; handle duplicates in staging.
    """
    # return final_dataframe
    
    bruin_vars = os.getenv("BRUIN_VARS", "{}")
    vars_payload = json.loads(bruin_vars)
    taxi_types = vars_payload.get("taxi_types", []) or ["yellow"]

    def parse_dt(value: str | None) -> datetime:
      if not value:
        return datetime.now(timezone.utc)
      try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
      except ValueError:
        return datetime.now(timezone.utc)

    start_dt = parse_dt(os.getenv("BRUIN_START_DATETIME") or os.getenv("BRUIN_START_DATE"))
    end_dt = parse_dt(os.getenv("BRUIN_END_DATETIME") or os.getenv("BRUIN_END_DATE"))
    extracted_at = datetime.now(timezone.utc)

    rows = [
      {
        "trip_id": f"{taxi_type}-{start_dt.isoformat()}",
        "taxi_type": taxi_type,
        "pickup_datetime": start_dt,
        "dropoff_datetime": end_dt,
        "payment_type_id": 1,
        "extracted_at": extracted_at,
      }
      for taxi_type in taxi_types
    ]

    return pd.DataFrame(rows, columns=[
      "trip_id",
      "taxi_type",
      "pickup_datetime",
      "dropoff_datetime",
      "payment_type_id",
      "extracted_at",
    ])


