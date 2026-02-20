# NYC Taxi Pipelines - Bruin Sample Project

A comprehensive ELT pipeline built with Bruin that demonstrates best practices for building data pipelines. This project processes NYC taxi trip data from public HTTP sources, transforms it through multiple tiers, and generates analytical reports.

The pipeline extracts NYC taxi trip data from HTTP parquet files, cleans and transforms it, and generates monthly summary reports. It uses DuckDB for local processing and follows a three-tier architecture: raw (ingestion & raw) → staging (cleaned) → reports (aggregated).

## What This Project Aims to Achieve

This project serves as a **template and learning resource** for developers who want to understand Bruin's capabilities and how to build production-ready data pipelines. It demonstrates:

- **End-to-end ELT workflows**: From raw data ingestion to analytical reporting
- **Multi-tier data architecture**: Implementing a layered approach (ingestion → raw → cleaned → aggregated)
- **Incremental data processing**: Using time-based incremental strategies for efficient data updates
- **Data quality and transformation**: Cleaning, enrichment, and data quality checks
- **Python and SQL integration**: Combining Python-based ingestion with SQL transformations

## What Tools and Features of Bruin This Project Showcases

### Core Bruin Features

1. **Python Asset Materialization**
   - Demonstrates how to use Python for complex data ingestion
   - Shows integration with external APIs and HTTP data sources
   - Returns Pandas DataFrames that Bruin automatically materializes into tables

2. **Append + Deduplication Strategy**
   - Raw layer uses `append` materialization for simple, fast ingestion
   - Staging layer deduplicates using `QUALIFY ROW_NUMBER()` to handle re-ingested data
   - Keeps the most recently extracted record when duplicates exist
   
3. **Time-Interval Incremental Strategy**
   - Efficient incremental processing using `time_interval` materialization
   - Automatic date range handling and data deletion/replacement
   - Month-level truncation for batch processing

4. **Pipeline Variables**
   - Using pipeline-level variables (e.g., `taxi_types`) for configuration
   - Accessing variables in Python assets via `BRUIN_VARS` environment variable
   - Using variables in SQL assets via Jinja templating

5. **Data Lineage and Dependencies**
   - Explicit dependency declarations between assets
   - Automatic dependency resolution and execution ordering
   - Cross-tier data flow

6. **Metadata Management**
   - Comprehensive column descriptions and documentation
   - Primary key definitions and nullable constraints

7. **Data Quality Checks**
   - Custom quality checks using SQL queries to validate business rules and data integrity
   - Column-level checks using built-in check types (non_negative, positive, min, max)

## Learning Path

1. Start with `trips_raw.py` to understand Python asset materialization with append strategy
2. Review `taxi_zone_lookup.sql` and `payment_lookup.asset.yml` to see different lookup table patterns
3. Review `trips_summary.sql` for column normalization, enrichment, and deduplication patterns
4. Examine `report_trips_monthly.sql` for aggregation techniques
5. Explore `pipeline.yml` to understand configuration and variables

## Data Source

This project uses publicly available NYC taxi trip data from the [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). The data is available via HTTP endpoints that provide historical NYC taxi ride information in parquet format.






# Implementation Instructions

These instructions are especially useful as a prompt to an AI agent - this would work best when the Bruin MCP is configured. Please follow the instructions in the link below to set up Bruin MCP:
https://getbruin.com/docs/bruin/getting-started/bruin-mcp.html

## Data Sources

### Trip Data
- **URL**: `https://d37ci6vzurychx.cloudfront.net/trip-data/`
- **Format**: Parquet files, one per taxi type per month
- **Naming**: `<taxi_type>_tripdata_<year>-<month>.parquet`
- **Examples**:
  - `yellow_tripdata_2022-03.parquet`
  - `green_tripdata_2025-01.parquet`
- **Taxi Types**: `yellow` (default), `green`

### Lookup Table
- **URL**: `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`
- **Purpose**: Maps LocationID to Borough, Zone, and service_zone
- **Refresh**: Replaced on every pipeline run

## Pipeline Structure

### Directory Layout
```text
nyc-taxi/
├── .bruin.yml
├── pipeline.yml
├── requirements.txt
└── assets/
    ├── raw/
    │   ├── trips_raw.py
    │   ├── payment_lookup.asset.yml
    │   ├── payment_lookup.csv
    │   └── taxi_zone_lookup.sql
    ├── staging/
    │   └── trips_summary.sql
    └── reports/
        └── report_trips_monthly.sql
```

## Connection Configurations (`.bruin.yml`)

Before running the pipeline, you need to create a `.bruin.yml` file in the project root directory to [configure your local DuckDB connection](https://getbruin.com/docs/bruin/platforms/duckdb.html). This file is where you configure any database connection, as well as [custom secrets](https://getbruin.com/docs/bruin/secrets/bruinyml.html#bruin-yml-schema).

The `.bruin.yml` file configures your local development environment and tells Bruin where to create and store the DuckDB database file. If the duckdb database file does not already exist, it will automatically be created when you first run the pipeline.

**Create `.bruin.yml` file** in the project root:
   ```yaml
   default_environment: default
   environments:
       default:
           connections:
               duckdb:
                   - name: duckdb-default
                     path: duckdb.db
   ```

Note that it is best practice to add `.bruin.yml` to your `.gitignore` file because:
   - It may contain sensitive connection information and authentication credentials
   - Different developers may have different local database paths
   - Environment-specific configurations should not be committed to version control


## Pipeline Configuration (`pipeline.yml`)

```yaml
name: nyc-taxi-pipelines
schedule: monthly
start_date: "2022-01-01"
default_connections:
  duckdb: "duckdb-default"
variables:
  taxi_types:
    type: array
    items:
      type: string
    default: ["yellow", "green"]
```

### Configuration Sections

#### `start_date`
The `start_date` determines the earliest date for data processing. When a full-refresh run is triggered, the interval start is automatically set to this `start_date`, and the pipeline will ingest and process all data starting from this date. This is useful for:
- Setting a baseline for historical data backfills
- Limiting the scope of full-refresh operations to avoid processing extremely large date ranges
- Defining the earliest point in time your pipeline should consider

#### `default_connections`
This section initializes database connections that will be used throughout the pipeline. In this case, it initializes a DuckDB instance and provides a connection cursor named `duckdb-default` that can be referenced by assets. The connection name (`duckdb-default`) must match the connection name specified in your `.bruin.yml` file.

#### `variables`
Pipeline-level custom variables allow you to configure reusable values that can be accessed across all assets in the pipeline. Variables can be:
- **Used in Python assets**: Accessed via the `BRUIN_VARS` environment variable (parsed as JSON)
- **Used in SQL assets**: Referenced using Jinja templating syntax (e.g., `{{ taxi_types }}`)
- **Overridden at runtime**: Passed via command-line arguments when running the pipeline

In this pipeline, the `taxi_types` variable allows you to configure which taxi types to ingest (yellow, green, or both) without modifying the asset code.

## Asset Specifications

### Asset Naming

The `name` parameter in Bruin asset configurations (e.g., `name: raw.trips_raw`) is **optional**. If not provided, Bruin automatically infers the asset name from the file path:
- The parent folder name becomes the schema/dataset name (e.g., `raw`)
- The file name (without extension) becomes the asset name (e.g., `trips_raw.py` → `trips_raw`)
- Combined, this creates the full asset name: `raw.trips_raw`

**Example**: A file at `assets/raw/trips_raw.py` will automatically be named `raw.trips_raw` even without an explicit `name` parameter in the asset configuration.

You can still explicitly specify the `name` parameter if you want to override this default behavior or if your file structure doesn't match your desired naming convention.

### 1. Raw: Ingestion & Raw Data Storage

#### `raw.trips_raw`
- **Type**: `python`
- **Strategy**: `append`
- **Connection**: `duckdb-default`
- **Purpose**: Ingest raw trip data from HTTP parquet files using Python

**Python Materialization Overview**:
Bruin's Python materialization allows you to write Python code that returns a Pandas DataFrame, which Bruin automatically materializes into a database table. This approach is beneficial because:
- **No manual database operations**: You don't need to use DuckDB's Python library directly or write SQL to create/insert data
- **Automatic schema handling**: Bruin infers the schema from your DataFrame and creates the table accordingly
- **Consistent with SQL assets**: The materialized table can be referenced by SQL assets just like any other table
- **Simplified data processing**: You can focus on data extraction and transformation logic without worrying about database connection management

The `materialize()` function is required and must return a Pandas DataFrame. Bruin calls this function, receives the DataFrame, and handles all the database operations to store it as a table based on the materialization strategy.

**Bruin Configuration**:
- Preserves original column names from parquet files as-is (e.g., `tpep_pickup_datetime` for yellow taxis, `lpep_pickup_datetime` for green taxis, `pulocationid`, etc.)
- Column normalization (COALESCE for datetime columns, renaming) happens in staging, not in raw
- Adds `taxi_type` column from pipeline variables
- Adds `extracted_at` timestamp column
- Uses `append` strategy—deduplication is handled downstream in `staging.trips_summary`
- Python dependencies are defined in `requirements.txt` at the pipeline root

**Design Choice - Why Python with append?**:
- **Python for Complex Ingestion**: Python is ideal for this use case because it allows us to dynamically loop through date ranges and taxi types using Bruin variables (`BRUIN_START_DATE`, `BRUIN_END_DATE`, and `BRUIN_VARS`). This enables flexible ingestion logic that can handle multiple sources and date ranges without hardcoding values.
- **append Strategy**: The append strategy simply adds new records to the table. If the same date range is re-ingested (e.g., running the pipeline twice for January 2025), duplicates will be created in the raw layer. These duplicates are handled in the staging layer via `QUALIFY ROW_NUMBER()` deduplication.
- **Bruin Python Materialization**: Utilizing Bruin's Python materialization feature eliminates the need for manual database operations. We simply return a Pandas DataFrame, and Bruin handles all the database connection management, schema inference, and table creation/insertion automatically. This keeps the code focused on data extraction and transformation logic.

#### `raw.taxi_zone_lookup`
- **Type**: `duckdb.sql`
- **Strategy**: `create+replace` (implicit - no strategy specified, table is replaced on each run)
- **Purpose**: Load taxi zone lookup table from HTTP CSV source

**Bruin Configuration**:
- Primary key: `location_id` (non-nullable)
- Uses DuckDB's `read_csv()` function with `header=true` and `auto_detect=true` and pass the HTTP URL that contains the csv file
- Filters out NULL location IDs to ensure data quality
- Strategy: Table is replaced on each run to ensure we have the latest zone information

**Design Choice**:
- DuckDB has built-in ingestion features; when a URL is provided with the `read_csv` function, it essentially downloads the csv file and creates a table
- The lookup table may be updated by NYC TLC (new zones, renamed zones, etc.) - create/replace ensures we always have the latest zone information

#### `raw.payment_lookup`
- **Type**: `duckdb.seed`
- **Purpose**: Load payment type lookup table from local CSV seed file

**Bruin Configuration**:
- Primary key: `payment_type_id` (non-nullable)
- Reads from local CSV file: `payment_lookup.csv`
- Maps payment type codes (0-6) to human-readable descriptions

**Design Choice - Why DuckDB Seed Asset?**:
- **Static Reference Data**: Payment type codes are standardized and do not change frequently (unlike taxi zones which may be updated by NYC TLC)
- **Version Control**: Local seed file provides version control and reproducibility - the payment type mapping is part of the pipeline codebase
- **Simplicity**: Seed assets are a convenient way to load static data into a database without writing SQL queries
- **Automatic Materialization**: Seed assets automatically materialize into a table on each run, similar to SQL assets but with less boilerplate
- **No External Dependencies**: Unlike `taxi_zone_lookup` which depends on an external HTTP source, this lookup table is self-contained within the pipeline

**When to Use Seed Assets vs SQL Assets**:
- Use **seed assets** for static reference data that rarely changes and should be version-controlled
- Use **SQL assets** (like `taxi_zone_lookup`) for data that may change frequently or comes from external sources

### 2. Staging: Cleaned & Enriched Data

#### `staging.trips_summary`
- **Type**: `duckdb.sql`
- **Strategy**: `time_interval`
- **Incremental Key**: `pickup_time`
- **Time Granularity**: `timestamp`
- **Primary Key**: Composite (`pickup_time`, `dropoff_time`, `pickup_location_id`, `dropoff_location_id`, `taxi_type`)
- **Purpose**: Normalize column names, clean, and enrich trip data

**Time-Interval Strategy**:
The `time_interval` strategy is designed for incremental processing based on time-based keys. How it works:
- Bruin automatically calculates a date range based on the run parameters (`start_datetime` and `end_datetime`)
- It deletes all rows in the target table where the `incremental_key` (pickup_time) falls within this date range
- Then it inserts the new data from the query results for that same date range
- This ensures efficient updates: only the affected time period is processed, not the entire table

Why we chose it: This strategy is ideal for time-series data where we want to reprocess specific date ranges (e.g., to handle late-arriving data or corrections) without affecting other time periods.

**Bruin Configuration**:
- Reads from `raw.trips_raw`
- Normalizes column names using COALESCE to handle yellow (tpep_*) vs green (lpep_*) taxi types:
  - `COALESCE(tpep_pickup_datetime, lpep_pickup_datetime)` → `pickup_time` (cast to TIMESTAMP)
  - `COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime)` → `dropoff_time` (cast to TIMESTAMP)
  - `pulocationid` → `pickup_location_id`
  - `dolocationid` → `dropoff_location_id`
  - `payment_type` → cast to INTEGER
- Calculates `trip_duration_seconds` from pickup and dropoff times
- Enriches with location data from `raw.taxi_zone_lookup`
- Enriches with payment type descriptions from `raw.payment_lookup`
- Applies data quality filters (positive duration, reasonable trip length, non-negative amounts, valid payment types)
- **Deduplicates records** using `QUALIFY ROW_NUMBER()` partitioned by trip attributes (see Deduplication Strategy below)
- Adds `updated_at` timestamp column
- Preserves `extracted_at` timestamp from raw
- All primary key columns are non-nullable

**Deduplication Strategy**:

Since `raw.trips_raw` uses an **append** materialization strategy, re-ingesting the same date range (e.g., running the pipeline twice for January 2025) creates duplicate records. Deduplication is therefore handled in this staging layer using `QUALIFY ROW_NUMBER()`.

The deduplication logic partitions by these columns and keeps the most recently extracted record (`ORDER BY extracted_at DESC`):
- `pickup_time`, `dropoff_time`
- `pickup_location_id`, `dropoff_location_id`
- `taxi_type`, `trip_distance`, `passenger_count`
- `fare_amount`, `tip_amount`, `total_amount`, `payment_type`

**Why this combination?** The NYC TLC dataset has no single unique trip identifier. However, the odds of two completely identical trips (same pickup/dropoff times, locations, distance, passenger count, and payment details) occurring are effectively zero. This combination of columns reliably identifies unique rides.

### 3. Reports: Aggregated Analytics

#### `reports.report_trips_monthly`
- **Type**: `duckdb.sql`
- **Strategy**: `time_interval`
- **Incremental Key**: `month_date`
- **Time Granularity**: `timestamp`
- **Primary Key**: Composite (`taxi_type`, `month_date`)
- **Purpose**: Generate monthly summary reports

**Time-Interval Strategy**:
Uses `month_date` as the incremental key, which is the first day of each month. This allows reprocessing of specific months (e.g., if source data is corrected) without affecting other months.

**Bruin Configuration**:
- Reads from `staging.trips_summary`
- Aggregates data by `taxi_type` and month
- Adds `updated_at` timestamp column
- Aggregates `extracted_at` using MAX to track latest extraction time per month

## Test & Run Pipeline

Before running, validate your pipeline configuration:

```bash
bruin validate ./nyc-taxi/pipeline.yml --environment default
```

### Step 1: Test Individual Assets

Before running the full pipeline, it's recommended to test individual assets to ensure they work correctly. You can run a single asset by providing the path to the asset file:

```bash
# Example: Test the Python ingestion asset
bruin run ./nyc-taxi/assets/raw/trips_raw.py \
  --start-date 2021-01-01 \
  --end-date 2021-01-31 \
  --environment default
```

You can also test with a single taxi type by overriding the `taxi_types` variable:

```bash
# Test with only yellow taxis
bruin run ./nyc-taxi/assets/raw/trips_raw.py \
  --start-date 2021-01-01 \
  --end-date 2021-01-31 \
  --environment default \
  --var 'taxi_types=["yellow"]'

# Test with only green taxis
bruin run ./nyc-taxi/assets/raw/trips_raw.py \
  --start-date 2021-01-01 \
  --end-date 2021-01-31 \
  --environment default \
  --var 'taxi_types=["green"]'
```

When running a single asset, only that asset is executed. To also run all downstream assets (assets that depend on the one you're running), add the `--downstream` flag:

```bash
bruin run ./nyc-taxi/assets/raw/trips_raw.py \
  --start-date 2021-01-01 \
  --end-date 2021-01-31 \
  --environment default \
  --downstream
```

### Step 2: Run Full Pipeline

To run the entire pipeline, provide the path to `pipeline.yml`:

```bash
# Incremental run (default)
bruin run ./nyc-taxi/pipeline.yml \
  --start-date 2021-01-01 \
  --end-date 2022-02-28 \
  --environment default
```

**Incremental vs Full-Refresh:**

- **Incremental (default)**: Processes only the specified date range. Uses the `--start-date` and `--end-date` flags you provide.
- **Full-Refresh**: Reprocesses all data from the pipeline's `start_date` (defined in `pipeline.yml`) up to the `--end-date` you specify. Full refresh runs also recreate all the tables in the database instead of running an incremental query.

```bash
# Full-refresh run (reprocesses from pipeline start-date to end-date)
bruin run ./nyc-taxi/pipeline.yml \
  --end-date 2022-02-28 \
  --full-refresh \
  --environment default
```

**Running Pipeline vs Single Asset + Downstream:**

- **Running a pipeline** (`bruin run ./pipeline.yml`): Executes all assets in the pipeline in dependency order, respecting the date range you specify.
- **Running a single asset with `--downstream`**: Executes only that asset and its downstream dependencies. This is useful when you want to test a specific part of the pipeline without running everything.

**VS Code Extension:**

You can also run pipelines and assets directly from the Bruin VS Code extension. Open any asset file or `pipeline.yml` and open the Bruin panel interface (click the Bruin logo at the top of the opened file). Please refer to the Bruin VS Code extension page for more information.

## Verify Data

### Using `bruin query`

The `bruin query` command allows you to execute SQL queries against your database connections. You can query by connection name:

```bash
# Query using connection name
bruin query --connection duckdb-default --query "SELECT COUNT(*) FROM raw.trips_raw"
```

**Example queries:**

```bash
# Check monthly report (should show 14 months for 2021-01 to 2022-02)
bruin query --connection duckdb-default --query "SELECT COUNT(*) as month_count FROM reports.report_trips_monthly WHERE month_date >= '2021-01-01' AND month_date <= '2022-02-28'"

# Check monthly report details
bruin query --connection duckdb-default --query "SELECT * FROM reports.report_trips_monthly WHERE month_date >= '2021-01-01' AND month_date <= '2022-02-28' ORDER BY month_date"
```

### Using DuckDB CLI

You can also query your DuckDB database directly using the DuckDB CLI tool. The database file is located at `duckdb.db` (as specified in your `.bruin.yml`):

```bash
# Run queries in terminal
duckdb duckdb.db -c "SELECT COUNT(*) FROM raw.trips_raw"

# Open interactive DuckDB shell
duckdb duckdb.db

# Launch DuckDB web UI (localhost interface)
duckdb duckdb.db -ui
```

The `-ui` flag opens a web-based interface in your browser where you can run queries, explore tables, and visualize data interactively.

## Implementation Checklist

- [ ] Create `nyc-taxi/pipeline.yml` with correct configuration and variables
- [ ] Create `requirements.txt` in pipeline root with Python dependencies
- [ ] Create `.bruin.yml` for local environment configuration
- [ ] Create `raw.trips_raw.py` to ingest data from source website and materialize as a table using append strategy (using Bruin Python Materialization)
- [ ] Create `raw.taxi_zone_lookup.sql` with HTTP CSV ingestion
- [ ] Create `raw.payment_lookup.csv` with payment type mapping data
- [ ] Create `raw.payment_lookup.asset.yml` with seed file configuration
- [ ] Create `staging.trips_summary.sql` with column normalization and enrichment
- [ ] Create `reports.report_trips_monthly.sql` with monthly aggregations
- [ ] Add all required Bruin metadata (description, columns, etc.) to all assets (note: `name` is optional and will be inferred from file path if not provided)
- [ ] Set primary keys and nullable constraints correctly
- [ ] Add timestamp tracking columns (extracted_at, updated_at)
- [ ] Test individual assets
- [ ] Test full pipeline with different date ranges
- [ ] Verify data quality and row counts

## Key Implementation Details

1. **Date Range to Months**: 
   - Read dates from `BRUIN_START_DATE` and `BRUIN_END_DATE` environment variables (YYYY-MM-DD format)
   - Use `generate_month_range()` function to convert date range to list of (year, month) tuples
   - Handles cross-year ranges correctly (e.g., 2021-12-01 to 2022-01-01 → Dec 2021, Jan 2022)
2. **Deduplication in Staging**: 
   - The ingestion layer (`raw.trips_raw`) uses an **append** strategy, meaning re-ingesting the same date range creates duplicates. Deduplication is handled in `staging.trips_summary` using `QUALIFY ROW_NUMBER()`.
   - According to [NYC TLC documentation](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), there is no single unique trip identifier. However, the combination of trip attributes (pickup/dropoff times, locations, distance, passenger count, fare details) reliably identifies unique rides—the probability of two completely identical trips is effectively zero.
   - The deduplication keeps the most recently extracted record when duplicates exist (`ORDER BY extracted_at DESC`).
3. **Column Normalization**: 
   - **Ingestion Layer (raw.trips_raw)**: Preserves original column names from parquet files as-is (e.g., `vendorid`, `tpep_pickup_datetime` for yellow taxis, `lpep_pickup_datetime` for green taxis, `pulocationid`). Adds `taxi_type` and `extracted_at` columns.
   - **Staging Layer (staging.trips_summary)**: Transforms column names to more human-readable, consistent formats:
     - Uses `COALESCE(tpep_pickup_datetime, lpep_pickup_datetime)` → `pickup_time` (cast to TIMESTAMP) to handle both taxi types
     - Uses `COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime)` → `dropoff_time` (cast to TIMESTAMP)
     - `pulocationid` → `pickup_location_id`
     - `dolocationid` → `dropoff_location_id`
     - `payment_type` → cast to INTEGER
   - This separation allows the ingestion layer to process data as-is, while staging standardizes the schema for downstream consumption
4. **Taxi Types**: Configured via pipeline variables (default: `["yellow", "green"]`), accessible in Python assets via `BRUIN_VARS` environment variable
5. **Lookup Joins**: Use `LEFT JOIN` to retain all trips even if location_id or payment_type_id not found in lookup tables (taxi_zone_lookup and payment_lookup)
6. **Timestamp Tracking**: 
   - `extracted_at`: Set in ingestion layer when data is downloaded
   - `updated_at`: Set in staging and reports when data is updated/refreshed
