/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# TODO: Set the asset name (recommended: reports.trips_report).
name: TODO_SET_ASSET_NAME

# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: TODO

# TODO: Declare dependency on the staging asset(s) this report reads from.
depends:
  - TODO_DEP_STAGING_ASSET

# TODO: Choose materialization strategy.
# For reports, `time_interval` is a good choice to rebuild only the relevant time window.
# Important: Use the same `incremental_key` as staging (e.g., pickup_datetime) for consistency.
materialization:
  type: table
  # suggested strategy: time_interval
  strategy: TODO
  # TODO: set to your report's date column
  incremental_key: TODO
  # TODO: set to `date` or `timestamp`
  time_granularity: TODO

# TODO: Define report columns + primary key(s) at your chosen level of aggregation.
columns:
  - name: TODO_dim
    type: TODO
    description: TODO
    primary_key: true
  - name: TODO_date
    type: DATE
    description: TODO
    primary_key: true
  - name: TODO_metric
    type: BIGINT
    description: TODO
    checks:
      - name: non_negative

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT * -- TODO: replace with your aggregation logic
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
