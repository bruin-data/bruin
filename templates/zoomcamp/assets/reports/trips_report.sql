/* @bruin
#
# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks
#
# TODO: Set the asset name (recommended: reports.trips_report).
name: TODO_SET_ASSET_NAME
#
# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: TODO
#
# TODO: Declare dependency on the staging asset.
depends:
  - TODO_DEP_STAGING_ASSET
#
materialization:
  # What is materialization?
  # Materialization tells Bruin how to turn your SELECT query into a persisted dataset.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  #
  # Materialization "type":
  # - table: persisted table
  # - view: persisted view (if the platform supports it)
  type: table
  #
  # TODO: set a materialization strategy.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  # suggested strategy: time_interval
  #
  # Incremental strategies:
  # Incremental means you update only the relevant slice of the report for the run window (instead of rebuilding all time).
  # Use `strategy` + `incremental_key` + `time_granularity` to define that slice.
  #
  # Common strategies (see docs for full list):
  # - create+replace
  # - truncate+insert
  # - append
  # - delete+insert
  # - merge
  # - time_interval
  strategy: TODO
  #
  # TODO: set incremental_key to your report period date/time column.
  incremental_key: TODO_SET_INCREMENTAL_KEY
  #
  # TODO: set `date` or `timestamp` depending on incremental_key type.
  time_granularity: TODO_SET_GRANULARITY
#
# TODO: Define report columns + primary key(s) at your chosen level of aggregation.
columns:
  - name: TODO_dim
    type: TODO
    description: TODO
    primary_key: true
    nullable: false
  - name: TODO_date
    type: DATE
    description: TODO
    primary_key: true
    nullable: false
  - name: TODO_count
    type: BIGINT
    description: TODO
    checks:
      - name: non_negative
#
@bruin */

-- Purpose of reports:
-- - Aggregations for dashboards and stakeholder use-cases
-- - Lightweight checks to prevent regressions
--
-- TODO: Build an aggregation query from staging to your report level of aggregation.
--
-- Required Bruin concepts to demonstrate:
-- - Filter incremental window using `{{ start_datetime }}` / `{{ end_datetime }}` (or date-trunc to your report period).
-- - Produce exactly one row per primary key (your chosen aggregation level).
-- - Add an `updated_at` column using CURRENT_TIMESTAMP (useful for debugging freshness).

SELECT something
FROM somewhere


