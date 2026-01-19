/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

# TODO: Set the asset name (recommended: staging.trips).
name: TODO_SET_ASSET_NAME
# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: TODO

# TODO: Declare dependencies so `bruin run ... --downstream` and lineage work.
# Examples:
# depends:
#   - ingestion.trips
#   - ingestion.payment_lookup
depends:
  - TODO_DEP_1
  - TODO_DEP_2

# TODO: Choose time-based incremental processing if the dataset is naturally time-windowed.
# - This module expects you to use `time_interval` to reprocess only the requested window.
materialization:
  # What is materialization?
  # Materialization tells Bruin how to turn your SELECT query into a persisted dataset.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  #
  # Materialization "type":
  # - table: persisted table
  # - view: persisted view (if the platform supports it)
  type: table
  # TODO: set a materialization strategy.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  # suggested strategy: time_interval
  #
  # Incremental strategies (what does "incremental" mean?):
  # Incremental means you update only part of the destination instead of rebuilding everything every run.
  # In Bruin, this is controlled by `strategy` plus keys like `incremental_key` and `time_granularity`.
  #
  # Common strategies you can choose from (see docs for full list):
  # - create+replace (full rebuild)
  # - truncate+insert (full refresh without drop/create)
  # - append (insert new rows only)
  # - delete+insert (refresh partitions based on incremental_key values)
  # - merge (upsert based on primary key)
  # - time_interval (refresh rows within a time window)
  strategy: TODO
  # TODO: set incremental_key to your event time column (DATE or TIMESTAMP).
  incremental_key: TODO_SET_INCREMENTAL_KEY
  # TODO: choose `date` vs `timestamp` based on the incremental_key type.
  time_granularity: TODO_SET_GRANULARITY

# TODO: Define output columns, mark primary keys, and add a few checks.
columns:
  - name: TODO_pk1
    type: TODO
    description: TODO
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: TODO_metric
    type: TODO
    description: TODO
    checks:
      - name: non_negative

# TODO: Add one custom check that validates a staging invariant (uniqueness, ranges, etc.)
# Docs: https://getbruin.com/docs/bruin/quality/custom
custom_checks:
  - name: TODO_custom_check_name
    description: TODO
    query: |
      -- TODO: return a single scalar (COUNT(*), etc.) that should match `value`
      SELECT 0
    value: 0

@bruin */

-- TODO: Write the staging SELECT query.
--
-- Purpose of staging:
-- - Clean and normalize schema from ingestion
-- - Deduplicate records (important if ingestion uses append strategy)
-- - Enrich with lookup tables (JOINs)
-- - Filter invalid rows (null PKs, negative values, etc.)
--
-- Why filter by {{ start_datetime }} / {{ end_datetime }}?
-- When using `time_interval` strategy, Bruin:
--   1. DELETES rows where `incremental_key` falls within the run's time window
--   2. INSERTS the result of your query
-- Therefore, your query MUST filter to the same time window so only that subset is inserted.
-- If you don't filter, you'll insert ALL data but only delete the window's data = duplicates.

SELECT *
FROM ingestion.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
