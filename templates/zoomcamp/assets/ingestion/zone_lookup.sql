/* @bruin
# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Columns/metadata: https://getbruin.com/docs/bruin/assets/columns

# TODO: Set the asset name (recommended: ingestion.zone_lookup).
name: TODO_SET_ASSET_NAME

# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: TODO

# TODO: Choose a materialization approach for lookup/reference data.
# Common choices:
materialization:
  type: table
  # TODO: set a materialization strategy.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  # suggested strategy: create+replace (easy, full refresh each run)
  # suggested strategy: truncate+insert (refresh contents without changing table definition)
  strategy: TODO

# TODO: Define columns and a primary key.
# - Lookups should usually have a stable primary key and be non-nullable.
columns:
  - name: TODO_id
    type: TODO
    description: TODO
    primary_key: true
    nullable: false

@bruin */

-- TODO: Write a SELECT query that produces the lookup table.
-- Options:
-- - Load from an HTTP CSV (DuckDB has helpers for reading CSV from URLs).
-- - Load from a local seed asset instead (see docs on seeds).
--   Docs: https://getbruin.com/docs/bruin/assets/seed
--
-- Best practices TODOs:
-- - Filter out invalid rows that would violate primary key / non-null constraints.
-- - Keep this layer *simple* and deterministic; do not embed heavy business logic here.

SELECT something
FROM somewhere
