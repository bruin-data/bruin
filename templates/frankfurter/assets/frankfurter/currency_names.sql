/* @bruin

type: duckdb.sql
description: This query creates a table with available currency codes and their corresponding names.

materialization:
  type: table

depends:
  - frankfurter_raw.currencies

@bruin */

SELECT
    currency_code,
    currency_name
FROM
    frankfurter_raw.currencies;
