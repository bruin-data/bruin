/*@bruin

name: currency_names
type: duckdb.sql
materialization:
   type: table

description: This query creates a table with available currency codes and their corresponding names.

depends:
  - frankfurter_raw.currencies

@bruin*/

SELECT 
    currency_code,
    currency_name
FROM 
    frankfurter_raw.currencies;