/* @bruin

name: frankfurter.display_rates
type: duckdb.sql
materialization:
   type: table

depends:
   - frankfurter.latest

@bruin */

SELECT date, currency_name, rate FROM frankfurter.latest
WHERE currency_name IN ('EUR', 'GBP', 'IDR', 'USD')
