/*@bruin

name: frankfurter.currency_performance
type: duckdb.sql
materialization:
   type: table

description: This query retrieves the exchange rates from daily_rates for all currencies and joins currency_names to each currency code for better readability. Finally, it presents the exchange rates for the current date and compares them with the rates from the previous day, 7 days ago and 30 days ago. 

depends:
  - frankfurter.currency_names
  - frankfurter.daily_rates

@bruin*/

WITH
with_lags AS (
    SELECT
        currency_code,
        base_currency,
        date,
        rate,
        LAG(rate, 1) OVER (PARTITION BY currency_code ORDER BY date) AS rate_lag_1d,
        LAG(rate, 7) OVER (PARTITION BY currency_code ORDER BY date) AS rate_lag_7d,
        LAG(rate, 30) OVER (PARTITION BY currency_code ORDER BY date) AS rate_lag_30d
    FROM frankfurter.daily_rates
),
with_names AS (
    SELECT 
        wl.*,
        fc.currency_name
    FROM with_lags wl
    LEFT JOIN frankfurter.currency_names fc
    USING (currency_code)
)
SELECT 
    currency_name,
    currency_code,
    base_currency,
    date,
    rate,
    rate_lag_1d,
    rate_lag_7d,
    rate_lag_30d
FROM with_names
WHERE date = CURRENT_DATE
ORDER BY currency_code, date;