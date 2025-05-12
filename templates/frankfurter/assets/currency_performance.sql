/*@bruin

name: frankfurter.average_rate
type: duckdb.sql
materialization:
   type: table

description: This query retrieves the exchange rates from frankfurter.rates for all currencies for current date and compares them with the rates from the last 1, 7, and 30 days. It also includes the currency name from frankfurter.currencies for better readability. N.B. since the rates are not available for weekends, the query fills in the missing dates with the last available rate.

depends:
  - frankfurter.rates
  - frankfurter.currencies


@bruin*/

WITH dates AS (
    SELECT CAST(unnest(generate_series(CURRENT_DATE - 31, CURRENT_DATE, INTERVAL '1 day')) AS DATE) AS date
),
codes AS (
    SELECT DISTINCT currency_code, base_currency
    FROM frankfurter.rates
),
all_days AS (
    SELECT c.currency_code, c.base_currency, d.date
    FROM codes c
    CROSS JOIN dates d
),
filled_rates AS (
    SELECT
        a.currency_code,
        a.base_currency,
        a.date,
        (
            SELECT r.rate
            FROM frankfurter.rates r
            WHERE r.currency_code = a.currency_code
              AND CAST(r.date AS DATE) <= a.date
            ORDER BY CAST(r.date AS DATE) DESC
            LIMIT 1
        ) AS rate
    FROM all_days a
),
with_lags AS (
    SELECT
        currency_code,
        base_currency,
        date,
        rate,
        LAG(rate, 1) OVER (PARTITION BY currency_code ORDER BY date) AS rate_lag_1d,
        LAG(rate, 7) OVER (PARTITION BY currency_code ORDER BY date) AS rate_lag_7d,
        LAG(rate, 30) OVER (PARTITION BY currency_code ORDER BY date) AS rate_lag_30d
    FROM filled_rates
),
with_names AS (
    SELECT 
        wl.*,
        fc.currency_name
    FROM with_lags wl
    LEFT JOIN frankfurter.currencies fc
        ON wl.currency_code = fc.currency_code
)
SELECT 
    currency_name,
    currency_code,
    base_currency,
    TO_CHAR(date, 'YYYY-MM-DD') AS date,
    rate,
    rate_lag_1d,
    rate_lag_7d,
    rate_lag_30d
FROM with_names
WHERE date = CURRENT_DATE
ORDER BY currency_code, date;










