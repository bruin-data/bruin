/*@bruin

name: frankfurter.average_rate
type: duckdb.sql
materialization:
   type: table

description: This query joins currency code to currency name and displays the exchange rate of a currency against the Euro (EUR) for the last 30 days. 
    It shows the current rate, the rate from yesterday, and the rates from 7 days ago and 30 days ago.

depends:
  - frankfurter.rates


@bruin*/

WITH dates AS (
    SELECT CAST(unnest(generate_series(CURRENT_DATE - 31, CURRENT_DATE, INTERVAL '1 day')) AS DATE) AS date
),
codes AS (
    SELECT DISTINCT currency_name FROM frankfurter.rates
),
all_days AS (
    SELECT c.currency_name, d.date
    FROM codes c
    CROSS JOIN dates d
),
filled_rates AS (
    SELECT
        a.currency_name,
        a.date,
        (
            SELECT r.rate
            FROM frankfurter.rates r
            WHERE r.currency_name = a.currency_name
              AND CAST(r.date AS DATE) <= a.date
            ORDER BY CAST(r.date AS DATE) DESC
            LIMIT 1
        ) AS rate
    FROM all_days a
),
with_lags AS (
    SELECT
        currency_name,
        date,
        rate,
        LAG(rate, 1) OVER (PARTITION BY currency_name ORDER BY date) AS rate_lag_1d,
        LAG(rate, 7) OVER (PARTITION BY currency_name ORDER BY date) AS rate_lag_7d,
        LAG(rate, 30) OVER (PARTITION BY currency_name ORDER BY date) AS rate_lag_30d
    FROM filled_rates
)
SELECT 
    currency_name,
    TO_CHAR(date, 'YYYY-MM-DD') AS date,
    rate,
    rate_lag_1d,
    rate_lag_7d,
    rate_lag_30d
FROM with_lags
WHERE date = CURRENT_DATE
ORDER BY currency_name, date;






