/*@bruin

name: frankfurter.average_rate
type: duckdb.sql
materialization:
   type: table

description: This query joins currency code to currency name and displays the exchange rate of a currency against the Euro (EUR) for the last 30 days. 
    It shows the current rate, the rate from yesterday, and the rates from 7 days ago and 30 days ago.

depends:
  - frankfurter.rates
  - frankfurter.currencies

@bruin*/

WITH target_dates AS (
    SELECT CURRENT_DATE AS ref_date, 'today' AS label
    UNION ALL SELECT CURRENT_DATE - INTERVAL 1 DAY, 'yesterday'
    UNION ALL SELECT CURRENT_DATE - INTERVAL 7 DAY, 'week_ago'
    UNION ALL SELECT CURRENT_DATE - INTERVAL 30 DAY, 'month_ago'
),
rates AS (
    SELECT
        currency_name,
        CAST(date AS DATE) AS date,
        rate
    FROM frankfurter.rates
    WHERE CAST(date AS DATE) >= CURRENT_DATE - INTERVAL 40 DAY
),
closest_rates AS (
    SELECT
        td.label,
        r.currency_name,
        r.rate,
        r.date,
        ROW_NUMBER() OVER (
            PARTITION BY r.currency_name, td.label
            ORDER BY td.ref_date - r.date
        ) AS rn
    FROM target_dates td
    JOIN rates r
        ON r.date <= td.ref_date
),
joined AS (
    SELECT
        cr.label,
        cr.currency_name AS code,
        c.currency_name AS full_name,
        cr.rate,
        cr.date,
        cr.rn
    FROM closest_rates cr
    LEFT JOIN frankfurter.currencies c
        ON cr.currency_name = c.currency_code
)
SELECT
    code AS "Currency Code",
    full_name AS "Currency Name",
    'EUR' AS "Base Currency",
    MAX(CASE WHEN label = 'today' THEN rate END) AS current_rate,
    MAX(CASE WHEN label = 'yesterday' THEN rate END) AS "Rate: Yesterday",
    MAX(CASE WHEN label = 'week_ago' THEN rate END) AS "Rate: 7 Days Ago",
    MAX(CASE WHEN label = 'month_ago' THEN rate END) AS "Rate: 30 Days Ago",
FROM joined
WHERE rn = 1
GROUP BY code, full_name
ORDER BY code;


