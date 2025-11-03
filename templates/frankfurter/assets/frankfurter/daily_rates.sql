/* @bruin

type: duckdb.sql
description: This query retrieves the exchange rates from frankfurter_raw.rates and creates a table which fills in missing dates on public holidays and weekends with the last available rate.

materialization:
  type: table

depends:
  - frankfurter_raw.rates

@bruin */

WITH dates AS (
    SELECT
        CAST(
            UNNEST(
                GENERATE_SERIES(
                    CURRENT_DATE - 31, CURRENT_DATE, INTERVAL '1 day'
                )
            ) AS DATE
        ) AS date
),

codes AS (
    SELECT DISTINCT
        currency_code,
        base_currency
    FROM frankfurter_raw.rates
),

all_days AS (
    SELECT
        c.currency_code,
        c.base_currency,
        d.date
    FROM codes AS c
    CROSS JOIN dates AS d
),

filled_rates AS (
    SELECT
        a.currency_code,
        a.base_currency,
        a.date,
        (
            SELECT r.rate
            FROM frankfurter_raw.rates AS r
            WHERE
                r.currency_code = a.currency_code
                AND CAST(r.date AS DATE) <= a.date
            ORDER BY CAST(r.date AS DATE) DESC
            LIMIT 1
        ) AS rate
    FROM all_days AS a
)

SELECT * FROM filled_rates
ORDER BY currency_code, date;
