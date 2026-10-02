/* @bruin

name: silver.currency_rate_snapshot
type: sf.sql
materialization:
  type: table

description: |
  Aggregates the bronze Frankfurter exchange rate feed in Snowflake to provide a latest-rate snapshot
  with rolling thirty day statistics and point-in-time deltas.

depends:
  - bronze.frankfurter_rates

columns:
  - name: currency_code
    type: varchar
    description: "Quoted currency code for the FX pair."
    checks:
        - name: not_null
  - name: base_currency
    type: varchar
    description: "Shared base currency from the bronze layer."
    checks:
        - name: not_null
  - name: latest_rate_date
    type: date
    description: "Calendar date of the most recent available exchange rate."
    checks:
        - name: not_null
  - name: latest_rate
    type: float
    description: "Most recent exchange rate value for the currency pair."
    checks:
        - name: not_null
        - name: positive
  - name: rate_change_7d
    type: float
    description: "Difference between the latest rate and the rate seven days prior."
  - name: rate_change_30d
    type: float
    description: "Difference between the latest rate and the rate thirty days prior."
  - name: avg_rate_30d
    type: float
    description: "Average exchange rate over the past thirty days."
    checks:
        - name: positive
  - name: min_rate_30d
    type: float
    description: "Minimum exchange rate recorded in the thirty day window."
    checks:
        - name: positive
  - name: max_rate_30d
    type: float
    description: "Maximum exchange rate recorded in the thirty day window."
    checks:
        - name: positive

custom_checks:
  - name: ensure avg rate populated
    value: 0
    query: |
      SELECT COUNT(*) AS avg_rate_missing
      FROM silver.currency_rate_snapshot
      WHERE avg_rate_30d IS NULL

@bruin*/

WITH base_data AS (
    SELECT
        currency_code,
        base_currency,
        CAST(date AS DATE) AS rate_date,
        rate
    FROM bronze.frankfurter_rates
),
lagged AS (
    SELECT
        currency_code,
        base_currency,
        rate_date,
        rate,
        LAG(rate, 7) OVER (PARTITION BY currency_code ORDER BY rate_date) AS rate_lag_7d,
        LAG(rate, 30) OVER (PARTITION BY currency_code ORDER BY rate_date) AS rate_lag_30d,
        ROW_NUMBER() OVER (PARTITION BY currency_code ORDER BY rate_date DESC) AS row_num
    FROM base_data
),
latest AS (
    SELECT
        currency_code,
        base_currency,
        rate_date,
        rate,
        rate_lag_7d,
        rate_lag_30d
    FROM lagged
    WHERE row_num = 1
),
recent_window AS (
    SELECT
        currency_code,
        base_currency,
        rate
    FROM base_data
    WHERE rate_date >= DATEADD(day, -30, CURRENT_DATE())
),
aggregated AS (
    SELECT
        currency_code,
        base_currency,
        AVG(rate) AS avg_rate_30d,
        MIN(rate) AS min_rate_30d,
        MAX(rate) AS max_rate_30d
    FROM recent_window
    GROUP BY 1, 2
)
SELECT
    l.currency_code,
    l.base_currency,
    l.rate_date AS latest_rate_date,
    l.rate AS latest_rate,
    l.rate - COALESCE(l.rate_lag_7d, l.rate) AS rate_change_7d,
    l.rate - COALESCE(l.rate_lag_30d, l.rate) AS rate_change_30d,
    a.avg_rate_30d,
    a.min_rate_30d,
    a.max_rate_30d
FROM latest l
JOIN aggregated a
  ON a.currency_code = l.currency_code
 AND a.base_currency = l.base_currency
ORDER BY l.currency_code;
