/* @bruin

name: silver.fx_rate_enriched
type: bq.sql

materialization:
   type: table

description: |
  Enriches the bronze Frankfurter exchange rates with rolling aggregates and
  day-over-day deltas to illustrate a typical silver layer transformation in
  BigQuery.

depends:
  - bronze.frankfurter_rates

columns:
  - name: date
    type: date
    description: "Date of the exchange rate observation"
    primary_key: true
    checks:
      - name: not_null
  - name: currency_code
    type: string
    description: "ISO 4217 code for the quoted currency"
    primary_key: true
    checks:
      - name: not_null
  - name: base_currency
    type: string
    description: "Reference currency that the rate is quoted against"
    checks:
      - name: not_null
  - name: rate
    type: numeric
    description: "Spot exchange rate value for the currency on the given date"
    checks:
      - name: not_null
      - name: positive
  - name: avg_rate_7d
    type: numeric
    description: "Seven day rolling average to smooth short term volatility"
    checks:
      - name: not_null
      - name: positive
  - name: change_vs_prev_day
    type: numeric
    description: "Day-over-day change in the exchange rate"
    checks:
      - name: not_null

custom_checks:
  - name: ensure_rolling_average_populated
    description: "Ensure rows older than a week carry a computed rolling average"
    value: 0
    query: |
      SELECT COUNT(*) AS missing_average
      FROM silver.fx_rate_enriched
      WHERE avg_rate_7d IS NULL
        AND date <= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)

@bruin */

WITH cleaned_rates AS (
  SELECT
    DATE(date) AS rate_date,
    currency_code,
    base_currency,
    SAFE_CAST(rate AS NUMERIC) AS rate
  FROM `bronze.frankfurter_rates`
  WHERE DATE(date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
),
enriched AS (
  SELECT
    rate_date,
    currency_code,
    base_currency,
    rate,
    AVG(rate) OVER (
      PARTITION BY currency_code
      ORDER BY rate_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS avg_rate_7d,
    COALESCE(
      rate - LAG(rate) OVER (PARTITION BY currency_code ORDER BY rate_date),
      0
    ) AS change_vs_prev_day
  FROM cleaned_rates
)
SELECT
  rate_date AS date,
  currency_code,
  base_currency,
  rate,
  avg_rate_7d,
  change_vs_prev_day
FROM enriched
ORDER BY date, currency_code;
