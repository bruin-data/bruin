/* @bruin

name: public.silver_exchange_rate_summary
type: pg.sql

materialization:
  type: table

description: >
  Aggregates the bronze Frankfurter exchange rate feed into a silver summary table with
  fresh rates, rolling averages, and observation counts for downstream analytics.

depends:
  - public.bronze_exchange_rates

columns:
  - name: currency_code
    type: varchar
    description: "Quoted currency code."
    checks:
      - name: not_null
  - name: base_currency
    type: varchar
    description: "Base currency that the quote is relative to."
    checks:
      - name: not_null
  - name: latest_date
    type: date
    description: "Most recent exchange rate date captured in the bronze layer."
    checks:
      - name: not_null
  - name: latest_rate
    type: numeric
    description: "Latest available exchange rate between the currency pair."
    checks:
      - name: not_null
      - name: positive
  - name: avg_rate_7d
    type: numeric
    description: "Seven day rolling average of the conversion rate."
    checks:
      - name: positive
  - name: avg_rate_30d
    type: numeric
    description: "Thirty day rolling average of the conversion rate."
    checks:
      - name: positive
  - name: observations_7d
    type: integer
    description: "Number of daily observations in the last seven days."
    checks:
      - name: non_negative
  - name: observations_30d
    type: integer
    description: "Number of daily observations in the last thirty days."
    checks:
      - name: positive

custom_checks:
  - name: silver table populated
    value: 0
    query: |
      SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
      FROM public.silver_exchange_rate_summary
  - name: fresh latest date (<= 3 day lag)
    value: 0
    query: |
      SELECT COUNT(*)
      FROM public.silver_exchange_rate_summary
      WHERE latest_date < CURRENT_DATE - INTERVAL '3 days'

@bruin */

WITH bronze_data AS (
  SELECT
    CAST(date AS DATE) AS rate_date,
    base_currency,
    currency_code,
    CAST(rate AS NUMERIC) AS rate
  FROM public.bronze_exchange_rates
),
recent AS (
  SELECT
    currency_code,
    base_currency,
    AVG(rate) FILTER (WHERE rate_date >= CURRENT_DATE - INTERVAL '7 days') AS avg_rate_7d,
    AVG(rate) FILTER (WHERE rate_date >= CURRENT_DATE - INTERVAL '30 days') AS avg_rate_30d,
    COUNT(*) FILTER (WHERE rate_date >= CURRENT_DATE - INTERVAL '7 days') AS observations_7d,
    COUNT(*) FILTER (WHERE rate_date >= CURRENT_DATE - INTERVAL '30 days') AS observations_30d
  FROM bronze_data
  WHERE rate_date >= CURRENT_DATE - INTERVAL '30 days'
  GROUP BY currency_code, base_currency
),
latest AS (
  SELECT DISTINCT ON (currency_code, base_currency)
    currency_code,
    base_currency,
    rate_date AS latest_date,
    rate AS latest_rate
  FROM bronze_data
  ORDER BY currency_code, base_currency, rate_date DESC
)
SELECT
  r.currency_code,
  r.base_currency,
  l.latest_date,
  l.latest_rate,
  COALESCE(r.avg_rate_7d, r.avg_rate_30d) AS avg_rate_7d,
  r.avg_rate_30d,
  r.observations_7d,
  r.observations_30d
FROM recent r
JOIN latest l
  ON r.currency_code = l.currency_code
 AND r.base_currency = l.base_currency
ORDER BY r.currency_code, r.base_currency;
