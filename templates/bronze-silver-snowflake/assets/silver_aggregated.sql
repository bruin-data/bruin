/* @bruin

name: public.silver_exchange_rate_summary
type: sf.sql

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
    type: float
    description: "Latest available exchange rate between the currency pair."
    checks:
      - name: not_null
      - name: positive
  - name: avg_rate_7d
    type: float
    description: "Seven day rolling average of the conversion rate."
    checks:
      - name: positive
  - name: avg_rate_30d
    type: float
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
      WHERE latest_date < DATEADD(day, -3, CURRENT_DATE())

@bruin */

WITH bronze_data AS (
  SELECT
    TO_DATE(date) AS rate_date,
    base_currency,
    currency_code,
    CAST(rate AS FLOAT) AS rate
  FROM public.bronze_exchange_rates
),
recent AS (
  SELECT
    currency_code,
    base_currency,
    AVG(CASE WHEN rate_date >= DATEADD(day, -7, CURRENT_DATE()) THEN rate END) AS avg_rate_7d,
    AVG(CASE WHEN rate_date >= DATEADD(day, -30, CURRENT_DATE()) THEN rate END) AS avg_rate_30d,
    COUNT(CASE WHEN rate_date >= DATEADD(day, -7, CURRENT_DATE()) THEN 1 END) AS observations_7d,
    COUNT(CASE WHEN rate_date >= DATEADD(day, -30, CURRENT_DATE()) THEN 1 END) AS observations_30d
  FROM bronze_data
  WHERE rate_date >= DATEADD(day, -30, CURRENT_DATE())
  GROUP BY currency_code, base_currency
),

latest AS (
  SELECT
    currency_code,
    base_currency,
    rate_date AS latest_date,
    rate AS latest_rate
  FROM bronze_data
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY currency_code, base_currency
    ORDER BY rate_date DESC
  ) = 1
)

SELECT
  r.currency_code,
  r.base_currency,
  l.latest_date,
  l.latest_rate,
  r.avg_rate_7d,
  r.avg_rate_30d,
  r.observations_7d,
  r.observations_30d
FROM recent r
JOIN latest l
  ON r.currency_code = l.currency_code
 AND r.base_currency = l.base_currency
ORDER BY r.currency_code, r.base_currency;

