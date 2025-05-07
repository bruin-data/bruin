/*@bruin

name: frankfurter.average_rate
type: duckdb.sql
materialization:
   type: table

description: Displays the top 10 currencies with the highest average exchange rate against EUR over the last 30 days.

depends:
  - frankfurter.rates

@bruin*/

SELECT 
    date,
    currency_name AS 'currency',
    'EUR' AS base,
    rate,
    lag(rate) OVER (PARTITION BY currency_name ORDER BY date) AS lag_1d,
    lag(rate, 5) OVER (PARTITION BY currency_name ORDER BY date) AS lag_7d,
    lag(rate, 30) OVER (PARTITION BY currency_name ORDER BY date) AS lag_30d
FROM
    frankfurter.rates
WHERE
    currency_name = 'IDR'
ORDER BY
    1,2;