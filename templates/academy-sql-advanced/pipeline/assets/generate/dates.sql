/* @bruin
name: dates
type: duckdb.sql

description: "Calendar date spine."

materialization:
  type: table
  strategy: create+replace

columns:
  - name: date_day
    type: date
  - name: date_key
    type: integer
  - name: year
    type: integer
  - name: quarter
    type: integer
  - name: year_month
    type: varchar
  - name: month_number
    type: integer
  - name: month_name
    type: varchar
  - name: iso_week
    type: integer
  - name: day_of_week_number
    type: integer
  - name: day_of_week_name
    type: varchar
  - name: is_working_day
    type: boolean
@bruin */

-- Deterministic generation. Every value is a pure function of the day offset.
-- Do not introduce random(), now(), or current_date - see docs/_data-design.md.

WITH seq AS (
    -- range(0, 1096) yields 0..1095: one number per day across three years.
    -- range() produces BIGINT; cast to INTEGER so DATE + i type-checks.
    SELECT CAST(i AS INTEGER) AS i FROM range(0, 1096) AS t(i)
)
SELECT
    (DATE '2023-01-01' + i)                                    AS date_day,
    CAST(strftime(DATE '2023-01-01' + i, '%Y%m%d') AS INTEGER) AS date_key,
    CAST(year(DATE '2023-01-01' + i) AS INTEGER)               AS year,
    CAST(quarter(DATE '2023-01-01' + i) AS INTEGER)            AS quarter,
    strftime(DATE '2023-01-01' + i, '%Y-%m')                   AS year_month,
    CAST(month(DATE '2023-01-01' + i) AS INTEGER)              AS month_number,
    monthname(DATE '2023-01-01' + i)                           AS month_name,
    CAST(week(DATE '2023-01-01' + i) AS INTEGER)               AS iso_week,
    CAST(isodow(DATE '2023-01-01' + i) AS INTEGER)             AS day_of_week_number,
    dayname(DATE '2023-01-01' + i)                             AS day_of_week_name,
    isodow(DATE '2023-01-01' + i) < 6                          AS is_working_day
FROM seq
ORDER BY date_day;
