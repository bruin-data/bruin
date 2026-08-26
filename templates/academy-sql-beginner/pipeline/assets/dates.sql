/* @bruin
name: dates
type: duckdb.sql

description: >-
  One row per calendar day from 2023-01-01 to 2025-12-31 (1,096 days). A date
  spine you can join to so that every day appears in a report, even days with no
  orders. Generated sample data for the Agentic Data Analysis course.
  Deterministic: the same rows are produced on every run.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: date_day
    type: date
    description: "The calendar day. One row per day. Primary key."
    primary_key: true
    checks:
      - name: unique
      - name: not_null
  - name: date_key
    type: integer
    description: "The day as an integer YYYYMMDD, handy for joins and sorting."
  - name: year
    type: integer
    description: "Calendar year, e.g. 2024."
  - name: quarter
    type: integer
    description: "Calendar quarter, 1 to 4."
  - name: year_month
    type: varchar
    description: "Year and month as text, e.g. '2024-07'. Groups a whole month."
  - name: month_number
    type: integer
    description: "Month as a number, 1 to 12."
  - name: month_name
    type: varchar
    description: "Full month name in English, e.g. 'July'."
  - name: iso_week
    type: integer
    description: "ISO week number, 1 to 53."
  - name: day_of_week_number
    type: integer
    description: "ISO day of week, 1 = Monday through 7 = Sunday."
  - name: day_of_week_name
    type: varchar
    description: "Full weekday name in English, e.g. 'Monday'."
  - name: is_working_day
    type: boolean
    description: "True on Monday to Friday, false at the weekend."
@bruin */

-- Deterministic generation. Every value is a pure function of the day offset.
-- Do not introduce random(), now(), or current_date - see docs/data-design.md.

WITH seq AS (
    -- range(0, 1096) yields 0..1095: one number per day across three years.
    -- range() produces BIGINT; cast to INTEGER so DATE + i type-checks.
    SELECT CAST(i AS INTEGER) AS i FROM range(0, 1096) AS t(i)
)
SELECT
    (DATE '2023-01-01' + i)                                  AS date_day,
    CAST(strftime(DATE '2023-01-01' + i, '%Y%m%d') AS INTEGER) AS date_key,
    CAST(year(DATE '2023-01-01' + i) AS INTEGER)             AS year,
    CAST(quarter(DATE '2023-01-01' + i) AS INTEGER)          AS quarter,
    strftime(DATE '2023-01-01' + i, '%Y-%m')                 AS year_month,
    CAST(month(DATE '2023-01-01' + i) AS INTEGER)            AS month_number,
    monthname(DATE '2023-01-01' + i)                         AS month_name,
    CAST(week(DATE '2023-01-01' + i) AS INTEGER)             AS iso_week,
    CAST(isodow(DATE '2023-01-01' + i) AS INTEGER)           AS day_of_week_number,
    dayname(DATE '2023-01-01' + i)                           AS day_of_week_name,
    isodow(DATE '2023-01-01' + i) < 6                        AS is_working_day
FROM seq
ORDER BY date_day;
