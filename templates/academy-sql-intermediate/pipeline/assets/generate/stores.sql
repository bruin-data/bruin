/* @bruin
name: stores
type: duckdb.sql

description: "Retail stores."

materialization:
  type: table
  strategy: create+replace

columns:
  - name: store_id
    type: integer
  - name: store_code
    type: varchar
  - name: country_code
    type: varchar
  - name: country_name
    type: varchar
  - name: city
    type: varchar
  - name: opened_on
    type: date
  - name: closed_on
    type: date
  - name: status
    type: varchar
  - name: timezone
    type: varchar
@bruin */

-- Hand-written dimension. Deterministic by construction.
-- Paris closed on 2026-01-31, after the last order in the data, so no order
-- postdates its own store's closing date.

SELECT * FROM (
    VALUES
        (1, 'NYC01', 'US', 'United States',  'New York', DATE '2019-03-15', CAST(NULL AS DATE), 'open',   'America/New_York'),
        (2, 'LON01', 'GB', 'United Kingdom', 'London',   DATE '2020-06-01', CAST(NULL AS DATE), 'open',   'Europe/London'),
        (3, 'BER01', 'DE', 'Germany',        'Berlin',   DATE '2021-01-20', CAST(NULL AS DATE), 'open',   'Europe/Berlin'),
        (4, 'TOR01', 'CA', 'Canada',         'Toronto',  DATE '2020-11-10', CAST(NULL AS DATE), 'open',   'America/Toronto'),
        (5, 'SYD01', 'AU', 'Australia',      'Sydney',   DATE '2022-02-14', CAST(NULL AS DATE), 'open',   'Australia/Sydney'),
        (6, 'PAR01', 'FR', 'France',         'Paris',    DATE '2018-09-01', DATE '2026-01-31',  'closed', 'Europe/Paris')
) AS s(store_id, store_code, country_code, country_name, city, opened_on, closed_on, status, timezone)
ORDER BY store_id;
