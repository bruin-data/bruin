/* @bruin
name: stores
type: duckdb.sql

description: >-
  One row per physical store, six in total. A small dimension table written out
  by hand so the cities and countries read like real ones. Generated sample data
  for the Agentic Data Analysis course. Deterministic: the same rows are produced
  on every run.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: store_id
    type: integer
    description: "Unique identifier for the store. One row per store_id. Primary key."
    primary_key: true
    checks:
      - name: unique
      - name: not_null
  - name: store_code
    type: varchar
    description: "Short human-readable code for the store, e.g. 'NYC01'."
  - name: country_code
    type: varchar
    description: "Two-letter country code, e.g. 'US'."
  - name: country_name
    type: varchar
    description: "Full country name, e.g. 'United States'."
  - name: city
    type: varchar
    description: "City the store is in."
  - name: opened_on
    type: date
    description: "The date the store opened."
  - name: closed_on
    type: date
    description: "The date the store closed, or null if it is still open."
  - name: status
    type: varchar
    description: "'open' or 'closed'."
  - name: timezone
    type: varchar
    description: "IANA timezone name for the store's local time."
@bruin */

-- Hand-written dimension. Six stores, one closed. Deterministic by construction.
-- Paris closed on 2026-01-31, after the last order in the data, so no order
-- postdates its own store's closing date. The later courses use closed_on; the
-- beginner course never touches it.

SELECT * FROM (
    VALUES
        (1, 'NYC01', 'US', 'United States', 'New York',  DATE '2019-03-15', CAST(NULL AS DATE), 'open',   'America/New_York'),
        (2, 'LON01', 'GB', 'United Kingdom', 'London',   DATE '2020-06-01', CAST(NULL AS DATE), 'open',   'Europe/London'),
        (3, 'BER01', 'DE', 'Germany',        'Berlin',   DATE '2021-01-20', CAST(NULL AS DATE), 'open',   'Europe/Berlin'),
        (4, 'TOR01', 'CA', 'Canada',         'Toronto',  DATE '2020-11-10', CAST(NULL AS DATE), 'open',   'America/Toronto'),
        (5, 'SYD01', 'AU', 'Australia',      'Sydney',   DATE '2022-02-14', CAST(NULL AS DATE), 'open',   'Australia/Sydney'),
        (6, 'PAR01', 'FR', 'France',         'Paris',    DATE '2018-09-01', DATE '2026-01-31',  'closed', 'Europe/Paris')
) AS s(store_id, store_code, country_code, country_name, city, opened_on, closed_on, status, timezone)
ORDER BY store_id;
