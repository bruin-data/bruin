/* @bruin

name: fn.date_in_range
type: bq.sql

@bruin */

CREATE OR REPLACE FUNCTION fn.date_in_range(table_suffix STRING, start_dt DATE, end_dt DATE) AS 
(
    replace(table_suffix, 'intraday_', '') between format_date('%Y%m%d', start_dt) and format_date('%Y%m%d', end_dt)
    and
    replace(table_suffix, 'intraday_', '') between '20200101' and '21000101'
);
