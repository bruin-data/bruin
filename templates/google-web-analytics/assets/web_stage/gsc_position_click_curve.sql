/* @bruin
name: web_stage.gsc_position_click_curve
type: bq.sql
description: >
  Click-through rate the property actually earns at each search position, built
  from its own query and page history rather than a published industry curve.
  Nothing in Search Console answers "is this CTR good for where I rank?", which is
  the question that separates a page that needs a better title from a page that
  simply needs to rank higher.

  The curve is cut by search type but not by device, because splitting it further
  leaves too few impressions per position to be stable. Buckets under a thousand
  impressions are flagged rather than dropped so a consumer can decide whether to
  trust them.

materialization:
  type: table

depends:
  - web_stage.gsc_url_query_daily

tags:
  - web_stage
  - search_console
  - seo

columns:
  - name: site_url
    type: STRING
    description: Search Console property the curve was measured on.
    primary_key: true
    checks:
      - name: not_null
  - name: search_type
    type: STRING
    description: Search surface the curve applies to.
    primary_key: true
    checks:
      - name: not_null
  - name: position_bucket
    type: INT64
    description: One-based search position, floored from the row's average position.
    primary_key: true
    checks:
      - name: not_null
      - name: positive
  - name: bucket_impressions
    type: INT64
    description: Impressions observed at this position across the window.
    checks:
      - name: positive
  - name: bucket_clicks
    type: INT64
    description: Clicks observed at this position across the window.
    checks:
      - name: non_negative
  - name: expected_ctr
    type: FLOAT64
    description: Click-through rate the property earns at this position.
    checks:
      - name: non_negative
  - name: is_reliable_bucket
    type: BOOL
    description: Whether the bucket has at least 1000 impressions behind it.
    checks:
      - name: not_null
  - name: window_start_date
    type: DATE
    description: First reporting date included in the curve.
  - name: window_end_date
    type: DATE
    description: Last reporting date included in the curve.

custom_checks:
  - name: curve grain is unique
    description: One row per property, search type, and position.
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT
          site_url,
          search_type,
          position_bucket
        FROM {{ this }}
        GROUP BY 1, 2, 3
        HAVING COUNT(*) > 1
      )
    value: 0
  - name: expected ctr stays a rate
    description: A click-through rate cannot exceed one.
    query: |
      SELECT COUNT(*)
      FROM {{ this }}
      WHERE expected_ctr > 1
    value: 0
@bruin */

WITH bounds AS (
  SELECT
    DATE_SUB(DATE('{{ end_date }}'), INTERVAL {{ var.reporting_window_days }} DAY) AS window_start,
    DATE('{{ end_date }}') AS window_end
),

positioned AS (
  SELECT
    daily.site_url,
    daily.search_type,
    CAST(FLOOR({{ average_position('daily.sum_position', 'daily.impressions') }}) AS INT64)
      AS position_bucket,
    daily.impressions,
    daily.clicks,
    bounds.window_start,
    bounds.window_end
  FROM web_stage.gsc_url_query_daily AS daily
  CROSS JOIN bounds
  WHERE daily.data_date > bounds.window_start
    AND daily.data_date <= bounds.window_end
    AND daily.impressions > 0
)

SELECT
  site_url,
  search_type,
  position_bucket,
  SUM(impressions) AS bucket_impressions,
  SUM(clicks) AS bucket_clicks,
  SAFE_DIVIDE(SUM(clicks), NULLIF(SUM(impressions), 0)) AS expected_ctr,
  SUM(impressions) >= 1000 AS is_reliable_bucket,
  DATE_ADD(MIN(window_start), INTERVAL 1 DAY) AS window_start_date,
  MAX(window_end) AS window_end_date
FROM positioned
WHERE position_bucket BETWEEN 1 AND 50
GROUP BY 1, 2, 3;
