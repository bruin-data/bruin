/* @bruin
name: web_reports.search_page_trend
type: bq.sql
description: >
  Every page's search performance over the trailing window against the window
  before it, classified into growing, declining, stable, new, and lost. Search
  Console can compare two date ranges, but only for one view at a time and only
  within its thousand-row export limit, so the pages quietly bleeding traffic in
  the long tail never surface. Content decay is gradual by nature: a page loses a
  fifth of its clicks over a month without ever producing a visible drop.

  Position change is reported alongside click change because the two separate a
  ranking loss from a demand loss. Clicks down with position flat usually means
  seasonality or a new search feature taking the click; clicks down with position
  worse means the page lost ground.

materialization:
  type: table
  strategy: truncate+insert

depends:
  - web_stage.gsc_url_query_daily

tags:
  - web_reports
  - search_console
  - seo

columns:
  - name: site_url
    type: STRING
    description: Search Console property.
    primary_key: true
    checks:
      - name: not_null
  - name: page_path
    type: STRING
    description: Normalized page path.
    primary_key: true
    checks:
      - name: not_null
  - name: trend_class
    type: STRING
    description: >
      'new' when the page had no prior impressions, 'lost' when it has none now,
      'declining' and 'growing' when clicks moved by at least 20%, and 'stable'
      otherwise.
    checks:
      - name: not_null
      - name: accepted_values
        value: ["new", "lost", "declining", "growing", "stable"]
  - name: window_days
    type: INT64
    description: Length of each comparison window in days.
  - name: current_window_start_date
    type: DATE
    description: First date of the current window.
  - name: current_window_end_date
    type: DATE
    description: Last date of the current window.
  - name: prior_window_start_date
    type: DATE
    description: First date of the prior window.
  - name: prior_window_end_date
    type: DATE
    description: Last date of the prior window.
  - name: current_impressions
    type: INT64
    description: Impressions in the current window.
    checks:
      - name: non_negative
  - name: current_clicks
    type: INT64
    description: Clicks in the current window.
    checks:
      - name: non_negative
  - name: current_ctr
    type: FLOAT64
    description: CTR in the current window.
  - name: current_avg_position
    type: FLOAT64
    description: One-based average position in the current window.
  - name: prior_impressions
    type: INT64
    description: Impressions in the prior window.
    checks:
      - name: non_negative
  - name: prior_clicks
    type: INT64
    description: Clicks in the prior window.
    checks:
      - name: non_negative
  - name: prior_ctr
    type: FLOAT64
    description: CTR in the prior window.
  - name: prior_avg_position
    type: FLOAT64
    description: One-based average position in the prior window.
  - name: click_change
    type: INT64
    description: Current clicks minus prior clicks.
  - name: click_change_rate
    type: FLOAT64
    description: Click change relative to the prior window, NULL without prior clicks.
  - name: impression_change
    type: INT64
    description: Current impressions minus prior impressions.
  - name: position_change
    type: FLOAT64
    description: >
      Current average position minus prior average position. Negative is an
      improvement because position one is the best rank.
  - name: distinct_query_count
    type: INT64
    description: Distinct non-anonymized queries the page served in the current window.

custom_checks:
  - name: page trend grain is unique
    description: One row per property and page.
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT
          site_url,
          page_path
        FROM {{ this }}
        GROUP BY 1, 2
        HAVING COUNT(*) > 1
      )
    value: 0
  - name: comparison windows do not overlap
    description: >
      The prior window must end the day before the current window starts,
      otherwise the same day would be counted on both sides of the comparison.
    query: |
      SELECT COUNT(*)
      FROM {{ this }}
      WHERE prior_window_end_date >= current_window_start_date
    value: 0
@bruin */

WITH bounds AS (
  SELECT
    DATE('{{ end_date }}') AS current_end,
    DATE_SUB(DATE('{{ end_date }}'), INTERVAL {{ var.trend_window_days }} DAY) AS current_start,
    DATE_SUB(
      DATE_SUB(DATE('{{ end_date }}'), INTERVAL {{ var.trend_window_days }} DAY),
      INTERVAL {{ var.trend_window_days }} DAY
    ) AS prior_start
),

windowed AS (
  SELECT
    daily.site_url,
    daily.page_path,
    daily.data_date > bounds.current_start AS is_current_window,
    daily.impressions,
    daily.clicks,
    daily.sum_position,
    IF(daily.is_anonymized_query, NULL, daily.query) AS query
  FROM web_stage.gsc_url_query_daily AS daily
  CROSS JOIN bounds
  WHERE daily.data_date > bounds.prior_start
    AND daily.data_date <= bounds.current_end
),

compared AS (
  SELECT
    site_url,
    page_path,
    SUM(IF(is_current_window, impressions, 0)) AS current_impressions,
    SUM(IF(is_current_window, clicks, 0)) AS current_clicks,
    SUM(IF(is_current_window, sum_position, 0)) AS current_sum_position,
    SUM(IF(is_current_window, 0, impressions)) AS prior_impressions,
    SUM(IF(is_current_window, 0, clicks)) AS prior_clicks,
    SUM(IF(is_current_window, 0, sum_position)) AS prior_sum_position,
    COUNT(DISTINCT IF(is_current_window, query, NULL)) AS distinct_query_count
  FROM windowed
  GROUP BY 1, 2
  HAVING SUM(impressions) >= {{ var.min_page_impressions }}
)

SELECT
  compared.site_url,
  compared.page_path,
  CASE
    WHEN compared.prior_impressions = 0 AND compared.current_impressions > 0 THEN 'new'
    WHEN compared.current_impressions = 0 AND compared.prior_impressions > 0 THEN 'lost'
    WHEN compared.prior_clicks > 0
      AND SAFE_DIVIDE(compared.current_clicks - compared.prior_clicks, compared.prior_clicks)
        <= -0.2 THEN 'declining'
    WHEN compared.prior_clicks > 0
      AND SAFE_DIVIDE(compared.current_clicks - compared.prior_clicks, compared.prior_clicks)
        >= 0.2 THEN 'growing'
    ELSE 'stable'
  END AS trend_class,
  {{ var.trend_window_days }} AS window_days,
  DATE_ADD(bounds.current_start, INTERVAL 1 DAY) AS current_window_start_date,
  bounds.current_end AS current_window_end_date,
  DATE_ADD(bounds.prior_start, INTERVAL 1 DAY) AS prior_window_start_date,
  bounds.current_start AS prior_window_end_date,
  compared.current_impressions,
  compared.current_clicks,
  SAFE_DIVIDE(compared.current_clicks, NULLIF(compared.current_impressions, 0)) AS current_ctr,
  {{ average_position('compared.current_sum_position', 'compared.current_impressions') }}
    AS current_avg_position,
  compared.prior_impressions,
  compared.prior_clicks,
  SAFE_DIVIDE(compared.prior_clicks, NULLIF(compared.prior_impressions, 0)) AS prior_ctr,
  {{ average_position('compared.prior_sum_position', 'compared.prior_impressions') }}
    AS prior_avg_position,
  compared.current_clicks - compared.prior_clicks AS click_change,
  SAFE_DIVIDE(
    compared.current_clicks - compared.prior_clicks,
    NULLIF(compared.prior_clicks, 0)
  ) AS click_change_rate,
  compared.current_impressions - compared.prior_impressions AS impression_change,
  {{ average_position('compared.current_sum_position', 'compared.current_impressions') }}
    - {{ average_position('compared.prior_sum_position', 'compared.prior_impressions') }}
    AS position_change,
  compared.distinct_query_count
FROM compared
CROSS JOIN bounds;
