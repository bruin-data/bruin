/* @bruin
name: web_analytics_reports.gsc_new_and_lost_queries
type: bq.sql
description: >
  Queries the property started ranking for, and queries it stopped ranking for,
  between the trailing window and the window before it. Search Console cannot
  answer this at all: comparing periods there shows how shared queries moved, not
  which queries appeared or disappeared, so finding them means exporting both
  periods and diffing them offline.

  Newly appearing queries show what Google now thinks the site is about, which is
  the fastest signal that published content landed or that a page's topic drifted.
  Disappearing queries are the earlier warning: a query usually vanishes from the
  export a few weeks before the page it belonged to shows a measurable click loss.

materialization:
  type: table

depends:
  - web_analytics_staging.gsc_url_query_daily

tags:
  - web_analytics_reports
  - search_console
  - seo

columns:
  - name: site_url
    type: STRING
    description: Search Console property.
    primary_key: true
    checks:
      - name: not_null
  - name: search_type
    type: STRING
    description: Search surface the query was measured on.
    primary_key: true
  - name: query
    type: STRING
    description: Search query that appeared or disappeared.
    primary_key: true
    checks:
      - name: not_null
  - name: query_change_type
    type: STRING
    description: >
      'new' when the query earned impressions only in the current window, 'lost'
      when only in the prior window.
    checks:
      - name: not_null
      - name: accepted_values
        value: ["new", "lost"]
  - name: query_brand_type
    type: STRING
    description: Whether the query is branded or non_branded.
  - name: query_word_count
    type: INT64
    description: Word count of the query.
  - name: top_page_hostname
    type: STRING
    description: Hostname of the page that earned the most impressions for this query.
  - name: top_page_path
    type: STRING
    description: >
      Page that earned the most impressions for this query, in whichever window
      the query was present.
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
  - name: prior_avg_position
    type: FLOAT64
    description: One-based average position in the prior window.
  - name: click_change
    type: INT64
    description: Current clicks minus prior clicks.
  - name: impression_change
    type: INT64
    description: Current impressions minus prior impressions.

custom_checks:
  - name: query change grain is unique
    description: One row per property, search type, and query.
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT
          site_url,
          search_type,
          query
        FROM {{ this }}
        GROUP BY 1, 2, 3
        HAVING COUNT(*) > 1
      )
    value: 0
  - name: new and lost queries appear in exactly one window
    description: >
      A new query must be absent from the prior window and a lost query absent
      from the current one, otherwise it is a retained query and does not belong
      in this report.
    query: |
      SELECT COUNT(*)
      FROM {{ this }}
      WHERE (query_change_type = 'new' AND prior_impressions > 0)
        OR (query_change_type = 'lost' AND current_impressions > 0)
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
    daily.search_type,
    daily.query,
    daily.query_brand_type,
    daily.query_word_count,
    daily.page_hostname,
    daily.page_path,
    daily.data_date > bounds.current_start AS is_current_window,
    daily.impressions,
    daily.clicks,
    daily.sum_position
  FROM web_analytics_staging.gsc_url_query_daily AS daily
  CROSS JOIN bounds
  WHERE daily.data_date > bounds.prior_start
    AND daily.data_date <= bounds.current_end
    AND NOT daily.is_anonymized_query
    AND daily.query IS NOT NULL
),

query_page AS (
  SELECT
    site_url,
    search_type,
    query,
    page_hostname,
    page_path,
    SUM(impressions) AS impressions
  FROM windowed
  GROUP BY 1, 2, 3, 4, 5
),

top_page AS (
  SELECT
    site_url,
    search_type,
    query,
    page_hostname AS top_page_hostname,
    page_path AS top_page_path
  FROM (
    SELECT
      site_url,
      search_type,
      query,
      page_hostname,
      page_path,
      ROW_NUMBER() OVER (
        PARTITION BY site_url, search_type, query
        ORDER BY impressions DESC, page_hostname, page_path
      ) AS page_rank
    FROM query_page
  )
  WHERE page_rank = 1
),

compared AS (
  SELECT
    site_url,
    search_type,
    query,
    MAX(query_brand_type) AS query_brand_type,
    MAX(query_word_count) AS query_word_count,
    SUM(IF(is_current_window, impressions, 0)) AS current_impressions,
    SUM(IF(is_current_window, clicks, 0)) AS current_clicks,
    SUM(IF(is_current_window, sum_position, 0)) AS current_sum_position,
    SUM(IF(is_current_window, 0, impressions)) AS prior_impressions,
    SUM(IF(is_current_window, 0, clicks)) AS prior_clicks,
    SUM(IF(is_current_window, 0, sum_position)) AS prior_sum_position
  FROM windowed
  GROUP BY 1, 2, 3
  HAVING (SUM(IF(is_current_window, impressions, 0)) = 0)
      != (SUM(IF(is_current_window, 0, impressions)) = 0)
    AND SUM(impressions) >= {{ var.min_query_impressions }}
)

SELECT
  compared.site_url,
  compared.search_type,
  compared.query,
  IF(compared.prior_impressions = 0, 'new', 'lost') AS query_change_type,
  compared.query_brand_type,
  compared.query_word_count,
  pages.top_page_hostname,
  pages.top_page_path,
  {{ var.trend_window_days }} AS window_days,
  DATE_ADD(bounds.current_start, INTERVAL 1 DAY) AS current_window_start_date,
  bounds.current_end AS current_window_end_date,
  DATE_ADD(bounds.prior_start, INTERVAL 1 DAY) AS prior_window_start_date,
  bounds.current_start AS prior_window_end_date,
  compared.current_impressions,
  compared.current_clicks,
  {{ average_position('compared.current_sum_position', 'compared.current_impressions') }}
    AS current_avg_position,
  compared.prior_impressions,
  compared.prior_clicks,
  {{ average_position('compared.prior_sum_position', 'compared.prior_impressions') }}
    AS prior_avg_position,
  compared.current_clicks - compared.prior_clicks AS click_change,
  compared.current_impressions - compared.prior_impressions AS impression_change
FROM compared
LEFT JOIN top_page AS pages
  USING (site_url, search_type, query)
CROSS JOIN bounds;
