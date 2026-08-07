/* @bruin
name: web_reports.search_query_cannibalization
type: bq.sql
description: >
  Queries that more than one page on the property competes for. The Search Console
  interface can filter by query or group by page, but it cannot pivot one against
  the other, so finding these means exporting both dimensions and reconciling them
  by hand. Splitting a query across several pages divides the signals Google uses
  to rank any one of them and usually leaves all of them ranking worse than a
  single consolidated page would.

  Not every match is a problem. A query legitimately served by a category page and
  a product page will appear here with a high primary page click share, which is
  why the severity column keys off how evenly the clicks are split rather than off
  the page count alone.

materialization:
  type: table

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
  - name: search_type
    type: STRING
    description: Search surface the query was measured on.
    primary_key: true
  - name: query
    type: STRING
    description: Search query served by more than one page.
    primary_key: true
    checks:
      - name: not_null
  - name: query_brand_type
    type: STRING
    description: Whether the query is branded or non_branded.
  - name: ranking_page_count
    type: INT64
    description: >
      Pages that earned impressions for this query, counted per host so the same
      path served by two hosts counts as two competing pages.
    checks:
      - name: not_null
  - name: query_impressions
    type: INT64
    description: Impressions across every competing page.
    checks:
      - name: positive
  - name: query_clicks
    type: INT64
    description: Clicks across every competing page.
    checks:
      - name: non_negative
  - name: primary_page_hostname
    type: STRING
    description: Hostname of the page with the most clicks for this query.
  - name: primary_page_path
    type: STRING
    description: Page with the most clicks for this query.
    checks:
      - name: not_null
  - name: primary_page_impressions
    type: INT64
    description: Impressions the primary page earned.
  - name: primary_page_clicks
    type: INT64
    description: Clicks the primary page earned.
  - name: primary_page_avg_position
    type: FLOAT64
    description: One-based average position of the primary page.
  - name: primary_page_click_share
    type: FLOAT64
    description: >
      Share of the query's clicks the primary page earned. The lower this is, the
      more evenly demand is being split.
  - name: competing_page_hostname
    type: STRING
    description: Hostname of the second-ranked page for this query.
  - name: competing_page_path
    type: STRING
    description: Second-ranked page for this query.
    checks:
      - name: not_null
  - name: competing_page_impressions
    type: INT64
    description: Impressions the competing page earned.
  - name: competing_page_clicks
    type: INT64
    description: Clicks the competing page earned.
  - name: competing_page_avg_position
    type: FLOAT64
    description: One-based average position of the competing page.
  - name: cannibalization_severity
    type: STRING
    description: >
      'high' when the primary page holds less than 60% of the clicks and the
      competing page has real visibility, 'medium' when it holds less than 85%, and
      'low' otherwise.
    checks:
      - name: not_null
      - name: accepted_values
        value: ["high", "medium", "low"]
  - name: window_start_date
    type: DATE
    description: First reporting date included.
  - name: window_end_date
    type: DATE
    description: Last reporting date included.

custom_checks:
  - name: cannibalization grain is unique
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
  - name: every listed query has at least two pages
    description: A query served by a single page is not being cannibalized.
    query: |
      SELECT COUNT(*)
      FROM {{ this }}
      WHERE ranking_page_count < 2
    value: 0
@bruin */

WITH bounds AS (
  SELECT
    DATE_SUB(DATE('{{ end_date }}'), INTERVAL {{ var.reporting_window_days }} DAY) AS window_start,
    DATE('{{ end_date }}') AS window_end
),

query_page AS (
  SELECT
    daily.site_url,
    daily.search_type,
    daily.query,
    daily.query_brand_type,
    daily.page_hostname,
    daily.page_path,
    SUM(daily.impressions) AS impressions,
    SUM(daily.clicks) AS clicks,
    SUM(daily.sum_position) AS sum_position
  FROM web_stage.gsc_url_query_daily AS daily
  CROSS JOIN bounds
  WHERE daily.data_date > bounds.window_start
    AND daily.data_date <= bounds.window_end
    AND NOT daily.is_anonymized_query
    AND daily.query IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5, 6
),

ranked AS (
  SELECT
    site_url,
    search_type,
    query,
    query_brand_type,
    page_hostname,
    page_path,
    impressions,
    clicks,
    {{ average_position('sum_position', 'impressions') }} AS avg_position,
    ROW_NUMBER() OVER (
      PARTITION BY site_url, search_type, query
      ORDER BY clicks DESC, impressions DESC, page_hostname, page_path
    ) AS page_rank,
    COUNT(*) OVER (PARTITION BY site_url, search_type, query) AS ranking_page_count,
    SUM(impressions) OVER (PARTITION BY site_url, search_type, query) AS query_impressions,
    SUM(clicks) OVER (PARTITION BY site_url, search_type, query) AS query_clicks
  FROM query_page
),

paired AS (
  SELECT
    site_url,
    search_type,
    query,
    MAX(query_brand_type) AS query_brand_type,
    MAX(ranking_page_count) AS ranking_page_count,
    MAX(query_impressions) AS query_impressions,
    MAX(query_clicks) AS query_clicks,
    MAX(IF(page_rank = 1, page_hostname, NULL)) AS primary_page_hostname,
    MAX(IF(page_rank = 1, page_path, NULL)) AS primary_page_path,
    MAX(IF(page_rank = 1, impressions, NULL)) AS primary_page_impressions,
    MAX(IF(page_rank = 1, clicks, NULL)) AS primary_page_clicks,
    MAX(IF(page_rank = 1, avg_position, NULL)) AS primary_page_avg_position,
    MAX(IF(page_rank = 2, page_hostname, NULL)) AS competing_page_hostname,
    MAX(IF(page_rank = 2, page_path, NULL)) AS competing_page_path,
    MAX(IF(page_rank = 2, impressions, NULL)) AS competing_page_impressions,
    MAX(IF(page_rank = 2, clicks, NULL)) AS competing_page_clicks,
    MAX(IF(page_rank = 2, avg_position, NULL)) AS competing_page_avg_position
  FROM ranked
  GROUP BY 1, 2, 3
  HAVING MAX(ranking_page_count) >= 2
    AND MAX(query_impressions) >= {{ var.min_query_impressions }}
)

SELECT
  paired.site_url,
  paired.search_type,
  paired.query,
  paired.query_brand_type,
  paired.ranking_page_count,
  paired.query_impressions,
  paired.query_clicks,
  paired.primary_page_hostname,
  paired.primary_page_path,
  paired.primary_page_impressions,
  paired.primary_page_clicks,
  paired.primary_page_avg_position,
  SAFE_DIVIDE(paired.primary_page_clicks, NULLIF(paired.query_clicks, 0))
    AS primary_page_click_share,
  paired.competing_page_hostname,
  paired.competing_page_path,
  paired.competing_page_impressions,
  paired.competing_page_clicks,
  paired.competing_page_avg_position,
  CASE
    WHEN COALESCE(
        SAFE_DIVIDE(paired.primary_page_clicks, NULLIF(paired.query_clicks, 0)),
        1
      ) < 0.6
      AND SAFE_DIVIDE(paired.competing_page_impressions, NULLIF(paired.query_impressions, 0)) >= 0.1
      THEN 'high'
    WHEN COALESCE(
        SAFE_DIVIDE(paired.primary_page_clicks, NULLIF(paired.query_clicks, 0)),
        1
      ) < 0.85
      THEN 'medium'
    ELSE 'low'
  END AS cannibalization_severity,
  DATE_ADD(bounds.window_start, INTERVAL 1 DAY) AS window_start_date,
  bounds.window_end AS window_end_date
FROM paired
CROSS JOIN bounds;
