/* @bruin
name: web_analytics_reports.gsc_competitor_visibility
type: bq.sql
description: >
  How the property performs on queries that name a competitor, broken out per
  rival. "acme vs notion", "notion alternatives", "notion pricing" — these are the
  highest-intent non-branded queries a B2B SaaS company can rank for, because the
  searcher has already decided to buy something in the category and is choosing
  between vendors. Search Console will show them scattered through a thousand-row
  export with no notion that they belong together, and it cannot tell you which
  rival you are losing to.

  Every competitor named in the competitor_names variable gets its own rows, so a
  gap is visible per rival rather than hidden inside one aggregate. position_band
  is the column to act on: appearing at position fourteen for "competitor
  alternatives" means the demand exists and the page is nowhere a searcher will
  look, which is usually the cheapest pipeline available to a startup.

  A row with no dedicated page — where top_page_role is 'content' rather than
  'product' — normally means a blog post is accidentally ranking for a comparison
  query. Those convert far worse than a real comparison page, and the fix is to
  build one.

materialization:
  type: table

depends:
  - web_analytics_staging.gsc_url_query_daily

tags:
  - web_analytics_reports
  - search_console
  - seo
  - b2b_saas

columns:
  - name: site_url
    type: STRING
    description: Search Console property.
    primary_key: true
    checks:
      - name: not_null
  - name: competitor_name
    type: STRING
    description: Competitor the query names, from the competitor_names variable.
    primary_key: true
    checks:
      - name: not_null
  - name: query
    type: STRING
    description: Search query naming the competitor.
    primary_key: true
    checks:
      - name: not_null
  - name: query_intent_type
    type: STRING
    description: >
      Intent classification of the query, which is 'competitor' for every row here
      because the competitor test runs before the others.
  - name: window_start_date
    type: DATE
    description: First date included.
  - name: window_end_date
    type: DATE
    description: Last date included.
  - name: search_impressions
    type: INT64
    description: Impressions across the window.
    checks:
      - name: positive
  - name: search_clicks
    type: INT64
    description: Clicks across the window.
    checks:
      - name: non_negative
  - name: search_ctr
    type: FLOAT64
    description: Clicks divided by impressions.
  - name: search_avg_position
    type: FLOAT64
    description: One-based average position, weighted by impressions.
  - name: position_band
    type: STRING
    description: >
      'top_3', 'page_one' for positions four to ten, 'page_two' for eleven to
      twenty, and 'beyond' past that. Anything below page one earns almost no
      clicks however high the impressions look.
    checks:
      - name: not_null
      - name: accepted_values
        value: ["top_3", "page_one", "page_two", "beyond"]
  - name: ranking_page_count
    type: INT64
    description: Host-qualified pages that earned impressions for this query.
    checks:
      - name: positive
  - name: top_page_hostname
    type: STRING
    description: Hostname of the page earning the most impressions for this query.
  - name: top_page_path
    type: STRING
    description: Page earning the most impressions for this query.
  - name: top_page_role
    type: STRING
    description: >
      Role of that page. 'content' on a comparison query usually means a blog post
      is ranking where a dedicated comparison page should be.
  - name: has_dedicated_comparison_page
    type: BOOL
    description: >
      Whether the best-ranking page is a product page rather than content or
      documentation.
    checks:
      - name: not_null

custom_checks:
  - name: competitor query grain is unique
    description: One row per property, competitor, and query.
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT
          site_url,
          competitor_name,
          query
        FROM {{ this }}
        GROUP BY 1, 2, 3
        HAVING COUNT(*) > 1
      )
    value: 0
  - name: every row names a competitor
    description: >
      A row without a competitor label does not belong in a competitor report.
    query: |
      SELECT COUNT(*)
      FROM {{ this }}
      WHERE competitor_name IS NULL
        OR query IS NULL
    value: 0
  - name: position band agrees with average position
    description: >
      The band is derived from average position, so the two must never disagree.
    query: |
      SELECT COUNT(*)
      FROM {{ this }}
      WHERE (position_band = 'top_3' AND search_avg_position >= 4)
        OR (position_band = 'beyond' AND search_avg_position <= 20)
    value: 0
@bruin */

WITH bounds AS (
  SELECT
    DATE_SUB(DATE('{{ end_date }}'), INTERVAL {{ var.reporting_window_days }} DAY) AS window_start,
    DATE('{{ end_date }}') AS window_end
),

competitor_query_page AS (
  SELECT
    daily.site_url,
    daily.competitor_name,
    daily.query,
    daily.query_intent_type,
    daily.page_hostname,
    daily.page_path,
    daily.page_role,
    SUM(daily.impressions) AS impressions,
    SUM(daily.clicks) AS clicks,
    SUM(daily.sum_position) AS sum_position
  FROM web_analytics_staging.gsc_url_query_daily AS daily
  CROSS JOIN bounds
  WHERE daily.data_date > bounds.window_start
    AND daily.data_date <= bounds.window_end
    AND daily.search_type = 'WEB'
    AND NOT daily.is_anonymized_query
    AND daily.query IS NOT NULL
    AND daily.competitor_name IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5, 6, 7
),

query_totals AS (
  SELECT
    site_url,
    competitor_name,
    query,
    MAX(query_intent_type) AS query_intent_type,
    COUNT(DISTINCT {{ page_identity('page_hostname', 'page_path') }}) AS ranking_page_count,
    SUM(impressions) AS search_impressions,
    SUM(clicks) AS search_clicks,
    SUM(sum_position) AS sum_position
  FROM competitor_query_page
  GROUP BY 1, 2, 3
  HAVING SUM(impressions) >= {{ var.min_query_impressions }}
),

top_page AS (
  SELECT
    site_url,
    competitor_name,
    query,
    page_hostname AS top_page_hostname,
    page_path AS top_page_path,
    page_role AS top_page_role
  FROM (
    SELECT
      site_url,
      competitor_name,
      query,
      page_hostname,
      page_path,
      page_role,
      ROW_NUMBER() OVER (
        PARTITION BY site_url, competitor_name, query
        ORDER BY impressions DESC, clicks DESC, page_hostname, page_path
      ) AS page_rank
    FROM competitor_query_page
  )
  WHERE page_rank = 1
)

SELECT
  totals.site_url,
  totals.competitor_name,
  totals.query,
  totals.query_intent_type,
  DATE_ADD(bounds.window_start, INTERVAL 1 DAY) AS window_start_date,
  bounds.window_end AS window_end_date,
  totals.search_impressions,
  totals.search_clicks,
  SAFE_DIVIDE(totals.search_clicks, NULLIF(totals.search_impressions, 0)) AS search_ctr,
  {{ average_position('totals.sum_position', 'totals.search_impressions') }}
    AS search_avg_position,
  CASE
    WHEN {{ average_position('totals.sum_position', 'totals.search_impressions') }} < 4
      THEN 'top_3'
    WHEN {{ average_position('totals.sum_position', 'totals.search_impressions') }} <= 10
      THEN 'page_one'
    WHEN {{ average_position('totals.sum_position', 'totals.search_impressions') }} <= 20
      THEN 'page_two'
    ELSE 'beyond'
  END AS position_band,
  totals.ranking_page_count,
  pages.top_page_hostname,
  pages.top_page_path,
  pages.top_page_role,
  COALESCE(pages.top_page_role = 'product', FALSE) AS has_dedicated_comparison_page
FROM query_totals AS totals
LEFT JOIN top_page AS pages
  USING (site_url, competitor_name, query)
CROSS JOIN bounds;
