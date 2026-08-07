/* @bruin
name: web_reports.search_query_opportunities
type: bq.sql
description: >
  Queries worth acting on, sized by the clicks they are leaving on the table.
  Search Console shows position and CTR side by side but never says whether a CTR
  is good for that position, so a 2% CTR at position 3 and a 2% CTR at position 15
  look identical in the interface even though only the first is a problem. Each
  query here is compared against the click curve the property itself earns at its
  position.

  Two sizings are given. clicks_at_expected_ctr is what fixing the snippet alone
  would return, holding position constant. clicks_at_top_three_ctr is what ranking
  in the top three would return, holding impressions constant. Both are estimates
  from observed behaviour, not forecasts: impressions move when position moves.

materialization:
  type: table

depends:
  - web_stage.gsc_url_query_daily
  - web_stage.gsc_position_click_curve

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
    description: Search query.
    primary_key: true
    checks:
      - name: not_null
  - name: query_brand_type
    type: STRING
    description: Whether the query is branded or non_branded.
    checks:
      - name: accepted_values
        value: ["branded", "non_branded"]
  - name: query_word_count
    type: INT64
    description: Word count of the query, a rough proxy for how specific the intent is.
  - name: opportunity_type
    type: STRING
    description: >
      Why the query is listed. 'striking_distance' ranks just off the first page,
      'underperforming_ctr' ranks well but is not being clicked,
      'impressions_no_clicks' earns visibility but no traffic at all, and 'monitor'
      is already performing in line with its position.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - striking_distance
          - underperforming_ctr
          - impressions_no_clicks
          - monitor
  - name: top_page_hostname
    type: STRING
    description: Hostname of the page that earns the most clicks for this query.
  - name: top_page_path
    type: STRING
    description: Page that earns the most clicks for this query.
  - name: ranking_page_count
    type: INT64
    description: >
      Pages that earned impressions for this query, counted per host so the same
      path on two hosts counts as two pages. More than one means the property is
      competing with itself; see web_reports.search_query_cannibalization.
    checks:
      - name: positive
  - name: impressions
    type: INT64
    description: Impressions across the reporting window.
    checks:
      - name: positive
  - name: clicks
    type: INT64
    description: Clicks across the reporting window.
    checks:
      - name: non_negative
  - name: ctr
    type: FLOAT64
    description: Clicks divided by impressions.
  - name: avg_position
    type: FLOAT64
    description: One-based average position, weighted by impressions.
  - name: position_bucket
    type: INT64
    description: Floored average position, used to look up the click curve.
  - name: expected_ctr
    type: FLOAT64
    description: >
      CTR the property normally earns at this position. NULL when the property has
      too little history at that position.
  - name: is_expected_ctr_reliable
    type: BOOL
    description: Whether the click-curve bucket behind expected_ctr is well populated.
  - name: ctr_gap
    type: FLOAT64
    description: Actual CTR minus expected CTR. Negative means the snippet underperforms.
  - name: clicks_at_expected_ctr
    type: INT64
    description: Additional clicks a normal CTR would return at the current position.
    checks:
      - name: non_negative
  - name: clicks_at_top_three_ctr
    type: INT64
    description: Additional clicks a top-three position would return at current impressions.
    checks:
      - name: non_negative
  - name: window_start_date
    type: DATE
    description: First reporting date included.
  - name: window_end_date
    type: DATE
    description: Last reporting date included.

custom_checks:
  - name: query opportunity grain is unique
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
  - name: anonymized queries are excluded
    description: >
      Opportunity sizing needs a real query to act on, so withheld rows must not
      reach this report.
    query: |
      SELECT COUNT(*)
      FROM {{ this }}
      WHERE query IS NULL
        OR query_brand_type = 'anonymized'
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
    daily.query_word_count,
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
  GROUP BY 1, 2, 3, 4, 5, 6, 7
),

query_totals AS (
  SELECT
    site_url,
    search_type,
    query,
    MAX(query_brand_type) AS query_brand_type,
    MAX(query_word_count) AS query_word_count,
    COUNT(DISTINCT {{ page_identity('page_hostname', 'page_path') }}) AS ranking_page_count,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(sum_position) AS sum_position
  FROM query_page
  GROUP BY 1, 2, 3
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
        ORDER BY clicks DESC, impressions DESC, page_hostname, page_path
      ) AS page_rank
    FROM query_page
  )
  WHERE page_rank = 1
),

scored AS (
  SELECT
    totals.site_url,
    totals.search_type,
    totals.query,
    totals.query_brand_type,
    totals.query_word_count,
    pages.top_page_hostname,
    pages.top_page_path,
    totals.ranking_page_count,
    totals.impressions,
    totals.clicks,
    SAFE_DIVIDE(totals.clicks, NULLIF(totals.impressions, 0)) AS ctr,
    {{ average_position('totals.sum_position', 'totals.impressions') }} AS avg_position,
    CAST(
      FLOOR({{ average_position('totals.sum_position', 'totals.impressions') }}) AS INT64
    ) AS position_bucket
  FROM query_totals AS totals
  LEFT JOIN top_page AS pages
    USING (site_url, search_type, query)
  WHERE totals.impressions >= {{ var.min_query_impressions }}
),

benchmarked AS (
  SELECT
    scored.*,
    curve.expected_ctr,
    COALESCE(curve.is_reliable_bucket, FALSE) AS is_expected_ctr_reliable,
    top_three.expected_ctr AS top_three_expected_ctr
  FROM scored
  LEFT JOIN web_stage.gsc_position_click_curve AS curve
    ON curve.site_url = scored.site_url
    AND curve.search_type = scored.search_type
    AND curve.position_bucket = scored.position_bucket
  LEFT JOIN web_stage.gsc_position_click_curve AS top_three
    ON top_three.site_url = scored.site_url
    AND top_three.search_type = scored.search_type
    AND top_three.position_bucket = 3
)

SELECT
  benchmarked.site_url,
  benchmarked.search_type,
  benchmarked.query,
  benchmarked.query_brand_type,
  benchmarked.query_word_count,
  CASE
    WHEN benchmarked.avg_position BETWEEN 8 AND 20 THEN 'striking_distance'
    WHEN benchmarked.clicks = 0 THEN 'impressions_no_clicks'
    WHEN benchmarked.expected_ctr IS NOT NULL
      AND benchmarked.ctr < benchmarked.expected_ctr * 0.6 THEN 'underperforming_ctr'
    ELSE 'monitor'
  END AS opportunity_type,
  benchmarked.top_page_hostname,
  benchmarked.top_page_path,
  benchmarked.ranking_page_count,
  benchmarked.impressions,
  benchmarked.clicks,
  benchmarked.ctr,
  benchmarked.avg_position,
  benchmarked.position_bucket,
  benchmarked.expected_ctr,
  benchmarked.is_expected_ctr_reliable,
  benchmarked.ctr - benchmarked.expected_ctr AS ctr_gap,
  GREATEST(
    0,
    CAST(ROUND(benchmarked.impressions * COALESCE(benchmarked.expected_ctr, 0)) AS INT64)
      - benchmarked.clicks
  ) AS clicks_at_expected_ctr,
  GREATEST(
    0,
    CAST(ROUND(benchmarked.impressions * COALESCE(benchmarked.top_three_expected_ctr, 0)) AS INT64)
      - benchmarked.clicks
  ) AS clicks_at_top_three_ctr,
  DATE_ADD(bounds.window_start, INTERVAL 1 DAY) AS window_start_date,
  bounds.window_end AS window_end_date
FROM benchmarked
CROSS JOIN bounds;
