/* @bruin
name: web_analytics_reports.ga4_gsc_query_value
type: bq.sql
description: >
  Estimated engagement, key events, and revenue behind each search query. Google
  deliberately never passes the query to analytics, so no report in GA4 or Search
  Console can tell you which searches make money — teams end up prioritizing
  keywords by click volume and discovering months later that the highest-traffic
  terms were the least valuable.

  Value comes from session_outcome_value_usd, which prices key events with the
  key_event_values variable and adds any real ecommerce revenue. When revenue is
  recognized outside GA4, those weights are the only way this report says anything:
  a report built on GA4 purchase revenue alone would value every query at zero.
  Set them before ranking anything by them.

  The numbers here are modelled, not measured. Each page's GA4 outcomes are split
  across the queries that sent clicks to it, in proportion to those clicks. Within
  a page the weights sum to one, so a page's whole outcome is distributed and page
  totals are preserved exactly; only the split between queries is an estimate. The
  assumption the split rests on is that every query sending traffic to a page
  converts at that page's average rate, which is wrong in the interesting
  direction: a high-intent query really does convert better than the page average,
  so its value is understated here and a broad informational query's is overstated.
  Use this to rank queries against each other, not as a revenue figure to report.

  Only Google web-search clicks carrying a query Google chose to disclose can be
  allocated. Withheld clicks are left out of the denominator rather than held back,
  so their share of a page's outcome is spread across that page's disclosed queries
  and inflates them a little. A page whose clicks were all withheld contributes
  nothing, which is why site totals here fall short of the site totals in
  web_analytics_reports.ga4_gsc_landing_page_performance.

  The grain is one row per property and query, and the GA4 side is joined on
  hostname and path alone because GA4 has no concept of a Search Console property.
  If two properties covering the same pages export into one dataset — a domain
  property alongside a URL-prefix property, say — each receives a full allocation
  of those pages' outcomes. Per property the numbers are right, and that is how
  this report is meant to be read; summing the modelled columns across properties
  counts the same GA4 outcome once per property. Filter to one site_url before
  aggregating.

materialization:
  type: table

depends:
  - web_analytics_staging.gsc_url_query_daily
  - web_analytics_staging.ga4_sessions

tags:
  - web_analytics_reports
  - search_console
  - ga4
  - seo

columns:
  - name: site_url
    type: STRING
    description: Search Console property.
    primary_key: true
    checks:
      - name: not_null
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
  - name: window_start_date
    type: DATE
    description: First date included.
  - name: window_end_date
    type: DATE
    description: Last date included.
  - name: ranking_page_count
    type: INT64
    description: >
      Pages that earned clicks for this query, counted per host so the same path on
      two hosts counts as two pages.
    checks:
      - name: positive
  - name: top_page_hostname
    type: STRING
    description: Hostname of the page that earned the most clicks for this query.
  - name: top_page_path
    type: STRING
    description: Page that earned the most clicks for this query.
  - name: search_impressions
    type: INT64
    description: Impressions for the query across the window.
    checks:
      - name: positive
  - name: search_clicks
    type: INT64
    description: Clicks for the query across the window.
    checks:
      - name: positive
  - name: search_ctr
    type: FLOAT64
    description: Clicks divided by impressions.
  - name: search_avg_position
    type: FLOAT64
    description: One-based average position, weighted by impressions.
  - name: modelled_sessions
    type: FLOAT64
    description: Organic sessions allocated to this query from its landing pages.
  - name: modelled_engaged_sessions
    type: FLOAT64
    description: Engaged organic sessions allocated to this query.
  - name: modelled_key_events
    type: FLOAT64
    description: Key events allocated to this query.
  - name: modelled_demo_events
    type: FLOAT64
    description: >
      Demo or contact-sales requests allocated to this query. The clearest signal
      that a query brings buyers rather than readers.
  - name: modelled_signup_events
    type: FLOAT64
    description: Self-serve signups and trial starts allocated to this query.
  - name: modelled_outcome_value_usd
    type: FLOAT64
    description: >
      Modelled key-event value plus ecommerce revenue allocated to this query.
  - name: modelled_outcome_value_per_click_usd
    type: FLOAT64
    description: >
      Allocated value per search click, and the column to rank a keyword roadmap
      by: what one more click on this query is worth.
  - name: modelled_revenue_usd
    type: FLOAT64
    description: Purchase revenue in USD allocated to this query.
  - name: modelled_key_event_rate
    type: FLOAT64
    description: Allocated key events per allocated session.
  - name: modelled_revenue_per_click_usd
    type: FLOAT64
    description: >
      Allocated ecommerce revenue per search click. It is zero unless the property
      sends purchase revenue; rank on modelled_outcome_value_per_click_usd instead,
      which also counts priced key events.
  - name: modelled_value_per_thousand_impressions_usd
    type: FLOAT64
    description: >
      Allocated revenue per thousand impressions, which values the visibility the
      query already has rather than the traffic it converts today.

custom_checks:
  - name: query value grain is unique
    description: One row per property and query.
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT
          site_url,
          query
        FROM {{ this }}
        GROUP BY 1, 2
        HAVING COUNT(*) > 1
      )
    value: 0
  - name: anonymized queries are excluded
    description: A withheld query cannot be attributed to a page or a value.
    query: |
      SELECT COUNT(*)
      FROM {{ this }}
      WHERE query IS NULL
        OR query_brand_type = 'anonymized'
    value: 0
  - name: allocated measures are never negative
    description: >
      Allocation weights are non-negative shares of non-negative measures, so a
      negative result means the weights were built wrong.
    query: |
      SELECT COUNT(*)
      FROM {{ this }}
      WHERE modelled_sessions < 0
        OR modelled_key_events < 0
        OR modelled_demo_events < 0
        OR modelled_outcome_value_usd < 0
        OR modelled_revenue_usd < 0
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
    daily.page_hostname,
    daily.page_path,
    daily.query,
    daily.query_brand_type,
    SUM(daily.impressions) AS search_impressions,
    SUM(daily.clicks) AS search_clicks,
    SUM(daily.sum_position) AS sum_position
  FROM web_analytics_staging.gsc_url_query_daily AS daily
  CROSS JOIN bounds
  WHERE daily.data_date > bounds.window_start
    AND daily.data_date <= bounds.window_end
    AND daily.search_type = 'WEB'
    AND NOT daily.is_anonymized_query
    AND daily.query IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5
  HAVING SUM(daily.clicks) > 0
),

-- Denominator of the allocation. It counts only the clicks this report can
-- attribute, so the weights of a page's disclosed queries sum to exactly one.
page_click_totals AS (
  SELECT
    site_url,
    page_hostname,
    page_path,
    SUM(search_clicks) AS attributable_clicks
  FROM query_page
  GROUP BY 1, 2, 3
),

page_outcomes AS (
  SELECT
    sessions.landing_page_hostname AS page_hostname,
    sessions.landing_page_path AS page_path,
    COUNT(*) AS organic_sessions,
    COUNTIF(sessions.is_engaged_session) AS engaged_sessions,
    SUM(sessions.key_event_count) AS key_events,
    SUM(sessions.demo_event_count) AS demo_events,
    SUM(sessions.signup_event_count) AS signup_events,
    SUM(sessions.session_outcome_value_usd) AS outcome_value_usd,
    SUM(sessions.purchase_revenue_in_usd) AS purchase_revenue_usd
  FROM web_analytics_staging.ga4_sessions AS sessions
  CROSS JOIN bounds
  WHERE sessions.session_date > bounds.window_start
    AND sessions.session_date <= bounds.window_end
    AND sessions.is_google_organic_session
    AND sessions.landing_page_path IS NOT NULL
  GROUP BY 1, 2
),

allocated AS (
  SELECT
    query_page.site_url,
    query_page.query,
    query_page.query_brand_type,
    query_page.page_hostname,
    query_page.page_path,
    query_page.search_impressions,
    query_page.search_clicks,
    query_page.sum_position,
    SAFE_DIVIDE(query_page.search_clicks, NULLIF(totals.attributable_clicks, 0)) AS click_weight,
    COALESCE(outcomes.organic_sessions, 0) AS page_organic_sessions,
    COALESCE(outcomes.engaged_sessions, 0) AS page_engaged_sessions,
    COALESCE(outcomes.key_events, 0) AS page_key_events,
    COALESCE(outcomes.demo_events, 0) AS page_demo_events,
    COALESCE(outcomes.signup_events, 0) AS page_signup_events,
    COALESCE(outcomes.outcome_value_usd, 0) AS page_outcome_value_usd,
    COALESCE(outcomes.purchase_revenue_usd, 0) AS page_purchase_revenue_usd
  FROM query_page
  JOIN page_click_totals AS totals
    USING (site_url, page_hostname, page_path)
  LEFT JOIN page_outcomes AS outcomes
    ON outcomes.page_hostname = query_page.page_hostname
    AND outcomes.page_path = query_page.page_path
),

rolled_up AS (
  SELECT
    site_url,
    query,
    MAX(query_brand_type) AS query_brand_type,
    COUNT(DISTINCT {{ page_identity('page_hostname', 'page_path') }}) AS ranking_page_count,
    SUM(search_impressions) AS search_impressions,
    SUM(search_clicks) AS search_clicks,
    SUM(sum_position) AS sum_position,
    SUM(COALESCE(click_weight, 0) * page_organic_sessions) AS modelled_sessions,
    SUM(COALESCE(click_weight, 0) * page_engaged_sessions) AS modelled_engaged_sessions,
    SUM(COALESCE(click_weight, 0) * page_key_events) AS modelled_key_events,
    SUM(COALESCE(click_weight, 0) * page_demo_events) AS modelled_demo_events,
    SUM(COALESCE(click_weight, 0) * page_signup_events) AS modelled_signup_events,
    SUM(COALESCE(click_weight, 0) * page_outcome_value_usd) AS modelled_outcome_value_usd,
    SUM(COALESCE(click_weight, 0) * page_purchase_revenue_usd) AS modelled_revenue_usd
  FROM allocated
  GROUP BY 1, 2
  -- Qualified with the CTE name on purpose: BigQuery resolves a bare column in
  -- HAVING against the SELECT aliases first, and `search_impressions` is itself
  -- a SUM there, which it rejects as an aggregation of an aggregation.
  HAVING SUM(allocated.search_impressions) >= {{ var.min_query_impressions }}
),

top_page AS (
  SELECT
    site_url,
    query,
    page_hostname AS top_page_hostname,
    page_path AS top_page_path
  FROM (
    SELECT
      site_url,
      query,
      page_hostname,
      page_path,
      ROW_NUMBER() OVER (
        PARTITION BY site_url, query
        ORDER BY search_clicks DESC, search_impressions DESC, page_hostname, page_path
      ) AS page_rank
    FROM allocated
  )
  WHERE page_rank = 1
)

SELECT
  rolled_up.site_url,
  rolled_up.query,
  rolled_up.query_brand_type,
  DATE_ADD(bounds.window_start, INTERVAL 1 DAY) AS window_start_date,
  bounds.window_end AS window_end_date,
  rolled_up.ranking_page_count,
  pages.top_page_hostname,
  pages.top_page_path,
  rolled_up.search_impressions,
  rolled_up.search_clicks,
  SAFE_DIVIDE(rolled_up.search_clicks, NULLIF(rolled_up.search_impressions, 0)) AS search_ctr,
  {{ average_position('rolled_up.sum_position', 'rolled_up.search_impressions') }}
    AS search_avg_position,
  rolled_up.modelled_sessions,
  rolled_up.modelled_engaged_sessions,
  rolled_up.modelled_key_events,
  rolled_up.modelled_demo_events,
  rolled_up.modelled_signup_events,
  rolled_up.modelled_outcome_value_usd,
  SAFE_DIVIDE(rolled_up.modelled_outcome_value_usd, NULLIF(rolled_up.search_clicks, 0))
    AS modelled_outcome_value_per_click_usd,
  rolled_up.modelled_revenue_usd,
  SAFE_DIVIDE(rolled_up.modelled_key_events, NULLIF(rolled_up.modelled_sessions, 0))
    AS modelled_key_event_rate,
  SAFE_DIVIDE(rolled_up.modelled_revenue_usd, NULLIF(rolled_up.search_clicks, 0))
    AS modelled_revenue_per_click_usd,
  SAFE_DIVIDE(rolled_up.modelled_revenue_usd, NULLIF(rolled_up.search_impressions, 0)) * 1000
    AS modelled_value_per_thousand_impressions_usd
FROM rolled_up
LEFT JOIN top_page AS pages
  USING (site_url, query)
CROSS JOIN bounds;
