/* @bruin
name: web_analytics_reports.ga4_gsc_intent_pipeline
type: bq.sql
description: >
  What each kind of search demand actually contributes, crossed with the kind of
  page that serves it. This is the report that settles the recurring B2B SaaS
  argument about whether content marketing works: a blog earning fifty thousand
  informational clicks and two demo requests, next to eleven hundred competitor
  comparison clicks and forty, is an unambiguous answer that no dashboard in GA4 or
  Search Console can produce. Search Console knows the query but not the outcome,
  GA4 knows the outcome but never receives the query, and neither has a concept of
  commercial intent at all.

  Read it as a portfolio: informational content usually earns most of the clicks and
  little of the pipeline, and that is not automatically wrong, because it is how
  a site earns the authority to rank for commercial terms later. What matters is
  whether the commercial and competitor rows are growing, and whether the value per
  click gap between them and informational content is as wide as it usually is.

  Outcomes are modelled the same way as web_analytics_reports.ga4_gsc_query_value: each
  page's GA4 results are split across the queries that sent it clicks, in
  proportion to those clicks. Within a page the weights sum to one, so page totals
  are preserved and only the split across intents is estimated. Withheld queries
  carry no intent and are excluded, so the totals here fall short of the site
  totals in web_analytics_reports.ga4_gsc_landing_page_performance by whatever share of
  clicks Google chose not to disclose.

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
  - b2b_saas

columns:
  - name: site_url
    type: STRING
    description: Search Console property.
    primary_key: true
    checks:
      - name: not_null
  - name: query_intent_type
    type: STRING
    description: >
      Commercial intent of the demand: 'competitor', 'branded', 'commercial', or
      'informational'.
    primary_key: true
    checks:
      - name: not_null
      - name: accepted_values
        value: ["competitor", "branded", "commercial", "informational"]
  - name: page_role
    type: STRING
    description: >
      Kind of page that served the demand: 'product', 'content', 'support', or
      'unknown'.
    primary_key: true
    checks:
      - name: not_null
  - name: window_start_date
    type: DATE
    description: First date included.
  - name: window_end_date
    type: DATE
    description: Last date included.
  - name: distinct_query_count
    type: INT64
    description: Distinct disclosed queries in this cell.
    checks:
      - name: positive
  - name: distinct_page_count
    type: INT64
    description: Distinct host-qualified pages in this cell.
    checks:
      - name: positive
  - name: search_impressions
    type: INT64
    description: Impressions across the window.
    checks:
      - name: positive
  - name: search_clicks
    type: INT64
    description: Clicks across the window.
    checks:
      - name: positive
  - name: search_ctr
    type: FLOAT64
    description: Clicks divided by impressions.
  - name: search_avg_position
    type: FLOAT64
    description: One-based average position, weighted by impressions.
  - name: click_share
    type: FLOAT64
    description: Share of the property's disclosed clicks landing in this cell.
  - name: modelled_sessions
    type: FLOAT64
    description: Organic sessions allocated to this cell.
  - name: modelled_engaged_sessions
    type: FLOAT64
    description: Engaged organic sessions allocated to this cell.
  - name: modelled_key_events
    type: FLOAT64
    description: Key events allocated to this cell.
  - name: modelled_demo_events
    type: FLOAT64
    description: >
      Demo or contact-sales requests allocated to this cell. The bottom-of-funnel
      measure a B2B SaaS pipeline is judged on.
  - name: modelled_signup_events
    type: FLOAT64
    description: Self-serve signups and trial starts allocated to this cell.
  - name: modelled_outcome_value_usd
    type: FLOAT64
    description: >
      Modelled key-event value plus ecommerce revenue allocated to this cell.
    checks:
      - name: non_negative
  - name: outcome_value_share
    type: FLOAT64
    description: >
      Share of the property's modelled value landing in this cell. Compare it
      against click_share: a cell taking far more clicks than value is where
      traffic is being mistaken for demand.
  - name: modelled_value_per_click_usd
    type: FLOAT64
    description: >
      Modelled value per click. The number that makes an informational click and a
      competitor comparison click directly comparable.
  - name: modelled_demo_events_per_thousand_clicks
    type: FLOAT64
    description: Demo requests per thousand clicks in this cell.

custom_checks:
  - name: intent and page role grain is unique
    description: One row per property, intent, and page role.
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT
          site_url,
          query_intent_type,
          page_role
        FROM {{ this }}
        GROUP BY 1, 2, 3
        HAVING COUNT(*) > 1
      )
    value: 0
  - name: anonymized demand is excluded
    description: >
      A withheld query carries no intent, so it must not reach an intent report.
    query: |
      SELECT COUNT(*)
      FROM {{ this }}
      WHERE query_intent_type = 'anonymized'
    value: 0
  - name: click shares sum to one per property
    description: >
      The cells partition the property's disclosed clicks, so their shares must add
      up.
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT site_url
        FROM {{ this }}
        GROUP BY 1
        HAVING ABS(SUM(click_share) - 1) > 0.001
      )
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
    daily.query_intent_type,
    daily.page_role,
    daily.page_hostname,
    daily.page_path,
    daily.query,
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
  GROUP BY 1, 2, 3, 4, 5, 6
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
    SUM(sessions.session_outcome_value_usd) AS outcome_value_usd
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
    query_page.query_intent_type,
    query_page.page_role,
    query_page.query,
    {{ page_identity('query_page.page_hostname', 'query_page.page_path') }} AS page_key,
    query_page.search_impressions,
    query_page.search_clicks,
    query_page.sum_position,
    SAFE_DIVIDE(query_page.search_clicks, NULLIF(totals.attributable_clicks, 0)) AS click_weight,
    COALESCE(outcomes.organic_sessions, 0) AS page_organic_sessions,
    COALESCE(outcomes.engaged_sessions, 0) AS page_engaged_sessions,
    COALESCE(outcomes.key_events, 0) AS page_key_events,
    COALESCE(outcomes.demo_events, 0) AS page_demo_events,
    COALESCE(outcomes.signup_events, 0) AS page_signup_events,
    COALESCE(outcomes.outcome_value_usd, 0) AS page_outcome_value_usd
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
    query_intent_type,
    page_role,
    COUNT(DISTINCT query) AS distinct_query_count,
    COUNT(DISTINCT page_key) AS distinct_page_count,
    SUM(search_impressions) AS search_impressions,
    SUM(search_clicks) AS search_clicks,
    SUM(sum_position) AS sum_position,
    SUM(COALESCE(click_weight, 0) * page_organic_sessions) AS modelled_sessions,
    SUM(COALESCE(click_weight, 0) * page_engaged_sessions) AS modelled_engaged_sessions,
    SUM(COALESCE(click_weight, 0) * page_key_events) AS modelled_key_events,
    SUM(COALESCE(click_weight, 0) * page_demo_events) AS modelled_demo_events,
    SUM(COALESCE(click_weight, 0) * page_signup_events) AS modelled_signup_events,
    SUM(COALESCE(click_weight, 0) * page_outcome_value_usd) AS modelled_outcome_value_usd
  FROM allocated
  GROUP BY 1, 2, 3
)

SELECT
  site_url,
  query_intent_type,
  page_role,
  DATE_ADD(bounds.window_start, INTERVAL 1 DAY) AS window_start_date,
  bounds.window_end AS window_end_date,
  distinct_query_count,
  distinct_page_count,
  search_impressions,
  search_clicks,
  SAFE_DIVIDE(search_clicks, NULLIF(search_impressions, 0)) AS search_ctr,
  {{ average_position('sum_position', 'search_impressions') }} AS search_avg_position,
  SAFE_DIVIDE(search_clicks, NULLIF(SUM(search_clicks) OVER (PARTITION BY site_url), 0))
    AS click_share,
  modelled_sessions,
  modelled_engaged_sessions,
  modelled_key_events,
  modelled_demo_events,
  modelled_signup_events,
  modelled_outcome_value_usd,
  SAFE_DIVIDE(
    modelled_outcome_value_usd,
    NULLIF(SUM(modelled_outcome_value_usd) OVER (PARTITION BY site_url), 0)
  ) AS outcome_value_share,
  SAFE_DIVIDE(modelled_outcome_value_usd, NULLIF(search_clicks, 0))
    AS modelled_value_per_click_usd,
  SAFE_DIVIDE(modelled_demo_events, NULLIF(search_clicks, 0)) * 1000
    AS modelled_demo_events_per_thousand_clicks
FROM rolled_up
CROSS JOIN bounds;
