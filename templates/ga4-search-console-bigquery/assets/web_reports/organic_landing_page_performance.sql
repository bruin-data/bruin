/* @bruin
name: web_reports.organic_landing_page_performance
type: bq.sql
description: >
  Search Console visibility joined to what GA4 recorded after the click, per page.
  This is the join neither product will do for you. Search Console stops at the
  click and has no idea whether the visit converted; GA4 starts at the session and
  Google does not pass it the query or the impression. Linking the two is what
  turns a ranking into a number the business recognizes, and it is the only way to
  tell a page that ranks well but sells nothing from a page that converts well but
  nobody can find.

  Rows are joined on hostname and normalized page path from both sides, and the
  join is a full outer join on purpose, so pages present in only one system are
  kept rather than silently dropped. coverage_status explains each mismatch:
  'search_only' usually means missing or blocked tracking, a redirect, or bot
  filtering, while 'ga4_only' means the page draws visits from somewhere other
  than Google search. The GA4 side is restricted to Google organic sessions
  because Search Console never reports Bing or any other engine.

  The hostname is part of the grain because a property that spans hosts commonly
  serves the same path on more than one of them, and a docs or blog subdomain is
  a different page from the marketing site even when both answer /guide. Joining
  on the path alone would sum their impressions and revenue together and quietly
  dilute the value of whichever host actually earns it. The cost is that a
  property whose two systems disagree about the canonical host — GA4 recording
  a visit before a www redirect, say — reports that page twice, once as
  'search_only' and once as 'ga4_only', rather than hiding the disagreement in a
  merged row.

  Value is read from session_outcome_value_usd, which prices key events using the
  key_event_values variable and adds any real ecommerce revenue. That matters for a
  B2B SaaS site: revenue is recognized in a CRM weeks after the visit, so the GA4
  export carries no purchase amount and a revenue-only report would rank every page
  at zero. Set those weights before trusting the value columns.

  page_role separates the three jobs a B2B SaaS site's pages do. Documentation
  regularly out-ranks the marketing site and its clicks are mostly existing
  customers looking something up, so leaving support pages in the same pool as
  pricing pages makes acquisition conversion rates look far worse than they are.
  Filter to 'product' when judging acquisition.

  Session and click counts come from different measurement systems and will never
  match exactly; treat session_per_click_ratio as a health indicator, not a bug.

materialization:
  type: table
  strategy: truncate+insert

depends:
  - web_stage.gsc_url_query_daily
  - web_stage.ga4_sessions
  - web_stage.ga4_page_daily

tags:
  - web_reports
  - search_console
  - ga4
  - seo

columns:
  - name: site_url
    type: STRING
    description: >
      Search Console property. Filled from the search side, and from the property
      observed in the window for pages GA4 saw but Search Console did not.
  - name: page_hostname
    type: STRING
    description: >
      Hostname serving the page. Part of the grain so hosts sharing a path are
      never summed together.
    primary_key: true
  - name: page_path
    type: STRING
    description: Normalized page path. With the hostname, the join key between the two systems.
    primary_key: true
    checks:
      - name: not_null
  - name: page_role
    type: STRING
    description: >
      Whether the page is a 'product' page where buying decisions happen,
      'content' marketing, or 'support' documentation. Filter to 'product' when
      judging acquisition performance.
  - name: coverage_status
    type: STRING
    description: >
      'matched' when both systems agree the page gets Google search traffic,
      'search_only' when Search Console reports clicks GA4 never recorded,
      'ga4_only' when GA4 recorded organic sessions Search Console has no data for,
      and 'session_shortfall' when GA4 saw fewer than half the reported clicks.
    checks:
      - name: not_null
      - name: accepted_values
        value: ["matched", "search_only", "ga4_only", "session_shortfall"]
  - name: window_start_date
    type: DATE
    description: First date included.
  - name: window_end_date
    type: DATE
    description: Last date included.
  - name: search_impressions
    type: INT64
    description: Search Console impressions for the page.
    checks:
      - name: non_negative
  - name: search_clicks
    type: INT64
    description: Search Console clicks for the page.
    checks:
      - name: non_negative
  - name: search_ctr
    type: FLOAT64
    description: Clicks divided by impressions.
  - name: search_avg_position
    type: FLOAT64
    description: One-based average position, weighted by impressions.
  - name: organic_sessions
    type: INT64
    description: GA4 sessions that landed on this page from Google organic search.
    checks:
      - name: non_negative
  - name: session_per_click_ratio
    type: FLOAT64
    description: >
      GA4 organic sessions divided by Search Console clicks. Healthy pages sit near
      one. Well below one points at a tracking or redirect problem rather than at a
      search problem.
  - name: engaged_sessions
    type: INT64
    description: Organic sessions GA4 flagged as engaged.
    checks:
      - name: non_negative
  - name: engagement_rate
    type: FLOAT64
    description: Engaged sessions divided by organic sessions.
  - name: avg_engagement_time_seconds
    type: FLOAT64
    description: Mean engagement time per organic session, in seconds.
  - name: new_user_session_share
    type: FLOAT64
    description: Share of organic sessions that were a user's first.
  - name: key_events
    type: INT64
    description: >
      Key events in organic sessions that landed here, using the key_event_names
      pipeline variable.
    checks:
      - name: non_negative
  - name: demo_events
    type: INT64
    description: >
      Demo or contact-sales requests from organic sessions that landed here. The
      bottom-of-funnel signal a B2B SaaS pipeline is measured on.
    checks:
      - name: non_negative
  - name: signup_events
    type: INT64
    description: Self-serve signups and trial starts from organic sessions landing here.
    checks:
      - name: non_negative
  - name: demo_events_per_hundred_clicks
    type: FLOAT64
    description: >
      Demo requests per hundred search clicks. The comparable measure of whether a
      page attracts buyers rather than readers.
  - name: outcome_value_usd
    type: FLOAT64
    description: >
      Modelled key-event value plus any ecommerce revenue from organic sessions
      landing here. Partly modelled: rank pages with it, do not report it as
      recognized revenue.
    checks:
      - name: non_negative
  - name: outcome_value_per_search_click_usd
    type: FLOAT64
    description: >
      Modelled value per organic click: what one more click on this page is worth.
      This is the column to rank a B2B SaaS content roadmap by.
  - name: outcome_value_per_thousand_impressions_usd
    type: FLOAT64
    description: >
      Modelled value per thousand impressions, which prices the visibility a page
      already has and shows where a ranking gain pays back most.
  - name: key_event_rate
    type: FLOAT64
    description: Key events per organic session.
  - name: key_events_per_hundred_clicks
    type: FLOAT64
    description: >
      Key events per hundred search clicks. Comparable across pages of very
      different sizes, which raw counts are not.
  - name: purchase_revenue_usd
    type: FLOAT64
    description: Purchase revenue in USD from organic sessions that landed here.
  - name: revenue_per_search_click_usd
    type: FLOAT64
    description: >
      Revenue divided by search clicks: what one organic click from this page is
      worth.
  - name: revenue_per_thousand_impressions_usd
    type: FLOAT64
    description: >
      Revenue per thousand impressions. Ranks pages by the value of the visibility
      they already have, which is where a ranking improvement pays back most.
  - name: organic_page_views
    type: INT64
    description: >
      Google organic page views of this page from any session, not only sessions
      that landed on it. Search Console counts impressions for a URL however the
      visit reached it, so this is the closer comparison for pages deep in a
      journey.
    checks:
      - name: non_negative
  - name: organic_entrances
    type: INT64
    description: Google organic page views of this page that opened a session.
    checks:
      - name: non_negative

custom_checks:
  - name: landing page grain is unique
    description: >
      One row per host and page. The host belongs in the grain because a property
      spanning hosts can serve the same path on several of them.
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT
          page_hostname,
          page_path
        FROM {{ this }}
        GROUP BY 1, 2
        HAVING COUNT(*) > 1
      )
    value: 0
  - name: every row carries measurement from at least one system
    description: >
      A page with neither impressions nor sessions came from a join defect rather
      than from the data.
    query: |
      SELECT COUNT(*)
      FROM {{ this }}
      WHERE search_impressions = 0
        AND organic_sessions = 0
    value: 0
  - name: search only pages have no organic sessions
    description: The coverage label must agree with the measurements beside it.
    query: |
      SELECT COUNT(*)
      FROM {{ this }}
      WHERE coverage_status = 'search_only'
        AND organic_sessions > 0
    value: 0
@bruin */

WITH bounds AS (
  SELECT
    DATE_SUB(DATE('{{ end_date }}'), INTERVAL {{ var.reporting_window_days }} DAY) AS window_start,
    DATE('{{ end_date }}') AS window_end
),

search AS (
  SELECT
    daily.site_url,
    daily.page_hostname,
    daily.page_path,
    daily.page_role,
    SUM(daily.impressions) AS search_impressions,
    SUM(daily.clicks) AS search_clicks,
    SUM(daily.sum_position) AS sum_position
  FROM web_stage.gsc_url_query_daily AS daily
  CROSS JOIN bounds
  WHERE daily.data_date > bounds.window_start
    AND daily.data_date <= bounds.window_end
    AND daily.search_type = 'WEB'
  GROUP BY 1, 2, 3, 4
),

-- Search Console exports one property per dataset, so this resolves the property
-- for pages GA4 saw and Search Console did not. Filter the staging model first if
-- several properties share a dataset.
observed_site AS (
  SELECT MAX(site_url) AS site_url
  FROM search
),

landing AS (
  SELECT
    sessions.landing_page_hostname AS page_hostname,
    sessions.landing_page_path AS page_path,
    MAX(sessions.landing_page_role) AS page_role,
    COUNT(*) AS organic_sessions,
    COUNTIF(sessions.is_engaged_session) AS engaged_sessions,
    COUNTIF(sessions.is_new_user_session) AS new_user_sessions,
    SUM(sessions.engagement_time_seconds) AS engagement_time_seconds,
    SUM(sessions.key_event_count) AS key_events,
    SUM(sessions.demo_event_count) AS demo_events,
    SUM(sessions.signup_event_count) AS signup_events,
    SUM(sessions.session_outcome_value_usd) AS outcome_value_usd,
    SUM(sessions.purchase_revenue_in_usd) AS purchase_revenue_usd
  FROM web_stage.ga4_sessions AS sessions
  CROSS JOIN bounds
  WHERE sessions.session_date > bounds.window_start
    AND sessions.session_date <= bounds.window_end
    AND sessions.is_google_organic_session
    AND sessions.landing_page_path IS NOT NULL
  GROUP BY 1, 2
),

content AS (
  SELECT
    pages.page_hostname,
    pages.page_path,
    SUM(pages.page_views) AS organic_page_views,
    SUM(pages.entrances) AS organic_entrances
  FROM web_stage.ga4_page_daily AS pages
  CROSS JOIN bounds
  WHERE pages.page_date > bounds.window_start
    AND pages.page_date <= bounds.window_end
    AND pages.is_google_organic
  GROUP BY 1, 2
),

combined AS (
  SELECT
    COALESCE(search.page_hostname, landing.page_hostname) AS page_hostname,
    COALESCE(search.page_path, landing.page_path) AS page_path,
    COALESCE(search.page_role, landing.page_role, 'unknown') AS page_role,
    search.site_url,
    COALESCE(search.search_impressions, 0) AS search_impressions,
    COALESCE(search.search_clicks, 0) AS search_clicks,
    search.sum_position,
    COALESCE(landing.organic_sessions, 0) AS organic_sessions,
    COALESCE(landing.engaged_sessions, 0) AS engaged_sessions,
    COALESCE(landing.new_user_sessions, 0) AS new_user_sessions,
    landing.engagement_time_seconds,
    COALESCE(landing.key_events, 0) AS key_events,
    COALESCE(landing.demo_events, 0) AS demo_events,
    COALESCE(landing.signup_events, 0) AS signup_events,
    COALESCE(landing.outcome_value_usd, 0) AS outcome_value_usd,
    COALESCE(landing.purchase_revenue_usd, 0) AS purchase_revenue_usd
  FROM search
  FULL OUTER JOIN landing
    ON landing.page_hostname = search.page_hostname
    AND landing.page_path = search.page_path
)

SELECT
  COALESCE(combined.site_url, observed_site.site_url) AS site_url,
  combined.page_hostname,
  combined.page_path,
  combined.page_role,
  CASE
    WHEN combined.search_impressions = 0 THEN 'ga4_only'
    WHEN combined.organic_sessions = 0 THEN 'search_only'
    WHEN COALESCE(
        SAFE_DIVIDE(combined.organic_sessions, NULLIF(combined.search_clicks, 0)),
        1
      ) < 0.5 THEN 'session_shortfall'
    ELSE 'matched'
  END AS coverage_status,
  DATE_ADD(bounds.window_start, INTERVAL 1 DAY) AS window_start_date,
  bounds.window_end AS window_end_date,
  combined.search_impressions,
  combined.search_clicks,
  SAFE_DIVIDE(combined.search_clicks, NULLIF(combined.search_impressions, 0)) AS search_ctr,
  {{ average_position('combined.sum_position', 'combined.search_impressions') }}
    AS search_avg_position,
  combined.organic_sessions,
  SAFE_DIVIDE(combined.organic_sessions, NULLIF(combined.search_clicks, 0))
    AS session_per_click_ratio,
  combined.engaged_sessions,
  SAFE_DIVIDE(combined.engaged_sessions, NULLIF(combined.organic_sessions, 0)) AS engagement_rate,
  SAFE_DIVIDE(combined.engagement_time_seconds, NULLIF(combined.organic_sessions, 0))
    AS avg_engagement_time_seconds,
  SAFE_DIVIDE(combined.new_user_sessions, NULLIF(combined.organic_sessions, 0))
    AS new_user_session_share,
  combined.key_events,
  SAFE_DIVIDE(combined.key_events, NULLIF(combined.organic_sessions, 0)) AS key_event_rate,
  SAFE_DIVIDE(combined.key_events, NULLIF(combined.search_clicks, 0)) * 100
    AS key_events_per_hundred_clicks,
  combined.demo_events,
  combined.signup_events,
  SAFE_DIVIDE(combined.demo_events, NULLIF(combined.search_clicks, 0)) * 100
    AS demo_events_per_hundred_clicks,
  combined.outcome_value_usd,
  SAFE_DIVIDE(combined.outcome_value_usd, NULLIF(combined.search_clicks, 0))
    AS outcome_value_per_search_click_usd,
  SAFE_DIVIDE(combined.outcome_value_usd, NULLIF(combined.search_impressions, 0)) * 1000
    AS outcome_value_per_thousand_impressions_usd,
  combined.purchase_revenue_usd,
  SAFE_DIVIDE(combined.purchase_revenue_usd, NULLIF(combined.search_clicks, 0))
    AS revenue_per_search_click_usd,
  SAFE_DIVIDE(combined.purchase_revenue_usd, NULLIF(combined.search_impressions, 0)) * 1000
    AS revenue_per_thousand_impressions_usd,
  COALESCE(content.organic_page_views, 0) AS organic_page_views,
  COALESCE(content.organic_entrances, 0) AS organic_entrances
FROM combined
CROSS JOIN bounds
CROSS JOIN observed_site
LEFT JOIN content
  ON content.page_hostname = combined.page_hostname
  AND content.page_path = combined.page_path
WHERE combined.search_impressions >= {{ var.min_page_impressions }}
  OR combined.organic_sessions > 0;
