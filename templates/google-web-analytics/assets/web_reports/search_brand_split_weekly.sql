/* @bruin
name: web_reports.search_brand_split_weekly
type: bq.sql
description: >
  Weekly search performance split into branded, non-branded, and anonymized
  demand. Search Console has no concept of a brand, so its headline click trend
  mixes people who already know the site with people discovering it; those two
  numbers move for completely different reasons. Non-branded clicks are the ones
  that measure SEO work, and non-branded click share is the ratio to watch after a
  brand campaign or a ranking change.

  Weeks start on Monday. The most recent week is partial until Search Console
  finishes publishing it, so read it as incomplete rather than as a drop.

materialization:
  type: table

depends:
  - web_stage.gsc_site_query_daily

tags:
  - web_reports
  - search_console
  - seo

columns:
  - name: week_start_date
    type: DATE
    description: Monday of the reporting week.
    primary_key: true
    checks:
      - name: not_null
  - name: site_url
    type: STRING
    description: Search Console property.
    primary_key: true
    checks:
      - name: not_null
  - name: search_type
    type: STRING
    description: Search surface, such as WEB or IMAGE.
    primary_key: true
  - name: device
    type: STRING
    description: Device class of the searcher.
    primary_key: true
  - name: query_brand_type
    type: STRING
    description: Whether the demand was branded, non_branded, or anonymized.
    primary_key: true
    checks:
      - name: accepted_values
        value: ["branded", "non_branded", "anonymized"]
  - name: impressions
    type: INT64
    description: Property-level impressions in the week.
    checks:
      - name: non_negative
  - name: clicks
    type: INT64
    description: Clicks in the week.
    checks:
      - name: non_negative
  - name: ctr
    type: FLOAT64
    description: Clicks divided by impressions.
  - name: avg_position
    type: FLOAT64
    description: One-based average top position, weighted by impressions.
  - name: distinct_query_count
    type: INT64
    description: Distinct non-anonymized queries behind the row.
  - name: click_share
    type: FLOAT64
    description: >
      Share of the week's clicks for this property, search type, and device that
      came from this brand type.
  - name: impression_share
    type: FLOAT64
    description: Share of the week's impressions that came from this brand type.

custom_checks:
  - name: weekly brand grain is unique
    description: One row per week, property, search type, device, and brand type.
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT
          week_start_date,
          site_url,
          search_type,
          device,
          query_brand_type
        FROM {{ this }}
        GROUP BY 1, 2, 3, 4, 5
        HAVING COUNT(*) > 1
      )
    value: 0
  - name: click shares sum to one within each week
    description: >
      The brand types partition the week's clicks, so their shares must add up.
      Weeks without clicks are excluded because their shares are undefined.
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT
          week_start_date,
          site_url,
          search_type,
          device
        FROM {{ this }}
        GROUP BY 1, 2, 3, 4
        HAVING SUM(clicks) > 0
          AND ABS(SUM(click_share) - 1) > 0.001
      )
    value: 0
@bruin */

WITH bounds AS (
  SELECT
    DATE_SUB(DATE('{{ end_date }}'), INTERVAL {{ var.reporting_window_days }} DAY) AS window_start,
    DATE('{{ end_date }}') AS window_end
),

weekly AS (
  SELECT
    DATE_TRUNC(daily.data_date, WEEK(MONDAY)) AS week_start_date,
    daily.site_url,
    daily.search_type,
    daily.device,
    daily.query_brand_type,
    SUM(daily.impressions) AS impressions,
    SUM(daily.clicks) AS clicks,
    SUM(daily.sum_top_position) AS sum_top_position,
    COUNT(DISTINCT daily.query) AS distinct_query_count
  FROM web_stage.gsc_site_query_daily AS daily
  CROSS JOIN bounds
  WHERE daily.data_date > bounds.window_start
    AND daily.data_date <= bounds.window_end
  GROUP BY 1, 2, 3, 4, 5
)

SELECT
  week_start_date,
  site_url,
  search_type,
  device,
  query_brand_type,
  impressions,
  clicks,
  SAFE_DIVIDE(clicks, NULLIF(impressions, 0)) AS ctr,
  {{ average_position('sum_top_position', 'impressions') }} AS avg_position,
  distinct_query_count,
  SAFE_DIVIDE(
    clicks,
    NULLIF(SUM(clicks) OVER (PARTITION BY week_start_date, site_url, search_type, device), 0)
  ) AS click_share,
  SAFE_DIVIDE(
    impressions,
    NULLIF(SUM(impressions) OVER (PARTITION BY week_start_date, site_url, search_type, device), 0)
  ) AS impression_share
FROM weekly;
