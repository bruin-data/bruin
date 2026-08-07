/* @bruin
name: web_stage.ga4_page_daily
type: bq.sql
description: >
  Daily page-level GA4 behaviour by channel, source, and device. Where
  web_stage.ga4_sessions describes the page a visit started on, this model
  describes every page a visit touched, which is what Search Console impressions
  are actually reported against: a URL earns impressions whether or not anyone
  ever lands on it first.

  Page paths are normalized with the same rules as the Search Console models, so
  page_path joins directly against web_stage.gsc_url_query_daily.

materialization:
  type: table
  strategy: delete+insert
  incremental_key: page_date
  partition_by: page_date
  cluster_by:
    - page_path
    - session_default_channel_group

tags:
  - web_stage
  - ga4
  - content

columns:
  - name: page_date
    type: DATE
    description: Event date, in the property's reporting timezone.
    primary_key: true
    checks:
      - name: not_null
  - name: page_path
    type: STRING
    description: >
      Normalized path of the viewed page. Shared join key with
      web_stage.gsc_url_query_daily.
    primary_key: true
    checks:
      - name: not_null
  - name: page_hostname
    type: STRING
    description: >
      Hostname of the viewed page. Part of the grain because two hosts on the same
      property can serve the same path.
    primary_key: true
  - name: page_role
    type: STRING
    description: >
      Whether the page is a 'product' page, 'content' marketing, or 'support'
      documentation. Support pages draw mostly existing customers, so acquisition
      reporting should exclude them.
    checks:
      - name: not_null
  - name: session_default_channel_group
    type: STRING
    description: Session-scoped last non-direct channel group of the viewing session.
    primary_key: true
    checks:
      - name: not_null
  - name: session_source
    type: STRING
    description: Session-scoped last non-direct source of the viewing session.
    primary_key: true
  - name: device_category
    type: STRING
    description: Device class, such as desktop, mobile, or tablet.
    primary_key: true
  - name: is_google_organic
    type: BOOL
    description: >
      Whether the views came from Google organic search, matching the flag on
      web_stage.ga4_sessions used for Search Console joins.
    checks:
      - name: not_null
  - name: page_views
    type: INT64
    description: page_view events recorded for this page.
    checks:
      - name: positive
  - name: entrances
    type: INT64
    description: >
      page_view events that opened a session, derived from the entrances event
      parameter.
    checks:
      - name: non_negative
  - name: distinct_sessions
    type: INT64
    description: Sessions that viewed this page, the equivalent of unique page views.
    checks:
      - name: positive
  - name: distinct_users
    type: INT64
    description: Distinct user_pseudo_id values that viewed this page.
    checks:
      - name: positive
  - name: engagement_time_seconds
    type: FLOAT64
    description: Summed engagement_time_msec on this page, converted to seconds.

custom_checks:
  - name: daily page grain is unique
    description: >
      Each combination of date, host, page, channel, source, and device must
      appear once, otherwise joins against Search Console would multiply
      impressions.
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT
          page_date,
          page_hostname,
          page_path,
          session_default_channel_group,
          session_source,
          device_category
        FROM {{ this }}
        GROUP BY 1, 2, 3, 4, 5, 6
        HAVING COUNT(*) > 1
      )
    value: 0
  - name: entrances never exceed page views
    description: An entrance is a page view that happened to open a session.
    query: |
      SELECT COUNT(*)
      FROM {{ this }}
      WHERE entrances > page_views
    value: 0
@bruin */

WITH page_view_events AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS page_date,
    user_pseudo_id,
    (
      SELECT value.int_value
      FROM UNNEST(event_params)
      WHERE key = 'ga_session_id'
    ) AS ga_session_id,
    (
      SELECT value.string_value
      FROM UNNEST(event_params)
      WHERE key = 'page_location'
    ) AS page_location,
    (
      SELECT value.int_value
      FROM UNNEST(event_params)
      WHERE key = 'entrances'
    ) AS entrances,
    (
      SELECT value.int_value
      FROM UNNEST(event_params)
      WHERE key = 'engagement_time_msec'
    ) AS engagement_time_msec,
    device.category AS device_category,
    COALESCE(
      session_traffic_source_last_click.cross_channel_campaign.default_channel_group,
      'Unassigned'
    ) AS session_default_channel_group,
    session_traffic_source_last_click.manual_campaign.source AS session_source
  FROM `{{ var.ga4_dataset }}.events_*`
  WHERE _TABLE_SUFFIX
      BETWEEN FORMAT_DATE(
        '%Y%m%d',
        DATE_SUB(DATE('{{ start_date }}'), INTERVAL {{ var.source_lookback_days }} DAY)
      )
      AND FORMAT_DATE('%Y%m%d', DATE('{{ end_date }}'))
    AND REGEXP_CONTAINS(_TABLE_SUFFIX, r'^[0-9]{8}$')
    AND event_name = 'page_view'
),

normalized AS (
  SELECT
    page_view_events.*,
    {{ page_path('page_location') }} AS page_path,
    {{ url_hostname('page_location') }} AS page_hostname
  FROM page_view_events
)

SELECT
  page_date,
  page_path,
  page_hostname,
  {{ page_role('page_path', var.support_path_pattern, var.content_path_pattern) }} AS page_role,
  session_default_channel_group,
  session_source,
  device_category,
  session_default_channel_group = 'Organic Search'
    AND COALESCE(LOWER(session_source), '') LIKE '%google%'
    AS is_google_organic,
  COUNT(*) AS page_views,
  COUNTIF(COALESCE(entrances, 0) = 1) AS entrances,
  COUNT(DISTINCT CONCAT(user_pseudo_id, '-', CAST(ga_session_id AS STRING))) AS distinct_sessions,
  COUNT(DISTINCT user_pseudo_id) AS distinct_users,
  SAFE_DIVIDE(SUM(engagement_time_msec), 1000) AS engagement_time_seconds
FROM normalized
WHERE page_location IS NOT NULL
  AND user_pseudo_id IS NOT NULL
  AND ga_session_id IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;
