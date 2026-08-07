/* @bruin
name: web_stage.ga4_sessions
type: bq.sql
description: >
  One row per GA4 session, rebuilt from the event export. The GA4 interface
  reports sessions after sampling and after collapsing high-cardinality
  dimensions into an "(other)" row; this model reads every event, so landing page
  and channel breakdowns stay complete no matter how many distinct pages the site
  has.

  A session is keyed on user_pseudo_id and the ga_session_id event parameter, and
  dated by the earliest event_date it contains. event_date is stamped in the
  property's reporting timezone, which is the same basis the GA4 interface uses.
  Sessions that cross midnight span two daily export tables, so the run window is
  widened by source_lookback_days and whole days are replaced on every run; a
  session split by the window edge is completed by the following run.

materialization:
  type: table
  strategy: delete+insert
  incremental_key: session_date
  partition_by: session_date
  cluster_by:
    - session_default_channel_group
    - landing_page_path

tags:
  - web_stage
  - ga4
  - sessions

columns:
  - name: session_date
    type: DATE
    description: Date of the session's first event, in the property's reporting timezone.
    primary_key: true
    checks:
      - name: not_null
  - name: user_pseudo_id
    type: STRING
    description: GA4 pseudonymous client identifier.
    primary_key: true
    checks:
      - name: not_null
  - name: ga_session_id
    type: INT64
    description: GA4 session identifier, unique only within a user_pseudo_id.
    primary_key: true
    checks:
      - name: not_null
  - name: user_id
    type: STRING
    description: Logged-in user identifier, NULL when the site does not set one.
  - name: ga_session_number
    type: INT64
    description: Ordinal of this session for the user, starting at 1.
  - name: session_start_timestamp
    type: TIMESTAMP
    description: UTC timestamp of the session's first event.
  - name: session_end_timestamp
    type: TIMESTAMP
    description: UTC timestamp of the session's last event.
  - name: landing_page_location
    type: STRING
    description: page_location of the session's first page_view event.
  - name: landing_page_path
    type: STRING
    description: >
      Normalized path of the landing page. Shared join key with
      web_stage.gsc_url_query_daily.
  - name: landing_page_hostname
    type: STRING
    description: Hostname of the landing page.
  - name: landing_page_role
    type: STRING
    description: >
      Whether the landing page is a 'product' page where buying decisions happen,
      'content' marketing, or 'support' documentation. Support traffic is mostly
      existing customers, so acquisition reporting should exclude it rather than
      let it depress conversion rates.
  - name: session_default_channel_group
    type: STRING
    description: >
      Session-scoped last non-direct channel group from
      session_traffic_source_last_click. Google populates it on every event from
      10 October 2024; earlier sessions fall back to 'Unassigned'.
  - name: session_source
    type: STRING
    description: Session-scoped last non-direct source, such as google or newsletter.
  - name: session_medium
    type: STRING
    description: Session-scoped last non-direct medium, such as organic or cpc.
  - name: session_campaign_name
    type: STRING
    description: Session-scoped last non-direct campaign name.
  - name: is_organic_search_session
    type: BOOL
    description: Whether GA4 attributed the session to the Organic Search channel group.
    checks:
      - name: not_null
  - name: is_google_organic_session
    type: BOOL
    description: >
      Whether the session is organic search from a Google surface. Search Console
      only reports Google traffic, so this is the flag to use when joining against
      it; is_organic_search_session also includes Bing, DuckDuckGo, and others.
    checks:
      - name: not_null
  - name: is_new_user_session
    type: BOOL
    description: Whether this is the user's first session, based on ga_session_number.
  - name: is_engaged_session
    type: BOOL
    description: >
      Whether GA4 flagged the session as engaged through the session_engaged event
      parameter. Taken from the export rather than re-derived so it matches the
      engagement rate shown in the interface.
    checks:
      - name: not_null
  - name: device_category
    type: STRING
    description: Device class, such as desktop, mobile, or tablet.
  - name: operating_system
    type: STRING
    description: Operating system reported by the device.
  - name: country
    type: STRING
    description: Country of the session, as resolved by GA4.
  - name: event_count
    type: INT64
    description: Events recorded in the session.
    checks:
      - name: positive
  - name: page_view_count
    type: INT64
    description: page_view events in the session.
    checks:
      - name: non_negative
  - name: engagement_time_seconds
    type: FLOAT64
    description: Summed engagement_time_msec of the session, converted to seconds.
  - name: key_event_count
    type: INT64
    description: >
      Events matching the key_event_names pipeline variable. Change that variable
      to match the key events configured on the property.
    checks:
      - name: non_negative
  - name: demo_event_count
    type: INT64
    description: >
      Events matching demo_event_names: the prospect asked to talk to sales. This
      is the bottom-of-funnel signal a B2B SaaS pipeline is measured on.
    checks:
      - name: non_negative
  - name: signup_event_count
    type: INT64
    description: >
      Events matching signup_event_names: self-serve product entry, kept apart
      from demo requests because product-led and sales-led motions convert
      differently.
    checks:
      - name: non_negative
  - name: key_event_value_usd
    type: FLOAT64
    description: >
      Modelled pipeline value of the session's key events, priced by the
      key_event_values variable. This is what makes the value reports work for a
      business whose revenue is recognized in a CRM weeks later rather than on the
      site.
    checks:
      - name: non_negative
  - name: purchase_count
    type: INT64
    description: purchase events in the session.
    checks:
      - name: non_negative
  - name: purchase_revenue
    type: FLOAT64
    description: >
      Purchase revenue in the property's reporting currency. Do not sum across
      properties that report in different currencies.
  - name: purchase_revenue_in_usd
    type: FLOAT64
    description: >
      Purchase revenue converted to USD by GA4. The reports use this column so
      revenue stays additive.
  - name: distinct_transaction_count
    type: INT64
    description: Distinct ecommerce transaction identifiers in the session.
  - name: session_outcome_value_usd
    type: FLOAT64
    description: >
      Modelled key-event value plus any real ecommerce revenue, in USD. The single
      column the reports rank by, so it works whether the site sells directly or
      hands off to sales. Partly modelled: treat it as a way to compare pages and
      queries, not as recognized revenue.
    checks:
      - name: non_negative

custom_checks:
  - name: session grain is unique
    description: A session is identified by its user and session id on its start date.
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT
          session_date,
          user_pseudo_id,
          ga_session_id
        FROM {{ this }}
        GROUP BY 1, 2, 3
        HAVING COUNT(*) > 1
      )
    value: 0
  - name: google organic sessions are a subset of organic sessions
    description: >
      Every Google organic session must also be an organic search session,
      otherwise the Search Console join would read a wider population than
      intended.
    query: |
      SELECT COUNT(*)
      FROM {{ this }}
      WHERE is_google_organic_session
        AND NOT is_organic_search_session
    value: 0
@bruin */

WITH source_events AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_day,
    event_timestamp,
    event_name,
    user_pseudo_id,
    user_id,
    (
      SELECT value.int_value
      FROM UNNEST(event_params)
      WHERE key = 'ga_session_id'
    ) AS ga_session_id,
    (
      SELECT value.int_value
      FROM UNNEST(event_params)
      WHERE key = 'ga_session_number'
    ) AS ga_session_number,
    (
      SELECT COALESCE(value.string_value, CAST(value.int_value AS STRING))
      FROM UNNEST(event_params)
      WHERE key = 'session_engaged'
    ) AS session_engaged_raw,
    (
      SELECT value.int_value
      FROM UNNEST(event_params)
      WHERE key = 'engagement_time_msec'
    ) AS engagement_time_msec,
    (
      SELECT value.string_value
      FROM UNNEST(event_params)
      WHERE key = 'page_location'
    ) AS page_location,
    ecommerce.purchase_revenue AS purchase_revenue,
    ecommerce.purchase_revenue_in_usd AS purchase_revenue_in_usd,
    ecommerce.transaction_id AS transaction_id,
    device.category AS device_category,
    device.operating_system AS operating_system,
    geo.country AS country,
    session_traffic_source_last_click.cross_channel_campaign.default_channel_group
      AS session_default_channel_group,
    session_traffic_source_last_click.manual_campaign.source AS session_source,
    session_traffic_source_last_click.manual_campaign.medium AS session_medium,
    session_traffic_source_last_click.manual_campaign.campaign_name AS session_campaign_name
  FROM `{{ var.ga4_dataset }}.events_*`
  WHERE _TABLE_SUFFIX
      BETWEEN FORMAT_DATE(
        '%Y%m%d',
        DATE_SUB(DATE('{{ start_date }}'), INTERVAL {{ var.source_lookback_days }} DAY)
      )
      AND FORMAT_DATE('%Y%m%d', DATE('{{ end_date }}'))
    -- The events_* wildcard also matches the events_intraday_ tables. Their
    -- suffix is not eight digits, so this keeps the model on finalized days only.
    AND REGEXP_CONTAINS(_TABLE_SUFFIX, r'^[0-9]{8}$')
),

sessionized AS (
  SELECT
    MIN(event_day) AS session_date,
    user_pseudo_id,
    ga_session_id,
    MAX(user_id) AS user_id,
    MAX(ga_session_number) AS ga_session_number,
    TIMESTAMP_MICROS(MIN(event_timestamp)) AS session_start_timestamp,
    TIMESTAMP_MICROS(MAX(event_timestamp)) AS session_end_timestamp,
    -- The landing page is the first page_view of the session. IGNORE NULLS is
    -- applied before the limit, so an untagged first event does not blank it out.
    ARRAY_AGG(
      IF(event_name = 'page_view', page_location, NULL)
      IGNORE NULLS
      ORDER BY event_timestamp
      LIMIT 1
    )[SAFE_OFFSET(0)] AS landing_page_location,
    -- Traffic source and channel group are session-scoped, so every event in the
    -- session carries the same value and MAX is a deterministic way to read it.
    MAX(session_default_channel_group) AS session_default_channel_group,
    MAX(session_source) AS session_source,
    MAX(session_medium) AS session_medium,
    MAX(session_campaign_name) AS session_campaign_name,
    MAX(device_category) AS device_category,
    MAX(operating_system) AS operating_system,
    MAX(country) AS country,
    COALESCE(LOGICAL_OR(session_engaged_raw = '1'), FALSE) AS is_engaged_session,
    COUNT(*) AS event_count,
    COUNTIF(event_name = 'page_view') AS page_view_count,
    SUM(engagement_time_msec) AS engagement_time_msec,
    COUNTIF({{ in_string_list('event_name', var.key_event_names) }}) AS key_event_count,
    COUNTIF({{ in_string_list('event_name', var.demo_event_names) }}) AS demo_event_count,
    COUNTIF({{ in_string_list('event_name', var.signup_event_names) }}) AS signup_event_count,
    SUM({{ key_event_value('event_name', var.key_event_values) }}) AS key_event_value_usd,
    COUNTIF(event_name = 'purchase') AS purchase_count,
    SUM(IF(event_name = 'purchase', purchase_revenue, NULL)) AS purchase_revenue,
    SUM(IF(event_name = 'purchase', purchase_revenue_in_usd, NULL)) AS purchase_revenue_in_usd,
    COUNT(DISTINCT IF(event_name = 'purchase', transaction_id, NULL))
      AS distinct_transaction_count
  FROM source_events
  WHERE user_pseudo_id IS NOT NULL
    AND ga_session_id IS NOT NULL
  GROUP BY 2, 3
),

resolved AS (
  SELECT
    sessionized.*,
    {{ page_path('landing_page_location') }} AS landing_page_path,
    {{ url_hostname('landing_page_location') }} AS landing_page_hostname
  FROM sessionized
)

SELECT
  session_date,
  user_pseudo_id,
  ga_session_id,
  user_id,
  ga_session_number,
  session_start_timestamp,
  session_end_timestamp,
  landing_page_location,
  landing_page_path,
  landing_page_hostname,
  -- Derived from the normalized path rather than the raw location, so the role
  -- rules are written against the same paths the reports join on.
  {{ page_role('landing_page_path', var.support_path_pattern, var.content_path_pattern) }}
    AS landing_page_role,
  COALESCE(session_default_channel_group, 'Unassigned') AS session_default_channel_group,
  session_source,
  session_medium,
  session_campaign_name,
  COALESCE(session_default_channel_group, 'Unassigned') = 'Organic Search'
    AS is_organic_search_session,
  COALESCE(session_default_channel_group, 'Unassigned') = 'Organic Search'
    AND COALESCE(LOWER(session_source), '') LIKE '%google%'
    AS is_google_organic_session,
  COALESCE(ga_session_number = 1, FALSE) AS is_new_user_session,
  is_engaged_session,
  device_category,
  operating_system,
  country,
  event_count,
  page_view_count,
  SAFE_DIVIDE(engagement_time_msec, 1000) AS engagement_time_seconds,
  key_event_count,
  demo_event_count,
  signup_event_count,
  key_event_value_usd,
  purchase_count,
  purchase_revenue,
  purchase_revenue_in_usd,
  distinct_transaction_count,
  key_event_value_usd + COALESCE(purchase_revenue_in_usd, 0) AS session_outcome_value_usd
FROM resolved;
