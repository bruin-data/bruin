/* @bruin
name: posthog_stage.sessions
type: bq.sql
description: >
  One row per PostHog session, aggregated from `posthog_stage.events` on
  `$session_id`. PostHog assigns the session ID client-side and rotates it after
  30 minutes of inactivity, so this model does not re-sessionize; it summarises
  the window PostHog already defined. Events without a session ID, such as
  server-side events, are excluded.

materialization:
  type: table

depends:
  - posthog_stage.events
  - posthog_stage.person_distinct_ids

tags:
  - posthog_stage
  - posthog
  - product_analytics
  - sessions

columns:
  - name: session_id
    type: STRING
    description: PostHog session identifier and natural key.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: person_id
    type: STRING
    description: >
      PostHog person resolved by looking the session's distinct ID up in
      `posthog_stage.persons.distinct_ids`. It is null when the distinct ID has
      not yet been merged into a person record.
  - name: distinct_id
    type: STRING
    description: >
      Distinct ID that started the session. A session belongs to one browser, so
      this is taken from the earliest event in the session.
    checks:
      - name: not_null
  - name: session_start
    type: TIMESTAMP
    description: Timestamp of the first event in the session.
    checks:
      - name: not_null
  - name: session_end
    type: TIMESTAMP
    description: Timestamp of the last event in the session.
    checks:
      - name: not_null
  - name: duration_seconds
    type: INT64
    description: >
      Seconds between the first and last event. Single-event sessions are zero,
      because there is no later event to measure against.
  - name: pageview_count
    type: INT64
    description: Number of `$pageview` events in the session.
  - name: event_count
    type: INT64
    description: Number of events of any kind in the session.
    checks:
      - name: positive
  - name: entry_pathname
    type: STRING
    description: >
      Path of the first event in the session that carried one, which is the
      landing page.
  - name: exit_pathname
    type: STRING
    description: Path of the last event in the session that carried one.
  - name: device_type
    type: STRING
    description: Device class of the session, taken from its first event that reports one.
  - name: browser
    type: STRING
    description: Browser of the session, taken from its first event that reports one.
  - name: os
    type: STRING
    description: Operating system of the session, taken from its first event that reports one.
  - name: is_bounce
    type: BOOL
    description: >
      Whether the session saw at most one page. Sessions with no pageview at all,
      such as pure `$autocapture` sessions, also count as a bounce.
  - name: converted
    type: BOOL
    description: >
      Whether the session contains one of the events named by the
      `conversion_events` pipeline variable. It marks the session in which the
      conversion happened, so it does not carry back to the sessions that led up
      to it.
  - name: conversion_events_are_sessionized
    type: BOOL
    description: >
      Whether any conversion event in the whole warehouse carries a
      `$session_id`. Conversions are commonly captured server-side, and a
      server-side event belongs to no session, so on those projects `converted`
      is FALSE on every row. Without this column there is no way to tell that
      apart from a project that genuinely had no conversions — the same all-FALSE
      column means "nobody converted" in one case and "this cannot be measured
      here" in the other. If it is FALSE everywhere, either attach session
      context to your conversion captures or attribute conversions by
      `distinct_id` and time instead of by session.
    checks:
      - name: not_null
@bruin */

WITH conversion_coverage AS (
  -- Measured across the whole event table rather than per session, because the
  -- question is whether this instrumentation can answer the question at all.
  SELECT
    COUNTIF(session_id IS NOT NULL) > 0 AS conversion_events_are_sessionized
  FROM posthog_stage.events
  WHERE {{ in_string_list('event_name', var.conversion_events) }}
),

session_events AS (
  SELECT
    session_id,
    distinct_id,
    event_name,
    event_at,
    pathname,
    device_type,
    browser,
    os,
    is_pageview
  FROM posthog_stage.events
  WHERE session_id IS NOT NULL
),

session_summary AS (
  SELECT
    session_id,
    -- First and last non-null values, ordered by event time. IGNORE NULLS
    -- keeps events that never carried the property from winning the slot.
    ARRAY_AGG(distinct_id IGNORE NULLS ORDER BY event_at LIMIT 1)[SAFE_OFFSET(0)] AS distinct_id,
    MIN(event_at) AS session_start,
    MAX(event_at) AS session_end,
    COUNTIF(is_pageview) AS pageview_count,
    COUNT(*) AS event_count,
    ARRAY_AGG(pathname IGNORE NULLS ORDER BY event_at LIMIT 1)[SAFE_OFFSET(0)] AS entry_pathname,
    ARRAY_AGG(pathname IGNORE NULLS ORDER BY event_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS exit_pathname,
    ARRAY_AGG(device_type IGNORE NULLS ORDER BY event_at LIMIT 1)[SAFE_OFFSET(0)] AS device_type,
    ARRAY_AGG(browser IGNORE NULLS ORDER BY event_at LIMIT 1)[SAFE_OFFSET(0)] AS browser,
    ARRAY_AGG(os IGNORE NULLS ORDER BY event_at LIMIT 1)[SAFE_OFFSET(0)] AS os,
    COUNTIF({{ in_string_list('event_name', var.conversion_events) }}) > 0 AS converted
  FROM session_events
  GROUP BY session_id
)

SELECT
  summary.session_id,
  person.person_id,
  summary.distinct_id,
  summary.session_start,
  summary.session_end,
  TIMESTAMP_DIFF(summary.session_end, summary.session_start, SECOND) AS duration_seconds,
  summary.pageview_count,
  summary.event_count,
  summary.entry_pathname,
  summary.exit_pathname,
  summary.device_type,
  summary.browser,
  summary.os,
  summary.pageview_count <= 1 AS is_bounce,
  summary.converted,
  COALESCE(coverage.conversion_events_are_sessionized, FALSE)
    AS conversion_events_are_sessionized
FROM session_summary AS summary
LEFT JOIN posthog_stage.person_distinct_ids AS person
  ON summary.distinct_id = person.distinct_id
CROSS JOIN conversion_coverage AS coverage;
