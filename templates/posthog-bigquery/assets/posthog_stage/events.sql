/* @bruin
name: posthog_stage.events
type: bq.sql
description: >
  Conformed PostHog event stream: one row per event, with the properties that
  every downstream model needs lifted out of the `properties` JSON into typed
  columns. `posthog_raw.events` is append-loaded, so re-running a window
  duplicates rows there; this model keeps only the most recently loaded copy of
  each event id. The full `properties` payload is carried through as an escape
  hatch for event-specific keys that are not flattened here.

materialization:
  type: table
  partition_by: event_date
  cluster_by:
    - event_name
    - distinct_id

depends:
  - posthog_raw.events

tags:
  - posthog_stage
  - posthog
  - product_analytics
  - events

columns:
  - name: event_id
    type: STRING
    description: PostHog event identifier and natural key, deduplicated in this model.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: event_name
    type: STRING
    description: >
      Name of the event, for example `$pageview`, `$autocapture`, or a custom
      event such as `signed_up`.
    checks:
      - name: not_null
  - name: distinct_id
    type: STRING
    description: >
      Identifier of the user or device that triggered the event. PostHog merges
      several distinct IDs into one person over time, so resolve people through
      `posthog_stage.persons.distinct_ids` rather than treating this as a person
      key.
    checks:
      - name: not_null
  - name: event_at
    type: TIMESTAMP
    description: When the event occurred, as reported by the client.
    checks:
      - name: not_null
  - name: event_date
    type: DATE
    description: UTC date of `event_at`. The table is partitioned on this column.
    checks:
      - name: not_null
  - name: session_id
    type: STRING
    description: >
      PostHog session identifier read from `$session_id`. It is null for events
      sent outside a browser session, such as server-side or backfilled events.
  - name: current_url
    type: STRING
    description: Full URL the event was sent from, read from `$current_url`.
  - name: host
    type: STRING
    description: Host portion of the page URL, read from `$host`.
  - name: pathname
    type: STRING
    description: >
      Path portion of the page URL, read from `$pathname`. It is the grain most
      page-level reporting groups by, since it excludes host and query string.
  - name: page_title
    type: STRING
    description: Document title of the page, read from the `title` property.
  - name: browser
    type: STRING
    description: Browser reported by the PostHog client, read from `$browser`.
  - name: os
    type: STRING
    description: Operating system reported by the PostHog client, read from `$os`.
  - name: device_type
    type: STRING
    description: >
      Device class reported by the PostHog client, read from `$device_type`, for
      example `Desktop`, `Mobile`, or `Tablet`.
  - name: referrer
    type: STRING
    description: Full referrer URL of the event, read from `$referrer`.
  - name: referring_domain
    type: STRING
    description: >
      Domain the visit was referred from, read from `$referring_domain`. PostHog
      writes `$direct` when there was no referrer.
  - name: is_pageview
    type: BOOL
    description: Whether the event is a `$pageview`, derived from `event_name`.
  - name: is_autocapture
    type: BOOL
    description: Whether the event is an `$autocapture`, derived from `event_name`.
  - name: revenue
    type: NUMERIC
    description: >
      Monetary value attached to the event, read from PostHog's `$revenue`
      property on any event, and from the `amount` property on the events named
      by the `revenue_events` pipeline variable. `amount` is deliberately not
      read elsewhere, because plenty of unrelated events carry an `amount` that
      is not money. It is null on events that carry no value, and it is stated
      in the currency of the `currency` property rather than being converted.
  - name: active_feature_flags
    type: ARRAY<STRING>
    description: >
      Feature flag keys that were active for the person when the event fired,
      read from `$active_feature_flags`. BigQuery stores a missing array as an
      empty array, so use `ARRAY_LENGTH` rather than a null test.
  - name: properties
    type: JSON
    description: >
      Full event property payload. Use it for event-specific keys that are not
      flattened here, such as `duration_ms`, `rows_returned`, `plan`, or the
      per-flag `$feature/<flag_key>` properties.
@bruin */

WITH deduplicated_events AS (
  -- `posthog_raw.events` appends, so a re-run of an already-loaded window lands
  -- the same event id twice. Keep the copy from the latest load. `timestamp` is
  -- a tiebreaker rather than a second grain: rows from the same load carry the
  -- same `_ingestr_loaded_at`, and without it the survivor would be whichever
  -- row the scan happened to reach first, which makes the table differ between
  -- rebuilds of identical input.
  SELECT
    id,
    event,
    distinct_id,
    timestamp,
    properties
  FROM (
    SELECT
      id,
      event,
      distinct_id,
      timestamp,
      properties,
      ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY _ingestr_loaded_at DESC, timestamp DESC
      ) AS load_rank
    FROM posthog_raw.events
    WHERE id IS NOT NULL
  )
  WHERE load_rank = 1
)

SELECT
  id AS event_id,
  event AS event_name,
  distinct_id,
  timestamp AS event_at,
  DATE(timestamp) AS event_date,
  -- PostHog's own properties are `$`-prefixed, which is not a legal bare
  -- JSONPath key, so the key is escaped with double quotes.
  JSON_VALUE(properties, '$."$session_id"') AS session_id,
  JSON_VALUE(properties, '$."$current_url"') AS current_url,
  JSON_VALUE(properties, '$."$host"') AS host,
  JSON_VALUE(properties, '$."$pathname"') AS pathname,
  JSON_VALUE(properties, '$.title') AS page_title,
  JSON_VALUE(properties, '$."$browser"') AS browser,
  JSON_VALUE(properties, '$."$os"') AS os,
  JSON_VALUE(properties, '$."$device_type"') AS device_type,
  JSON_VALUE(properties, '$."$referrer"') AS referrer,
  JSON_VALUE(properties, '$."$referring_domain"') AS referring_domain,
  event = '$pageview' AS is_pageview,
  event = '$autocapture' AS is_autocapture,
  -- `$revenue` is PostHog's convention and is read on any event. `amount` is a
  -- common generic property name -- a row count, a file size, a discount -- so
  -- it is only read as money on the events named by the `revenue_events`
  -- pipeline variable, which is what keeps a `rows_exported: 40000` out of the
  -- revenue column.
  COALESCE(
    SAFE_CAST(JSON_VALUE(properties, '$."$revenue"') AS NUMERIC),
    IF(
      {{ in_string_list('event', var.revenue_events) }},
      SAFE_CAST(JSON_VALUE(properties, '$.amount') AS NUMERIC),
      NULL
    )
  ) AS revenue,
  -- JSON_VALUE_ARRAY unnests a JSON array of scalars into ARRAY<STRING>; it
  -- returns null when the key is absent, which BigQuery stores as an empty array.
  JSON_VALUE_ARRAY(properties, '$."$active_feature_flags"') AS active_feature_flags,
  properties
FROM deduplicated_events;
