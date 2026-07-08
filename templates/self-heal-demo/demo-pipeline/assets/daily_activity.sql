/* @bruin
name: fulfillment.daily_activity
description: Daily operational activity from fulfillment event traffic.
tags:
  - self-heal-demo
  - freshness-check
materialization:
  type: table
columns:
  - name: event_date
    type: DATE
    checks:
      - name: not_null
  - name: event_count
    type: INTEGER
    checks:
      - name: positive
  - name: latest_event_at
    type: TIMESTAMP
custom_checks:
  - name: latest fulfillment activity exists
    query: SELECT CASE WHEN max(event_date) = DATE '2025-01-03' THEN 1 ELSE 0 END FROM fulfillment.daily_activity
    value: 1
@bruin */

SELECT
    CAST(event_timestamp AS DATE) AS event_date,
    COUNT(*) AS event_count,
    MAX(event_timestamp) AS latest_event_at
FROM raw.fulfillment_events
WHERE event_type IN ('packed', 'shipped')
GROUP BY 1;
