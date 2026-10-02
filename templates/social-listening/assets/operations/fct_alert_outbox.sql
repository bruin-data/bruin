/* @bruin
name: operations.fct_alert_outbox
description: |
  The alert outbox: one row per routing decision with its current delivery
  state, derived from the insert-only decision log and the attempt log.

  status:
    pending                 routed, not attempted yet
    dry_run                 recorded in dry-run mode; nothing was sent
    retrying                a live attempt failed; will retry next run
    delivered               delivered once; never sent again
    failed_permanent        max_delivery_attempts reached without success
    expired                 content older than alert_max_age_hours; not sent
    skipped_no_destination  notification_destination = none
tags: [enrich, routing, delivery]
depends:
  - operations.fct_alert_decision
  - operations.alert_delivery_attempt
materialization:
  type: table
columns:
  - name: alert_key
    type: varchar
    primary_key: true
    checks:
      - name: not_null
      - name: unique
      - name: relationships
    foreign_key:
      table: operations.fct_alert_decision
      column: alert_key
  - name: content_id
    type: varchar
    checks:
      - name: not_null
  - name: status
    type: varchar
    checks:
      - name: accepted_values
        value: [pending, dry_run, retrying, delivered, failed_permanent, expired, skipped_no_destination]
  - name: attempt_count
    type: integer
    checks:
      - name: non_negative
  - name: retries
    type: integer
    checks:
      - name: non_negative
@bruin */

WITH attempts AS (
    SELECT *
    FROM operations.alert_delivery_attempt
    WHERE record_kind = 'attempt'
),

rolled AS (
    SELECT
        alert_key,
        SUM(CASE WHEN status IN ('delivered', 'failed') THEN 1 ELSE 0 END) AS attempt_count,
        MAX(CASE WHEN status = 'delivered' THEN attempted_at END) AS delivered_at,
        MAX(CASE WHEN status = 'dry_run' THEN 1 ELSE 0 END) AS has_dry_run,
        MAX(CASE WHEN status = 'expired' THEN 1 ELSE 0 END) AS is_expired,
        MAX(CASE WHEN status = 'skipped_no_destination' THEN 1 ELSE 0 END) AS is_skipped,
        MAX(attempted_at) AS last_attempt_at
    FROM attempts
    GROUP BY alert_key
),

last_error AS (
    SELECT alert_key, error_message AS last_error, http_status AS last_http_status
    FROM (
        SELECT
            alert_key,
            error_message,
            http_status,
            ROW_NUMBER() OVER (PARTITION BY alert_key ORDER BY attempted_at DESC, attempt_number DESC) AS rn
        FROM attempts
        WHERE status = 'failed'
    ) ranked
    WHERE rn = 1
)

SELECT
    d.alert_key,
    d.content_id,
    d.assessment_id,
    d.destination,
    d.payload_version,
    d.routing_policy_version,
    d.priority,
    d.assessment_status,
    d.model_generated,
    d.payload_json,
    d.routed_at,
    CASE
        WHEN r.delivered_at IS NOT NULL THEN 'delivered'
        WHEN r.is_expired = 1 THEN 'expired'
        WHEN r.is_skipped = 1 THEN 'skipped_no_destination'
        WHEN COALESCE(r.attempt_count, 0) >= {{ var.max_delivery_attempts }} THEN 'failed_permanent'
        WHEN COALESCE(r.attempt_count, 0) > 0 THEN 'retrying'
        WHEN r.has_dry_run = 1 THEN 'dry_run'
        ELSE 'pending'
    END AS status,
    CAST(COALESCE(r.attempt_count, 0) AS INTEGER) AS attempt_count,
    CAST(GREATEST(COALESCE(r.attempt_count, 0) - 1, 0) AS INTEGER) AS retries,
    r.last_attempt_at,
    r.delivered_at,
    e.last_error,
    e.last_http_status,
    CASE WHEN r.delivered_at IS NOT NULL THEN {{ sl_minutes_between("d.routed_at", "r.delivered_at") }} END AS delivery_lag_minutes
FROM operations.fct_alert_decision d
LEFT JOIN rolled r ON r.alert_key = d.alert_key
LEFT JOIN last_error e ON e.alert_key = d.alert_key
