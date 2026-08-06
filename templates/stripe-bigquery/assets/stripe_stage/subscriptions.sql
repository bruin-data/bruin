/* @bruin
name: stripe_stage.subscriptions
type: bq.sql
description: >
  Current Stripe subscription state, including trial, cancellation, and renewal
  fields. Stripe subscriptions are mutable; use the daily subscription-item
  snapshot for durable as-of reporting.

materialization:
  type: table
  strategy: truncate+insert

depends:
  - stripe_raw.subscription

tags:
  - stripe_stage
  - stripe
  - billing
  - subscriptions

columns:
  - name: stripe_subscription_id
    type: STRING
    description: Stripe subscription identifier and natural key.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: stripe_customer_id
    type: STRING
    description: Stripe billing customer associated with the subscription.
    checks:
      - name: not_null
@bruin */

SELECT
  id AS stripe_subscription_id,
  customer AS stripe_customer_id,
  status AS subscription_status,
  currency,
  livemode AS is_live_mode,
  TIMESTAMP_SECONDS(SAFE_CAST(created AS INT64)) AS subscription_created_at,
  TIMESTAMP_SECONDS(SAFE_CAST(start_date AS INT64)) AS subscription_started_at,
  TIMESTAMP_SECONDS(
    SAFE_CAST(current_period_start AS INT64)
  ) AS current_period_started_at,
  TIMESTAMP_SECONDS(
    SAFE_CAST(current_period_end AS INT64)
  ) AS current_period_ends_at,
  TIMESTAMP_SECONDS(SAFE_CAST(cancel_at AS INT64)) AS cancel_at,
  cancel_at_period_end AS cancel_at_period_end,
  TIMESTAMP_SECONDS(SAFE_CAST(canceled_at AS INT64)) AS canceled_at,
  TIMESTAMP_SECONDS(SAFE_CAST(ended_at AS INT64)) AS ended_at,
  TIMESTAMP_SECONDS(SAFE_CAST(trial_start AS INT64)) AS trial_started_at,
  TIMESTAMP_SECONDS(SAFE_CAST(trial_end AS INT64)) AS trial_ends_at,
  metadata AS subscription_metadata
FROM stripe_raw.subscription;
