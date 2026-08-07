/* @bruin
name: stripe_stage.subscriptions
type: bq.sql
description: >
  Current Stripe subscription state, including trial, cancellation, and renewal
  fields. Stripe subscriptions are mutable; use the daily subscription-item
  snapshot for durable as-of reporting.

materialization:
  type: table

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
  - name: subscription_status
    type: STRING
    description: >
      Current Stripe subscription status. Only `active` and `past_due`
      subscriptions contribute to MRR.
  - name: currency
    type: STRING
    description: Three-letter ISO currency code the subscription is billed in.
  - name: is_live_mode
    type: BOOL
    description: Whether the subscription exists in Stripe live mode.
  - name: subscription_created_at
    type: TIMESTAMP
    description: When the subscription record was created in Stripe.
  - name: subscription_started_at
    type: TIMESTAMP
    description: When the subscription first started, from Stripe's start date.
  - name: current_period_started_at
    type: TIMESTAMP
    description: >
      Start of the current billing period. Recent Stripe API versions report
      periods per subscription item, so this can be null.
  - name: current_period_ends_at
    type: TIMESTAMP
    description: >
      End of the current billing period. Recent Stripe API versions report
      periods per subscription item, so this can be null.
  - name: cancel_at
    type: TIMESTAMP
    description: Scheduled cancellation time, when one is set.
  - name: cancel_at_period_end
    type: BOOL
    description: Whether the subscription cancels at the end of the current period.
  - name: canceled_at
    type: TIMESTAMP
    description: When cancellation was requested, which can precede `ended_at`.
  - name: ended_at
    type: TIMESTAMP
    description: When the subscription ended.
  - name: trial_started_at
    type: TIMESTAMP
    description: When the trial period started.
  - name: trial_ends_at
    type: TIMESTAMP
    description: When the trial period ends.
  - name: subscription_metadata
    type: JSON
    description: Full key-value metadata set on the Stripe subscription.
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
