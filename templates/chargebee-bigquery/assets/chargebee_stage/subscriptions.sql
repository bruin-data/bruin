/* @bruin
name: chargebee_stage.subscriptions
type: bq.sql
description: >
  Conformed Chargebee subscriptions. Unix timestamps are cast to TIMESTAMP and
  the Chargebee-computed `mrr` is kept in native-currency minor units. An
  `is_mrr_eligible` flag marks the subscriptions whose MRR counts toward the
  reporting layer; which statuses qualify is controlled by the
  `mrr_active_statuses` pipeline variable (default active + non_renewing, the
  same set Chargebee uses). Subscriptions are mutable, so this holds current
  state only; the daily MRR snapshot is what accrues history. Deleted
  subscription tombstones are retained with MRR eligibility forced off, allowing
  the snapshot to emit a zero-MRR row for a customer's final deleted subscription.

materialization:
  type: table

depends:
  - chargebee_raw.subscription

tags:
  - chargebee_stage
  - chargebee
  - billing
  - subscriptions

columns:
  - name: chargebee_subscription_id
    type: STRING
    description: Chargebee subscription identifier and natural key.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: chargebee_customer_id
    type: STRING
    description: Customer the subscription belongs to.
    checks:
      - name: not_null
  - name: source_system
    type: STRING
    description: Constant `chargebee`.
    checks:
      - name: not_null
  - name: subscription_status
    type: STRING
    description: >
      Current status, one of future, in_trial, active, non_renewing, paused,
      cancelled, transferred.
  - name: is_mrr_eligible
    type: BOOL
    description: >
      Whether this subscription's MRR counts toward reporting, per the
      `mrr_active_statuses` variable.
  - name: currency_code
    type: STRING
    description: Native currency the subscription is billed in.
  - name: billing_period
    type: INT64
    description: Number of billing-period units between invoices.
  - name: billing_period_unit
    type: STRING
    description: Unit of the billing period, one of day, week, month, year.
  - name: subscription_mrr_minor
    type: NUMERIC
    description: >
      Chargebee-computed subscription MRR in native-currency minor units. It is
      a run rate, not recognized revenue.
  - name: subscription_created_at
    type: TIMESTAMP
    description: When the subscription record was created in Chargebee.
  - name: subscription_started_at
    type: TIMESTAMP
    description: When the subscription first started.
  - name: subscription_activated_at
    type: TIMESTAMP
    description: When the subscription became active (left trial/future).
  - name: current_term_started_at
    type: TIMESTAMP
    description: Start of the current billing term.
  - name: current_term_ends_at
    type: TIMESTAMP
    description: End of the current billing term.
  - name: next_billing_at
    type: TIMESTAMP
    description: When the subscription next bills.
  - name: trial_started_at
    type: TIMESTAMP
    description: When the trial started.
  - name: trial_ends_at
    type: TIMESTAMP
    description: When the trial ends.
  - name: cancelled_at
    type: TIMESTAMP
    description: When the subscription was cancelled.
  - name: subscription_updated_at
    type: TIMESTAMP
    description: When the subscription was last modified.
@bruin */

SELECT
  id AS chargebee_subscription_id,
  customer_id AS chargebee_customer_id,
  'chargebee' AS source_system,
  status AS subscription_status,
  {{ in_string_list('status', var.mrr_active_statuses) }}
    AND NOT COALESCE(deleted, FALSE) AS is_mrr_eligible,
  currency_code,
  SAFE_CAST(billing_period AS INT64) AS billing_period,
  billing_period_unit,
  SAFE_CAST(mrr AS NUMERIC) AS subscription_mrr_minor,
  TIMESTAMP_SECONDS(SAFE_CAST(created_at AS INT64)) AS subscription_created_at,
  TIMESTAMP_SECONDS(SAFE_CAST(started_at AS INT64)) AS subscription_started_at,
  TIMESTAMP_SECONDS(SAFE_CAST(activated_at AS INT64)) AS subscription_activated_at,
  TIMESTAMP_SECONDS(SAFE_CAST(current_term_start AS INT64)) AS current_term_started_at,
  TIMESTAMP_SECONDS(SAFE_CAST(current_term_end AS INT64)) AS current_term_ends_at,
  TIMESTAMP_SECONDS(SAFE_CAST(next_billing_at AS INT64)) AS next_billing_at,
  -- Trial fields are absent from some Chargebee API schema variants. Keep the
  -- conformed columns stable; trial timing can be reconstructed from events.
  CAST(NULL AS TIMESTAMP) AS trial_started_at,
  CAST(NULL AS TIMESTAMP) AS trial_ends_at,
  TIMESTAMP_SECONDS(SAFE_CAST(cancelled_at AS INT64)) AS cancelled_at,
  TIMESTAMP_SECONDS(SAFE_CAST(updated_at AS INT64)) AS subscription_updated_at
FROM chargebee_raw.subscription;
