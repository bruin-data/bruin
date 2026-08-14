/* @bruin
name: stripe_stage.subscription_item_daily_snapshot
type: bq.sql
description: >
  Daily observation history of subscription-item MRR state at item grain, kept
  so a customer's MRR change can be traced to the price, plan, and quantity
  behind it. Stripe subscription records are mutable and overwrite their own
  past, so historic item state cannot be reconstructed from Stripe; this asset
  records that state while it is still current. Each run observes the pipeline
  end date and adds that day, replacing only the rows for the date being run, so
  re-running a date is idempotent. History accrues from the first run onward and
  cannot be backfilled.

materialization:
  type: table
  strategy: delete+insert
  incremental_key: snapshot_date
  partition_by: snapshot_date

depends:
  - stripe_stage.subscription_items

tags:
  - stripe_stage
  - stripe
  - billing
  - snapshot

custom_checks:
  - name: snapshot keys are unique
    description: >
      Each daily snapshot must contain at most one row per snapshot date and
      Stripe subscription item.
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT
          snapshot_date,
          stripe_subscription_item_id
        FROM {{ this }}
        GROUP BY 1, 2
        HAVING COUNT(*) > 1
      )
    value: 0
  - name: current subscription items are covered by this snapshot
    description: >
      Every current subscription item with an identifier must be represented
      in the snapshot for the pipeline end date. An empty Stripe account passes
      this check.
    query: |
      SELECT COUNT(*)
      FROM {{ schema_prefix }}stripe_stage.subscription_items AS source
      LEFT JOIN {{ this }} AS snapshot
        ON snapshot.snapshot_date = DATE('{{ end_date }}')
        AND snapshot.stripe_subscription_item_id = source.stripe_subscription_item_id
      WHERE source.stripe_subscription_item_id IS NOT NULL
        AND snapshot.stripe_subscription_item_id IS NULL
    value: 0

columns:
  - name: snapshot_date
    type: DATE
    description: >
      UTC date this row observed. It is the incremental key and the partition
      key, so a re-run replaces only its own day and leaves the rest of the
      history in place.
    primary_key: true
    checks:
      - name: not_null
  - name: stripe_subscription_item_id
    type: STRING
    description: Stripe subscription-item identifier.
    primary_key: true
    checks:
      - name: not_null
  - name: stripe_subscription_id
    type: STRING
    description: Stripe subscription identifier.
  - name: stripe_customer_id
    type: STRING
    description: Stripe billing customer identifier.
  - name: subscription_status
    type: STRING
    description: Current Stripe subscription status.
  - name: cancel_at_period_end
    type: BOOL
    description: Whether the subscription is scheduled to cancel at period end.
  - name: cancel_at
    type: TIMESTAMP
    description: Scheduled cancellation timestamp.
  - name: trial_started_at
    type: TIMESTAMP
    description: Trial start timestamp.
  - name: trial_ends_at
    type: TIMESTAMP
    description: Trial end timestamp.
  - name: current_period_started_at
    type: TIMESTAMP
    description: Current billing period start timestamp.
  - name: current_period_ends_at
    type: TIMESTAMP
    description: Current billing period end timestamp.
  - name: stripe_price_id
    type: STRING
    description: Stripe price identifier.
  - name: stripe_product_id
    type: STRING
    description: Stripe product identifier.
  - name: product_name
    type: STRING
    description: Product name.
  - name: currency
    type: STRING
    description: Native Stripe currency.
  - name: recurring_interval
    type: STRING
    description: Stripe recurring interval.
  - name: recurring_interval_count
    type: INT64
    description: Number of recurring intervals in one billing period.
  - name: recurring_usage_type
    type: STRING
    description: Stripe recurring usage type.
  - name: quantity
    type: INT64
    description: Subscription-item quantity.
  - name: is_mrr_eligible
    type: BOOL
    description: Whether the item contributes to gross list-price MRR.
  - name: mrr_exclusion_reason
    type: STRING
    description: Reason the item does not contribute to MRR.
  - name: gross_mrr_minor
    type: NUMERIC
    description: Gross list-price MRR in native-currency minor units.
  - name: is_live_mode
    type: BOOL
    description: Stripe live-mode flag.
@bruin */

SELECT
  DATE('{{ end_date }}') AS snapshot_date,
  stripe_subscription_item_id,
  stripe_subscription_id,
  stripe_customer_id,
  subscription_status,
  cancel_at_period_end,
  cancel_at,
  trial_started_at,
  trial_ends_at,
  current_period_started_at,
  current_period_ends_at,
  stripe_price_id,
  stripe_product_id,
  product_name,
  currency,
  recurring_interval,
  recurring_interval_count,
  recurring_usage_type,
  quantity,
  is_mrr_eligible,
  mrr_exclusion_reason,
  gross_mrr_minor,
  is_live_mode
FROM stripe_stage.subscription_items;
