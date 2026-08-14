/* @bruin
name: stripe_stage.customer_currency_daily_mrr_snapshot
type: bq.sql
description: >
  Daily MRR observation history at Stripe customer and native-currency grain,
  and the snapshot every monthly report reads. Stripe subscription records are
  mutable and overwrite their own past, so a repriced or cancelled subscription
  reports only its current state and prior daily MRR cannot be recovered from
  Stripe; this asset records that state while it is still current. Each run
  observes the pipeline end date and adds that day, replacing only the rows for
  the date being run, so re-running a date is idempotent. History accrues from
  the first run onward and cannot be backfilled, and the movement and retention
  reports need two contiguous monthly observations before they return anything.
  It starts from subscriptions and left joins subscription items, retaining a
  zero-MRR row when a cancelled or itemless subscription has no MRR-eligible
  items, so a churned customer stays visible at zero instead of disappearing
  from the snapshot and becoming indistinguishable from missing data.

materialization:
  type: table
  strategy: delete+insert
  incremental_key: snapshot_date
  partition_by: snapshot_date

depends:
  - stripe_stage.subscriptions
  - stripe_stage.subscription_items

tags:
  - stripe_stage
  - stripe
  - billing
  - snapshot
  - mrr

custom_checks:
  - name: snapshot keys are unique
    description: >
      Each daily reporting snapshot must contain at most one row per snapshot
      date, Stripe customer, and native currency.
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT
          snapshot_date,
          stripe_customer_id,
          currency
        FROM {{ this }}
        GROUP BY 1, 2, 3
        HAVING COUNT(*) > 1
      )
    value: 0
  - name: current customer currencies are covered by this snapshot
    description: >
      Every current subscription customer/currency pair must be represented
      for the pipeline end date. An empty Stripe account passes this check.
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT DISTINCT
          stripe_customer_id,
          currency
        FROM {{ schema_prefix }}stripe_stage.subscriptions
        WHERE stripe_customer_id IS NOT NULL
          AND currency IS NOT NULL
      ) AS source
      LEFT JOIN {{ this }} AS snapshot
        ON snapshot.snapshot_date = DATE('{{ end_date }}')
        AND snapshot.stripe_customer_id = source.stripe_customer_id
        AND snapshot.currency = source.currency
      WHERE snapshot.stripe_customer_id IS NULL
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
  - name: stripe_customer_id
    type: STRING
    description: Stripe billing customer identifier.
    primary_key: true
    checks:
      - name: not_null
  - name: currency
    type: STRING
    description: Native Stripe currency.
    primary_key: true
    checks:
      - name: not_null
  - name: active_subscription_count
    type: INT64
    description: Count of subscriptions with at least one MRR-eligible item.
  - name: active_subscription_item_count
    type: INT64
    description: Count of MRR-eligible subscription items.
  - name: ending_mrr_minor
    type: NUMERIC
    description: Ending gross list-price MRR in native-currency minor units.
  - name: is_live_mode
    type: BOOL
    description: Stripe live-mode flag.
@bruin */

SELECT
  DATE('{{ end_date }}') AS snapshot_date,
  subscription.stripe_customer_id,
  subscription.currency,
  COUNT(DISTINCT IF(
    COALESCE(item.is_mrr_eligible, FALSE),
    subscription.stripe_subscription_id,
    NULL
  )) AS active_subscription_count,
  COUNTIF(COALESCE(item.is_mrr_eligible, FALSE)) AS active_subscription_item_count,
  COALESCE(
    SUM(IF(
      COALESCE(item.is_mrr_eligible, FALSE),
      item.gross_mrr_minor,
      CAST(0 AS NUMERIC)
    )),
    CAST(0 AS NUMERIC)
  ) AS ending_mrr_minor,
  LOGICAL_OR(COALESCE(subscription.is_live_mode, FALSE)) AS is_live_mode
FROM stripe_stage.subscriptions AS subscription
LEFT JOIN stripe_stage.subscription_items AS item
  ON subscription.stripe_subscription_id = item.stripe_subscription_id
WHERE subscription.stripe_customer_id IS NOT NULL
  AND subscription.currency IS NOT NULL
GROUP BY 1, 2, 3;
