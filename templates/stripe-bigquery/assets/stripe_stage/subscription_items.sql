/* @bruin
name: stripe_stage.subscription_items
type: bq.sql
description: >
  Current subscription-item state joined to prices and products. gross_mrr_minor
  is normalized from active or past-due monthly and annual licensed recurring
  prices only. price_based_recurring_mrr_minor normalizes the same price shape
  regardless of subscription status for operational risk and trial-conversion
  reporting, with each currency reported independently. This model holds current
  state only, because Stripe overwrites its own history; use the daily
  subscription-item snapshot for as-of reporting over time.

materialization:
  type: table

depends:
  - stripe_raw.subscription_item
  - stripe_stage.subscriptions
  - stripe_stage.prices

tags:
  - stripe_stage
  - stripe
  - billing
  - subscription-items

columns:
  - name: stripe_subscription_item_id
    type: STRING
    description: Stripe subscription-item identifier and natural key.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: stripe_subscription_id
    type: STRING
    description: Stripe subscription that owns the item.
    checks:
      - name: not_null
  - name: stripe_customer_id
    type: STRING
    description: Stripe billing customer that owns the subscription.
  - name: subscription_status
    type: STRING
    description: Status of the owning subscription at the time of the run.
  - name: cancel_at_period_end
    type: BOOL
    description: Whether the subscription cancels at the end of the current period.
  - name: cancel_at
    type: TIMESTAMP
    description: Scheduled cancellation time of the owning subscription.
  - name: trial_started_at
    type: TIMESTAMP
    description: Trial start time of the owning subscription.
  - name: trial_ends_at
    type: TIMESTAMP
    description: Trial end time of the owning subscription.
  - name: subscription_item_created_at
    type: TIMESTAMP
    description: When the subscription item was created in Stripe.
  - name: current_period_started_at
    type: TIMESTAMP
    description: >
      Start of the item's current billing period, falling back to the
      subscription-level period when Stripe reports it there.
  - name: current_period_ends_at
    type: TIMESTAMP
    description: >
      End of the item's current billing period, falling back to the
      subscription-level period when Stripe reports it there.
  - name: stripe_price_id
    type: STRING
    description: Stripe price attached to the item.
  - name: stripe_product_id
    type: STRING
    description: Stripe product resolved through the price.
  - name: product_name
    type: STRING
    description: Product name resolved through the price.
  - name: currency
    type: STRING
    description: Native Stripe currency of the price.
  - name: recurring_interval
    type: STRING
    description: Billing interval of the price, for example `month` or `year`.
  - name: recurring_interval_count
    type: INT64
    description: Number of intervals in one billing period of the price.
  - name: recurring_usage_type
    type: STRING
    description: Usage type of the price, either `licensed` or `metered`.
  - name: unit_amount_minor
    type: NUMERIC
    description: Price unit amount in native-currency minor units.
  - name: quantity
    type: INT64
    description: Item quantity, defaulted to 1 when Stripe omits it.
  - name: is_recurring
    type: BOOL
    description: Whether the attached price is recurring.
  - name: is_mrr_eligible
    type: BOOL
    description: >
      Whether the item contributes to gross list-price MRR. It requires an
      active or past-due subscription and a licensed monthly or annual price
      with a positive unit amount.
  - name: mrr_exclusion_reason
    type: STRING
    description: >
      Why the item is excluded from MRR, combining subscription status and price
      shape. It is null for MRR-eligible items.
  - name: gross_mrr_minor
    type: NUMERIC
    description: >
      Gross list-price MRR in native-currency minor units, normalized to a
      monthly amount. It is zero for items that are not MRR-eligible.
  - name: price_based_mrr_exclusion_reason
    type: STRING
    description: >
      Price-shape exclusion reason evaluated independently of subscription
      status, for operational risk and trial-conversion reporting.
  - name: price_based_recurring_mrr_minor
    type: NUMERIC
    description: >
      Monthly-normalized price amount in minor units regardless of subscription
      status. Use it for trial and at-risk pipeline reporting, not for MRR.
  - name: is_live_mode
    type: BOOL
    description: Whether the owning subscription exists in Stripe live mode.
  - name: subscription_item_metadata
    type: JSON
    description: Full key-value metadata set on the Stripe subscription item.
@bruin */

WITH raw_subscription_items AS (
  SELECT
    id AS stripe_subscription_item_id,
    subscription AS stripe_subscription_id,
    TIMESTAMP_SECONDS(SAFE_CAST(created AS INT64)) AS subscription_item_created_at,
    TIMESTAMP_SECONDS(
      SAFE_CAST(current_period_start AS INT64)
    ) AS current_period_started_at,
    TIMESTAMP_SECONDS(
      SAFE_CAST(current_period_end AS INT64)
    ) AS current_period_ends_at,
    COALESCE(SAFE_CAST(quantity AS INT64), 1) AS quantity,
    COALESCE(
      JSON_VALUE(price, '$.id'),
      JSON_VALUE(price, '$')
    ) AS stripe_price_id,
    metadata AS subscription_item_metadata
  FROM stripe_raw.subscription_item
),
subscription_item_context AS (
  SELECT
    item.stripe_subscription_item_id,
    item.stripe_subscription_id,
    subscription.stripe_customer_id,
    subscription.subscription_status,
    subscription.cancel_at_period_end,
    subscription.cancel_at,
    subscription.trial_started_at,
    subscription.trial_ends_at,
    item.subscription_item_created_at,
    COALESCE(
      item.current_period_started_at,
      subscription.current_period_started_at
    ) AS current_period_started_at,
    COALESCE(
      item.current_period_ends_at,
      subscription.current_period_ends_at
    ) AS current_period_ends_at,
    item.stripe_price_id,
    price.stripe_product_id,
    price.product_name,
    price.currency,
    price.recurring_interval,
    price.recurring_interval_count,
    price.recurring_usage_type,
    price.unit_amount_minor,
    item.quantity,
    price.is_recurring,
    CASE
      WHEN NOT COALESCE(price.is_recurring, FALSE)
        THEN 'non_recurring_price'
      WHEN COALESCE(price.recurring_usage_type, 'licensed') = 'metered'
        THEN 'metered_price'
      WHEN COALESCE(price.unit_amount_minor, 0) <= 0
        THEN 'zero_amount_price'
      WHEN price.recurring_interval NOT IN ('month', 'year')
        THEN 'unsupported_cadence'
      ELSE NULL
    END AS price_based_mrr_exclusion_reason,
    CASE
      WHEN COALESCE(price.is_recurring, FALSE)
        AND COALESCE(price.recurring_usage_type, 'licensed') != 'metered'
        AND COALESCE(price.unit_amount_minor, 0) > 0
        AND price.recurring_interval = 'month'
        THEN SAFE_DIVIDE(
          price.unit_amount_minor * item.quantity,
          price.recurring_interval_count
        )
      WHEN COALESCE(price.is_recurring, FALSE)
        AND COALESCE(price.recurring_usage_type, 'licensed') != 'metered'
        AND COALESCE(price.unit_amount_minor, 0) > 0
        AND price.recurring_interval = 'year'
        THEN SAFE_DIVIDE(
          price.unit_amount_minor * item.quantity,
          12 * price.recurring_interval_count
        )
      ELSE CAST(0 AS NUMERIC)
    END AS price_based_recurring_mrr_minor,
    subscription.is_live_mode,
    item.subscription_item_metadata
  FROM raw_subscription_items AS item
  INNER JOIN stripe_stage.subscriptions AS subscription
    ON item.stripe_subscription_id = subscription.stripe_subscription_id
  LEFT JOIN stripe_stage.prices AS price
    ON item.stripe_price_id = price.stripe_price_id
)

SELECT
  stripe_subscription_item_id,
  stripe_subscription_id,
  stripe_customer_id,
  subscription_status,
  cancel_at_period_end,
  cancel_at,
  trial_started_at,
  trial_ends_at,
  subscription_item_created_at,
  current_period_started_at,
  current_period_ends_at,
  stripe_price_id,
  stripe_product_id,
  product_name,
  currency,
  recurring_interval,
  recurring_interval_count,
  recurring_usage_type,
  unit_amount_minor,
  quantity,
  is_recurring,
  CASE
    WHEN subscription_status IN ('active', 'past_due')
      AND price_based_mrr_exclusion_reason IS NULL THEN TRUE
    ELSE FALSE
  END AS is_mrr_eligible,
  CASE
    WHEN subscription_status = 'trialing' THEN 'trialing_subscription'
    WHEN subscription_status NOT IN ('active', 'past_due')
      THEN 'subscription_status'
    ELSE price_based_mrr_exclusion_reason
  END AS mrr_exclusion_reason,
  CASE
    WHEN subscription_status IN ('active', 'past_due')
      AND price_based_mrr_exclusion_reason IS NULL
      THEN price_based_recurring_mrr_minor
    ELSE CAST(0 AS NUMERIC)
  END AS gross_mrr_minor,
  price_based_mrr_exclusion_reason,
  price_based_recurring_mrr_minor,
  is_live_mode,
  subscription_item_metadata
FROM subscription_item_context;
