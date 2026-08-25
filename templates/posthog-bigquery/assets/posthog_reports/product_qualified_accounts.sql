/* @bruin
name: posthog_reports.product_qualified_accounts
type: bq.sql
description: >
  Product-qualified lead scoring at the account level, one row per company as of
  the most recent day of event data. It compares the trailing 28 days against
  the 28 days before that, scores each account 0-100 on how deeply the team is
  using the product, and flags the two account motions that need a human:
  expansion, where a free or pro account already behaves like an enterprise one,
  and churn risk, where a paying account's usage is collapsing. Every company
  known to `persons` is present, including completely dormant ones, because a
  silent paying account is the most important row in the table.

materialization:
  type: table

depends:
  - posthog_stage.events
  - posthog_stage.persons
  - posthog_stage.sessions
  - posthog_stage.accounts
  - posthog_stage.person_distinct_ids

tags:
  - posthog_reports
  - posthog
  - product_analytics
  - accounts
  - pql

columns:
  - name: company
    type: STRING
    description: Account the persons belong to, carried from the person profile.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: as_of_date
    type: DATE
    description: >
      Most recent event date in the warehouse. Both scoring windows are measured
      backwards from it, so the report is a snapshot rather than a time series.
      The `_28d` column suffixes name the default window; the actual length is
      `pql_window_days`.
    checks:
      - name: not_null
  - name: plan
    type: STRING
    description: >
      Highest plan tier held by any person at the account, carried from
      `posthog_stage.accounts`, or `unknown` when nobody carries a recognized
      plan.
    checks:
      - name: accepted_values
        value:
          - free
          - pro
          - enterprise
          - unknown
  - name: industry
    type: STRING
    description: >
      Most common industry recorded across the account's persons, or `unknown`
      when none of them carry one.
  - name: seats
    type: INT64
    description: Contracted seats on the account, null or zero when untracked.
  - name: mrr
    type: NUMERIC
    description: Account-level monthly recurring revenue, current snapshot.
  - name: is_paying
    type: BOOL
    description: Whether any person at the account is marked as paying.
  - name: known_users
    type: INT64
    description: Persons on record for the account, whether or not they were active.
  - name: active_users_28d
    type: INT64
    description: Distinct persons with at least one event in the trailing 28 days.
    checks:
      - name: non_negative
  - name: active_users_prior_28d
    type: INT64
    description: The same count for the 28 days before the trailing window.
    checks:
      - name: non_negative
  - name: seat_utilization_28d
    type: FLOAT64
    description: >
      Active users over licensed seats in the trailing window, capped at 1. When
      seats are missing the count of known users is the denominator.
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: sessions_28d
    type: INT64
    description: Sessions started by the account's persons in the trailing window.
    checks:
      - name: non_negative
  - name: product_actions_28d
    type: INT64
    description: >
      Events in the trailing window that represent real product work rather than
      navigation. The `product_action_events` pipeline variable names them.
    checks:
      - name: non_negative
  - name: product_actions_prior_28d
    type: INT64
    description: The same count for the 28 days before the trailing window.
    checks:
      - name: non_negative
  - name: features_used_28d
    type: INT64
    description: >
      Distinct product action types used in the trailing window, out of those
      named by the `product_action_events` pipeline variable.
      This is the breadth half of the qualification, and the strongest single
      predictor that an account has embedded the product in its workflow. It
      cannot exceed the length of `product_action_events`, so there is no fixed
      upper bound to assert here — hardcoding one would fail the run the first
      time somebody adds a ninth event.
    checks:
      - name: non_negative
  - name: power_user_count
    type: INT64
    description: >
      Persons meeting both the `power_user_min_actions` and
      `power_user_min_active_days` thresholds inside the trailing window.
      Sustained rather than bursty usage.
    checks:
      - name: non_negative
  - name: product_action_trend_rate
    type: FLOAT64
    description: >
      Change in product actions against the prior 28 days as a signed rate, so
      -0.5 is a halving. Null when the account had no product actions in the
      prior window and a trend cannot be expressed.
  - name: pql_score
    type: FLOAT64
    description: >
      Weighted 0-100 qualification score. Feature breadth contributes 25, action
      depth per active user 25, seat activation 20, power-user share 15, and
      momentum against the prior window 15. Components are clamped to 0-1 before
      weighting so account size does not dominate the ranking.
    checks:
      - name: min
        value: 0
      - name: max
        value: 100
  - name: pql_tier
    type: STRING
    description: >
      Score banded for routing by the `pql_hot_score`, `pql_warm_score`, and
      `pql_cool_score` variables. An account with no activity at all in the
      window is always `dormant`, whatever it scores.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - hot
          - warm
          - cool
          - dormant
  - name: expansion_signal
    type: BOOL
    description: >
      A free or pro account at or above `expansion_min_score` while either
      saturating its seats or running at least five active users. These accounts are using the
      product like an enterprise customer without paying like one.

      `expansion_min_score` is a calibration knob, not a constant. It sets how
      long the sales queue is, and the right value depends entirely on how your
      score is distributed — on a test project where most accounts were genuinely active
      it flagged a quarter of them, which is a list nobody works. Check the hit
      rate before trusting it: if `COUNTIF(expansion_signal) / COUNT(*)` is
      above a few percent, raise the threshold rather than the headcount.
  - name: churn_risk_signal
    type: BOOL
    description: >
      A paying account that has gone silent in the trailing window, or that has
      lost more than half its product actions from a prior window with a
      meaningful baseline of at least ten actions.
@bruin */

WITH reporting_window AS (
  -- Everything is measured backwards from the last day of event data rather
  -- than from the wall clock, so the report stays interpretable when a load is
  -- late or the pipeline is run over a historical range.
  SELECT
    MAX(event_date) AS as_of_date,
    DATE_SUB(MAX(event_date), INTERVAL {{ var.pql_window_days - 1 }} DAY) AS current_window_start,
    DATE_SUB(MAX(event_date), INTERVAL {{ var.pql_window_days }} DAY) AS prior_window_end,
    DATE_SUB(MAX(event_date), INTERVAL {{ var.pql_window_days * 2 - 1 }} DAY) AS prior_window_start
  FROM posthog_stage.events
  -- With no events there is no window to measure against, and MAX would still
  -- return one all-null row. Left in, it would cross join every account into a
  -- scored row with a null `as_of_date`, failing that column's not-null check
  -- and reporting every paying account as a churn risk on no evidence. An empty
  -- report is the honest answer.
  HAVING MAX(event_date) IS NOT NULL
),

windowed_events AS (
  SELECT
    directory.company,
    directory.person_id,
    events.event_date,
    events.event_name,
    events.event_date >= reporting_window.current_window_start AS in_current_window,
    events.event_date BETWEEN reporting_window.prior_window_start
      AND reporting_window.prior_window_end AS in_prior_window,
    {{ in_string_list('events.event_name', var.product_action_events) }} AS is_product_action
  FROM posthog_stage.events AS events
  -- One row per distinct ID by construction, so this join attributes an event
  -- to exactly one account and cannot fan it out.
  INNER JOIN posthog_stage.person_distinct_ids AS directory
    ON events.distinct_id = directory.distinct_id
  CROSS JOIN reporting_window
  WHERE directory.company IS NOT NULL
    AND events.event_date >= reporting_window.prior_window_start
),

person_window_activity AS (
  -- Power-user status is a per-person test, so it has to be evaluated before
  -- the account rollup.
  SELECT
    company,
    person_id,
    COUNTIF(is_product_action AND in_current_window) AS product_actions_28d,
    COUNT(DISTINCT IF(in_current_window, event_date, NULL)) AS active_days_28d
  FROM windowed_events
  GROUP BY 1, 2
),

account_activity AS (
  SELECT
    company,
    COUNT(DISTINCT IF(in_current_window, person_id, NULL)) AS active_users_28d,
    COUNT(DISTINCT IF(in_prior_window, person_id, NULL)) AS active_users_prior_28d,
    COUNTIF(is_product_action AND in_current_window) AS product_actions_28d,
    COUNTIF(is_product_action AND in_prior_window) AS product_actions_prior_28d,
    COUNT(DISTINCT IF(
      is_product_action AND in_current_window,
      event_name,
      NULL
    )) AS features_used_28d
  FROM windowed_events
  GROUP BY company
),

account_power_users AS (
  SELECT
    company,
    -- Sustained usage, not a single busy afternoon. Both thresholds are set by
    -- the `power_user_min_actions` and `power_user_min_active_days` variables.
    COUNTIF(
      product_actions_28d >= {{ var.power_user_min_actions }}
      AND active_days_28d >= {{ var.power_user_min_active_days }}
    ) AS power_user_count
  FROM person_window_activity
  GROUP BY company
),

account_sessions AS (
  SELECT
    persons.company,
    COUNT(*) AS sessions_28d
  FROM posthog_stage.sessions AS sessions
  INNER JOIN posthog_stage.persons AS persons
    ON sessions.person_id = persons.person_id
  CROSS JOIN reporting_window
  WHERE persons.company IS NOT NULL
    AND DATE(sessions.session_start) >= reporting_window.current_window_start
  GROUP BY persons.company
),

account_metrics AS (
  SELECT
    profile.company,
    reporting_window.as_of_date,
    profile.plan,
    profile.industry,
    profile.seats,
    profile.mrr,
    profile.is_paying,
    profile.known_users,
    profile.licensed_seats,
    COALESCE(activity.active_users_28d, 0) AS active_users_28d,
    COALESCE(activity.active_users_prior_28d, 0) AS active_users_prior_28d,
    COALESCE(sessions.sessions_28d, 0) AS sessions_28d,
    COALESCE(activity.product_actions_28d, 0) AS product_actions_28d,
    COALESCE(activity.product_actions_prior_28d, 0) AS product_actions_prior_28d,
    COALESCE(activity.features_used_28d, 0) AS features_used_28d,
    COALESCE(power_users.power_user_count, 0) AS power_user_count
  FROM posthog_stage.accounts AS profile
  CROSS JOIN reporting_window
  -- Left joins throughout: an account with no events at all still needs a row,
  -- because a dormant paying account is exactly what this report is for.
  LEFT JOIN account_activity AS activity
    ON profile.company = activity.company
  LEFT JOIN account_power_users AS power_users
    ON profile.company = power_users.company
  LEFT JOIN account_sessions AS sessions
    ON profile.company = sessions.company
),

scored_accounts AS (
  SELECT
    *,
    LEAST(
      COALESCE(SAFE_DIVIDE(active_users_28d, licensed_seats), 0),
      1
    ) AS seat_utilization_28d,
    SAFE_DIVIDE(
      product_actions_28d - product_actions_prior_28d,
      NULLIF(product_actions_prior_28d, 0)
    ) AS product_action_trend_rate,
    -- PQL score, 0-100, five clamped components:
    --   25  breadth     distinct product action types out of those configured
    --   25  depth       product actions per active user, `depth_target_actions` = full marks
    --   20  activation  active users over licensed seats
    --   15  power users share of active users who use the product sustainedly
    --   15  momentum    trailing window against the prior one, +50% = full marks
    -- Breadth and depth carry the most weight because an account using many
    -- parts of the product many times is the pattern that actually converts and
    -- renews; seats and momentum modulate it.
    ROUND(
      100 * (
        0.25 * LEAST(SAFE_DIVIDE(features_used_28d, GREATEST(ARRAY_LENGTH({{ string_array(var.product_action_events) }}), 1)), 1)
        + 0.25 * LEAST(
          COALESCE(SAFE_DIVIDE(product_actions_28d, active_users_28d), 0)
            / {{ var.depth_target_actions }},
          1
        )
        + 0.20 * LEAST(
          COALESCE(SAFE_DIVIDE(active_users_28d, licensed_seats), 0),
          1
        )
        + 0.15 * LEAST(
          COALESCE(SAFE_DIVIDE(power_user_count, active_users_28d), 0),
          1
        )
        + 0.15 * CASE
          -- No prior baseline: a brand new account gets full momentum credit,
          -- a still-silent one gets none.
          WHEN product_actions_prior_28d = 0 AND product_actions_28d > 0 THEN 1
          WHEN product_actions_prior_28d = 0 THEN 0
          ELSE LEAST(
            SAFE_DIVIDE(product_actions_28d, product_actions_prior_28d) / 1.5,
            1
          )
        END
      ),
      1
    ) AS pql_score
  FROM account_metrics
)

SELECT
  company,
  as_of_date,
  plan,
  industry,
  seats,
  mrr,
  is_paying,
  known_users,
  active_users_28d,
  active_users_prior_28d,
  seat_utilization_28d,
  sessions_28d,
  product_actions_28d,
  product_actions_prior_28d,
  features_used_28d,
  power_user_count,
  product_action_trend_rate,
  pql_score,
  CASE
    WHEN active_users_28d = 0 THEN 'dormant'
    WHEN pql_score >= {{ var.pql_hot_score }} THEN 'hot'
    WHEN pql_score >= {{ var.pql_warm_score }} THEN 'warm'
    WHEN pql_score >= {{ var.pql_cool_score }} THEN 'cool'
    ELSE 'dormant'
  END AS pql_tier,
  -- Expansion: the account is not on enterprise, is qualified, and is either
  -- out of seats or running a real team on the product.
  plan IN ('free', 'pro')
    AND pql_score >= {{ var.expansion_min_score }}
    AND (seat_utilization_28d >= 0.8 OR active_users_28d >= 5)
    AS expansion_signal,
  -- Churn risk: revenue is attached and usage has either stopped outright or
  -- more than halved against a baseline large enough to be meaningful.
  is_paying
    AND (
      active_users_28d = 0
      OR (
        product_actions_prior_28d >= 10
        AND product_actions_28d < product_actions_prior_28d * 0.5
      )
    ) AS churn_risk_signal
FROM scored_accounts;
