/* @bruin
name: posthog_reports.account_engagement_monthly
type: bq.sql
description: >
  Monthly product engagement rolled up to the account (company) level. PostHog
  is person-centric and cannot roll usage up to a company, so this report is the
  reason to land the data in the warehouse: it joins event, session, and person
  data into one row per company per month, carries the account's plan, seats,
  and MRR alongside the usage, and scores engagement so accounts can be ranked
  and trended. Attribution is account-first, so events whose `distinct_id` never
  resolved to a person, and persons with no company, are excluded.

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
  - engagement

columns:
  - name: company
    type: STRING
    description: Account the persons belong to, carried from the person profile.
    primary_key: true
    checks:
      - name: not_null
  - name: activity_month
    type: DATE
    description: First day of the month the activity is attributed to.
    primary_key: true
    checks:
      - name: not_null
  - name: plan
    type: STRING
    description: >
      Highest plan tier held by any person at the account, carried from
      `posthog_stage.accounts`, or `unknown` when nobody carries a recognized
      plan. `persons` is a current-state snapshot with no history, so this is
      today's plan repeated on every historical month rather than the plan in
      force at the time.
    checks:
      - name: accepted_values
        value:
          - free
          - pro
          - enterprise
          - unknown
  - name: industry
    type: STRING
    description: Most common industry recorded across the account's persons.
  - name: seats
    type: INT64
    description: >
      Contracted seats on the account, current snapshot. Null or zero when the
      account is on a plan that does not track seats.
  - name: mrr
    type: NUMERIC
    description: >
      Account-level monthly recurring revenue, current snapshot. It is repeated
      on every person row upstream, so it is taken as a maximum rather than
      summed, and it is not a historical revenue figure for the month.
  - name: known_users
    type: INT64
    description: Persons on record for the account, whether or not they were active.
  - name: active_users
    type: INT64
    description: Distinct persons at the account with at least one event in the month.
    checks:
      - name: non_negative
  - name: seat_utilization
    type: FLOAT64
    description: >
      Active users over licensed seats, capped at 1. When seats are missing the
      count of known users is used as the denominator instead.
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: total_sessions
    type: INT64
    description: Sessions started by the account's persons during the month.
    checks:
      - name: non_negative
  - name: total_events
    type: INT64
    description: All events emitted by the account's persons during the month.
    checks:
      - name: non_negative
  - name: product_actions
    type: INT64
    description: >
      Events that represent real product work rather than navigation. Which
      events those are is set by the `product_action_events` pipeline variable.
    checks:
      - name: non_negative
  - name: avg_session_minutes
    type: FLOAT64
    description: >
      Mean session duration in minutes. Null when the account started no
      sessions in the month.
  - name: features_used
    type: INT64
    description: >
      Distinct product action types used in the month, out of those named by the
      `product_action_events` pipeline variable. This is the breadth of the
      account's product usage.
    checks:
      - name: non_negative
  - name: active_days
    type: INT64
    description: Distinct days in the month on which the account emitted an event.
  - name: engagement_score
    type: FLOAT64
    description: >
      Weighted 0-100 engagement score. Seat activation contributes 30, action
      depth per active user 25, feature breadth 20, session depth 15, and
      day-over-day consistency 10. Every component is clamped to 0-1 before
      weighting, so the score is comparable across accounts of any size. What
      counts as full marks on the depth and session components is set by the
      `depth_target_actions` and `session_depth_target_minutes` variables.
    checks:
      - name: min
        value: 0
      - name: max
        value: 100
  - name: previous_month_engagement_score
    type: FLOAT64
    description: >
      The account's score in the immediately preceding calendar month. Null when
      the account had no activity in that month.
  - name: engagement_score_delta
    type: FLOAT64
    description: >
      Month-over-month change in the score, only computed against a contiguous
      prior month so a gap in activity is never read as a decline.
  - name: engagement_trend
    type: STRING
    description: >
      `new` for an account's first observed month, `reactivated` when the prior
      calendar month was silent, and otherwise `rising`, `stable`, or
      `declining` depending on whether the delta clears five points either way.
    checks:
      - name: accepted_values
        value:
          - new
          - reactivated
          - rising
          - stable
          - declining
@bruin */

WITH account_events AS (
  SELECT
    directory.company,
    directory.person_id,
    events.event_date,
    events.event_name,
    DATE_TRUNC(events.event_date, MONTH) AS activity_month,
    {{ in_string_list('events.event_name', var.product_action_events) }} AS is_product_action
  FROM posthog_stage.events AS events
  -- One row per distinct ID by construction, so this join attributes an event
  -- to exactly one account and cannot fan it out.
  INNER JOIN posthog_stage.person_distinct_ids AS directory
    ON events.distinct_id = directory.distinct_id
  WHERE directory.company IS NOT NULL
),

observed_month_days AS (
  -- The first and last months of the window are partial. Measuring consistency
  -- against calendar days would penalise them, so the denominator is the number
  -- of days that actually appear in the data for that month.
  SELECT
    activity_month,
    COUNT(DISTINCT event_date) AS observed_days_in_month
  FROM account_events
  GROUP BY activity_month
),

account_month_events AS (
  SELECT
    company,
    activity_month,
    COUNT(DISTINCT person_id) AS active_users,
    COUNT(*) AS total_events,
    COUNTIF(is_product_action) AS product_actions,
    COUNT(DISTINCT IF(is_product_action, event_name, NULL)) AS features_used,
    COUNT(DISTINCT event_date) AS active_days
  FROM account_events
  GROUP BY 1, 2
),

account_month_sessions AS (
  -- Sessions already carry `person_id`, so they resolve to an account without
  -- going back through the distinct_id map.
  SELECT
    persons.company,
    DATE_TRUNC(DATE(sessions.session_start), MONTH) AS activity_month,
    COUNT(*) AS total_sessions,
    SAFE_DIVIDE(AVG(sessions.duration_seconds), 60) AS avg_session_minutes
  FROM posthog_stage.sessions AS sessions
  INNER JOIN posthog_stage.persons AS persons
    ON sessions.person_id = persons.person_id
  WHERE persons.company IS NOT NULL
  GROUP BY 1, 2
),

account_month_engagement AS (
  SELECT
    monthly_events.company,
    monthly_events.activity_month,
    profile.plan,
    profile.industry,
    profile.seats,
    profile.mrr,
    profile.known_users,
    monthly_events.active_users,
    LEAST(
      COALESCE(
        SAFE_DIVIDE(monthly_events.active_users, profile.licensed_seats),
        0
      ),
      1
    ) AS seat_utilization,
    COALESCE(monthly_sessions.total_sessions, 0) AS total_sessions,
    monthly_events.total_events,
    monthly_events.product_actions,
    monthly_sessions.avg_session_minutes,
    monthly_events.features_used,
    monthly_events.active_days,
    -- Engagement score, 0-100. Each component is a ratio clamped to 0-1 so the
    -- score compares a two-seat startup with a 400-seat enterprise fairly:
    --   30  seat activation      active users over licensed seats
    --   25  depth                product actions per active user, `depth_target_actions` = full marks
    --   20  breadth              distinct product action types out of those configured
    --   15  session depth        mean session length, `session_depth_target_minutes` = full marks
    --   10  consistency          active days over days observed in the month
    ROUND(
      100 * (
        0.30 * LEAST(
          COALESCE(
            SAFE_DIVIDE(monthly_events.active_users, profile.licensed_seats),
            0
          ),
          1
        )
        + 0.25 * LEAST(
          COALESCE(
            SAFE_DIVIDE(
              monthly_events.product_actions,
              monthly_events.active_users
            ),
            0
          ) / {{ var.depth_target_actions }},
          1
        )
        + 0.20 * LEAST(SAFE_DIVIDE(COALESCE(monthly_events.features_used, 0), GREATEST(ARRAY_LENGTH({{ string_array(var.product_action_events) }}), 1)), 1)
        + 0.15 * LEAST(
          COALESCE(monthly_sessions.avg_session_minutes, 0)
            / {{ var.session_depth_target_minutes }},
          1
        )
        + 0.10 * LEAST(
          COALESCE(
            SAFE_DIVIDE(
              monthly_events.active_days,
              month_days.observed_days_in_month
            ),
            0
          ),
          1
        )
      ),
      1
    ) AS engagement_score
  FROM account_month_events AS monthly_events
  INNER JOIN posthog_stage.accounts AS profile
    ON monthly_events.company = profile.company
  INNER JOIN observed_month_days AS month_days
    ON monthly_events.activity_month = month_days.activity_month
  LEFT JOIN account_month_sessions AS monthly_sessions
    ON monthly_events.company = monthly_sessions.company
    AND monthly_events.activity_month = monthly_sessions.activity_month
),

account_month_trend AS (
  SELECT
    *,
    LAG(activity_month) OVER account_months AS previous_activity_month,
    LAG(engagement_score) OVER account_months AS previous_engagement_score
  FROM account_month_engagement
  WINDOW account_months AS (PARTITION BY company ORDER BY activity_month)
)

SELECT
  company,
  activity_month,
  plan,
  industry,
  seats,
  mrr,
  known_users,
  active_users,
  seat_utilization,
  total_sessions,
  total_events,
  product_actions,
  ROUND(avg_session_minutes, 2) AS avg_session_minutes,
  features_used,
  active_days,
  engagement_score,
  -- A gap in activity must not be reported as a decline, so the comparison is
  -- only made against the immediately preceding calendar month.
  IF(
    DATE_DIFF(activity_month, previous_activity_month, MONTH) = 1,
    previous_engagement_score,
    NULL
  ) AS previous_month_engagement_score,
  IF(
    DATE_DIFF(activity_month, previous_activity_month, MONTH) = 1,
    ROUND(engagement_score - previous_engagement_score, 1),
    NULL
  ) AS engagement_score_delta,
  CASE
    WHEN previous_activity_month IS NULL THEN 'new'
    WHEN DATE_DIFF(activity_month, previous_activity_month, MONTH) > 1
      THEN 'reactivated'
    WHEN engagement_score - previous_engagement_score >= 5 THEN 'rising'
    WHEN engagement_score - previous_engagement_score <= -5 THEN 'declining'
    ELSE 'stable'
  END AS engagement_trend
FROM account_month_trend;
