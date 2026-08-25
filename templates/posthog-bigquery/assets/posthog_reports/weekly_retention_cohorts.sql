/* @bruin
name: posthog_reports.weekly_retention_cohorts
type: bq.sql
description: >
  Weekly retention triangle for signup cohorts, sliced by the acquisition and
  billing attributes that live on the person profile rather than in the event
  stream. Each row is one cohort week, signup source, and plan at one weekly
  offset, so the table reads as a retention curve per acquisition channel and
  can be joined straight onto CRM or billing data to ask which channels buy
  users that stay. Weeks run Monday to Sunday and the curve is capped at twelve
  weeks.

materialization:
  type: table

depends:
  - posthog_stage.events
  - posthog_stage.persons
  - posthog_stage.person_distinct_ids

tags:
  - posthog_reports
  - posthog
  - product_analytics
  - retention
  - cohorts

columns:
  - name: cohort_week
    type: DATE
    description: Monday of the week the person signed up, from `signup_date`.
    primary_key: true
    checks:
      - name: not_null
  - name: signup_source
    type: STRING
    description: >
      Acquisition source recorded on the person profile, `unknown` when unset.
    primary_key: true
    checks:
      - name: not_null
  - name: plan
    type: STRING
    description: >
      Current plan tier of the person. `persons` is a snapshot with no history,
      so this is the plan today rather than the plan at signup. Slice on
      `initial_plan` upstream instead if you need signup-time banding.
    primary_key: true
    checks:
      - name: not_null
  - name: weeks_since_signup
    type: INT64
    description: >
      Weekly offset from the cohort week, where 0 is the signup week itself.
    primary_key: true
    checks:
      - name: min
        value: 0
      - name: max
        value: 12
  - name: week_start_date
    type: DATE
    description: Monday of the week this row measures.
    checks:
      - name: not_null
  - name: is_complete_week
    type: BOOL
    description: >
      Whether the warehouse holds event data for all seven days of this week.
      This guards both ends of the event history, not just the recent end: the
      last week of a curve is usually still in progress, and a cohort that
      signed up before event collection started has early weeks the warehouse
      cannot speak to at all. Both understate retention, and the second one
      produces a curve that rises with time, which is impossible for real
      retention. Filter on this before charting.
  - name: cohort_size
    type: INT64
    description: >
      Persons in the cohort, counted from the person profile rather than from
      activity. Someone who signed up and never returned is in the denominator.
    checks:
      - name: positive
  - name: retained_users
    type: INT64
    description: >
      Persons from the cohort with at least one event during the week. Retention
      is unbounded rather than rolling, so a person who returns in week 6 after
      three silent weeks counts in week 6.
    checks:
      - name: non_negative
  - name: retention_rate
    type: FLOAT64
    description: Retained users over cohort size.
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
@bruin */

WITH data_bounds AS (
  -- Cohort curves are only extended as far as the data actually goes, so the
  -- table never carries rows of structural zeroes for weeks that have not
  -- happened yet. The first event date is tracked for the same reason at the
  -- other end: `signup_date` is a person property and can predate the event
  -- history by months, so a cohort's early weeks may fall entirely before the
  -- warehouse holds anything. Those weeks are absence of data, not churn.
  SELECT
    MIN(event_date) AS first_event_date,
    MAX(event_date) AS last_event_date,
    DATE_TRUNC(MAX(event_date), WEEK(MONDAY)) AS last_event_week
  FROM posthog_stage.events
),

cohort_members AS (
  SELECT
    person_id,
    DATE_TRUNC(signup_date, WEEK(MONDAY)) AS cohort_week,
    COALESCE(signup_source, 'unknown') AS signup_source,
    COALESCE(plan, 'unknown') AS plan
  FROM posthog_stage.persons
  WHERE signup_date IS NOT NULL
),

cohorts AS (
  SELECT
    cohort_week,
    signup_source,
    plan,
    COUNT(DISTINCT person_id) AS cohort_size
  FROM cohort_members
  GROUP BY 1, 2, 3
),

person_active_weeks AS (
  SELECT DISTINCT
    directory.person_id,
    DATE_TRUNC(events.event_date, WEEK(MONDAY)) AS active_week
  FROM posthog_stage.events AS events
  INNER JOIN posthog_stage.person_distinct_ids AS directory
    ON events.distinct_id = directory.distinct_id
),

cohort_week_grid AS (
  -- One row per cohort per weekly offset, generated rather than derived from
  -- activity so that a week with nobody coming back is a zero instead of a
  -- missing row. ISOWEEK is the date part that counts Monday boundaries, which
  -- is what WEEK(MONDAY) truncation produced above.
  SELECT
    cohorts.cohort_week,
    cohorts.signup_source,
    cohorts.plan,
    cohorts.cohort_size,
    weeks_since_signup,
    DATE_ADD(cohorts.cohort_week, INTERVAL weeks_since_signup WEEK)
      AS week_start_date,
    bounds.first_event_date,
    bounds.last_event_date
  FROM cohorts
  CROSS JOIN data_bounds AS bounds
  CROSS JOIN UNNEST(
    GENERATE_ARRAY(
      0,
      LEAST(
        GREATEST(
          DATE_DIFF(bounds.last_event_week, cohorts.cohort_week, ISOWEEK),
          0
        ),
        12
      )
    )
  ) AS weeks_since_signup
),

cohort_retention AS (
  SELECT
    members.cohort_week,
    members.signup_source,
    members.plan,
    DATE_DIFF(activity.active_week, members.cohort_week, ISOWEEK)
      AS weeks_since_signup,
    COUNT(DISTINCT members.person_id) AS retained_users
  FROM cohort_members AS members
  INNER JOIN person_active_weeks AS activity
    ON members.person_id = activity.person_id
  -- Activity before the signup week belongs to the person's anonymous history
  -- and is not retention.
  WHERE activity.active_week >= members.cohort_week
  GROUP BY 1, 2, 3, 4
)

SELECT
  grid.cohort_week,
  grid.signup_source,
  grid.plan,
  grid.weeks_since_signup,
  grid.week_start_date,
  -- Complete at both ends: the week finished before the last event landed, and
  -- it started no earlier than the first event the warehouse holds.
  (
    DATE_ADD(grid.week_start_date, INTERVAL 6 DAY) <= grid.last_event_date
    AND grid.week_start_date >= grid.first_event_date
  ) AS is_complete_week,
  grid.cohort_size,
  COALESCE(retention.retained_users, 0) AS retained_users,
  COALESCE(
    SAFE_DIVIDE(retention.retained_users, grid.cohort_size),
    0
  ) AS retention_rate
FROM cohort_week_grid AS grid
LEFT JOIN cohort_retention AS retention
  ON grid.cohort_week = retention.cohort_week
  AND grid.signup_source = retention.signup_source
  AND grid.plan = retention.plan
  AND grid.weeks_since_signup = retention.weeks_since_signup;
