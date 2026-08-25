/* @bruin
name: posthog_reports.feature_adoption_by_segment
type: bq.sql
description: >
  Feature flag exposure and adoption cross-tabbed with the account attributes
  that only exist in the warehouse. PostHog can tell you how many people saw a
  flag and whether they converted; it cannot tell you that the compact-table
  variant of the pricing redesign converts twice as well inside manufacturing
  accounts paying more than a thousand a month. One row per flag, variant, plan,
  and industry, carrying exposure, adoption against the segment's active user
  base, post-exposure conversion, and the revenue sitting behind the exposed
  accounts.

  `was_enabled` separates the people who were given the feature from the people
  the flag deliberately held out, so a boolean flag's `false` arm is a
  comparison group rather than something to add to an adoption total. Conversion
  is measured inside a bounded window after exposure -- see
  `conversion_window_days` in `pipeline.yml` -- because over an unbounded window
  almost every active person eventually does something and every rate converges
  on 1.

materialization:
  type: table

depends:
  - posthog_stage.events
  - posthog_stage.persons
  - posthog_stage.feature_flag_exposures
  - posthog_stage.accounts
  - posthog_stage.person_distinct_ids

tags:
  - posthog_reports
  - posthog
  - product_analytics
  - feature_flags
  - experimentation

columns:
  - name: flag_key
    type: STRING
    description: Feature flag key as configured in PostHog.
    primary_key: true
    checks:
      - name: not_null
  - name: flag_variant
    type: STRING
    description: >
      Variant the person was served, as PostHog reported it. Multivariate flags
      report the variant key; boolean flags report the strings `true` and
      `false`. A `false` row is a holdout group, so read it alongside
      `was_enabled` rather than as adoption. `unknown` covers evaluations
      PostHog answered without a response, which are kept so exposure totals
      stay complete but count as not enabled.
    primary_key: true
    checks:
      - name: not_null
  - name: was_enabled
    type: BOOL
    description: >
      Whether this arm was actually given the feature. False for a boolean
      flag's `false` arm and for a multivariate `control` arm. Filter on it when
      you want adoption; group by it when you want the A/B comparison.
    checks:
      - name: not_null
  - name: plan
    type: STRING
    description: >
      Plan tier of the exposed person's account, falling back to the person's
      own plan when they belong to no company. `unknown` when neither is set.
    primary_key: true
    checks:
      - name: not_null
  - name: industry
    type: STRING
    description: >
      Industry of the exposed person's account, `unknown` when unrecorded.
    primary_key: true
    checks:
      - name: not_null
  - name: flag_name
    type: STRING
    description: Human-readable flag name carried from the flag definition.
  - name: flag_is_active
    type: BOOL
    description: Whether the flag is still active in PostHog.
  - name: first_exposure_date
    type: DATE
    description: First date anyone in this segment was exposed to the variant.
  - name: last_exposure_date
    type: DATE
    description: Most recent date anyone in this segment was exposed to the variant.
  - name: segment_active_users
    type: INT64
    description: >
      Distinct persons in this plan and industry segment with at least one event
      anywhere in the warehouse. This is the denominator of `adoption_rate`, and
      it is deliberately the whole segment rather than the flag's own audience,
      so a flag rolled out to ten percent of a segment reads as ten percent.
    checks:
      - name: non_negative
  - name: exposed_users
    type: INT64
    description: Distinct persons in the segment exposed to this flag variant.
    checks:
      - name: non_negative
  - name: exposed_accounts
    type: INT64
    description: >
      Distinct companies with at least one exposed person. Persons with no
      company are counted in `exposed_users` but not here.
    checks:
      - name: non_negative
  - name: adoption_rate
    type: FLOAT64
    description: Exposed users over the segment's active user base.
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: converted_users
    type: INT64
    description: >
      Exposed persons who performed at least one product action strictly after
      their first exposure to the variant and within
      `conversion_window_days` of it. Actions before exposure do not count, so
      the measure is directional rather than merely correlated. The window
      matters as much: without it, any person who stays active long enough
      eventually converts and the rate stops discriminating between arms.
    checks:
      - name: non_negative
  - name: conversion_rate
    type: FLOAT64
    description: Converted users over exposed users.
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: upgraded_users
    type: INT64
    description: >
      Exposed persons with one of the `upgrade_events` within
      `conversion_window_days` of first exposure. This is the readout the
      experiment is really for.
    checks:
      - name: non_negative
  - name: upgrade_rate
    type: FLOAT64
    description: Upgraded users over exposed users.
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: paying_exposed_accounts
    type: INT64
    description: Exposed accounts with at least one paying person.
    checks:
      - name: non_negative
  - name: avg_mrr
    type: NUMERIC
    description: >
      Mean monthly recurring revenue across the distinct exposed accounts, not
      across exposed persons, so a large account does not weight the average by
      its headcount. Null when no exposed person belongs to a company.
  - name: total_mrr
    type: NUMERIC
    description: Summed MRR of the distinct exposed accounts in the segment.
  - name: is_reportable
    type: BOOL
    description: >
      Whether the segment has at least `min_segment_users` exposed people. The
      flag × variant × plan × industry grain produces plenty of cells holding
      one or two people, where a rate can only be 0% or 100% and reads as a
      finding. Filter on this before charting a rate or quoting one in a
      readout; the rows are still here so the totals stay complete.
    checks:
      - name: not_null
@bruin */

WITH person_segment AS (
  -- Segment dimensions are read from the account where one exists so that every
  -- person at a company lands in the same cell, and fall back to the person's
  -- own attributes for self-serve users with no company.
  SELECT
    persons.person_id,
    persons.company,
    COALESCE(profile.plan, persons.plan, 'unknown') AS plan,
    COALESCE(profile.industry, persons.industry, 'unknown') AS industry,
    profile.mrr AS account_mrr,
    COALESCE(profile.is_paying, persons.is_paying, FALSE) AS account_is_paying
  FROM posthog_stage.persons AS persons
  LEFT JOIN posthog_stage.accounts AS profile
    ON persons.company = profile.company
  -- `persons.is_anonymous` exists to be used, and this is the report that has
  -- to use it. The other three reach the same place incidentally by
  -- requiring a company; this one segments on plan and industry, so an
  -- unidentified visitor would otherwise land in the unknown/unknown cell and
  -- inflate `segment_active_users`, which is the adoption-rate denominator.
  WHERE NOT persons.is_anonymous
),

active_persons AS (
  SELECT DISTINCT directory.person_id
  FROM posthog_stage.events AS events
  INNER JOIN posthog_stage.person_distinct_ids AS directory
    ON events.distinct_id = directory.distinct_id
),

segment_population AS (
  SELECT
    segment.plan,
    segment.industry,
    COUNT(*) AS segment_active_users
  FROM person_segment AS segment
  INNER JOIN active_persons
    ON segment.person_id = active_persons.person_id
  GROUP BY 1, 2
),

first_exposures AS (
  -- A person can be re-exposed on every page load, so collapse to the first
  -- exposure per person and variant. That timestamp is the line conversion is
  -- measured against.
  SELECT
    person_id,
    flag_key,
    -- Labelled rather than dropped. An evaluation PostHog answered with no
    -- response cannot be attributed to an arm, but discarding it would
    -- understate exposure -- and `flag_variant` is part of the key here, so it
    -- cannot simply stay null.
    COALESCE(flag_variant, 'unknown') AS flag_variant,
    LOGICAL_OR(was_enabled) AS was_enabled,
    MIN(exposed_at) AS first_exposed_at,
    MAX(flag_name) AS flag_name,
    LOGICAL_OR(COALESCE(flag_is_active, FALSE)) AS flag_is_active
  FROM posthog_stage.feature_flag_exposures
  WHERE person_id IS NOT NULL
    AND flag_key IS NOT NULL
  GROUP BY 1, 2, 3
),

outcome_events AS (
  -- Only the events that can count as an outcome, resolved to a person.
  SELECT
    directory.person_id,
    events.event_at,
    {{ in_string_list('events.event_name', var.product_action_events) }} AS is_product_action,
    {{ in_string_list('events.event_name', var.upgrade_events) }} AS is_upgrade
  FROM posthog_stage.events AS events
  INNER JOIN posthog_stage.person_distinct_ids AS directory
    ON events.distinct_id = directory.distinct_id
  WHERE {{ in_string_list('events.event_name', var.product_action_events) }}
    OR {{ in_string_list('events.event_name', var.upgrade_events) }}
),

exposed_users AS (
  SELECT
    exposures.flag_key,
    exposures.flag_variant,
    exposures.was_enabled,
    exposures.person_id,
    segment.company,
    segment.plan,
    segment.industry,
    segment.account_mrr,
    segment.account_is_paying,
    MAX(exposures.flag_name) AS flag_name,
    LOGICAL_OR(exposures.flag_is_active) AS flag_is_active,
    MIN(exposures.first_exposed_at) AS first_exposed_at,
    LOGICAL_OR(COALESCE(outcomes.is_product_action, FALSE)) AS converted,
    LOGICAL_OR(COALESCE(outcomes.is_upgrade, FALSE)) AS upgraded
  FROM first_exposures AS exposures
  INNER JOIN person_segment AS segment
    ON exposures.person_id = segment.person_id
  -- Bounded on both sides. Strictly after exposure, so pre-existing behaviour
  -- is never credited to the flag, and within the configured window, so the
  -- measure stays a readout on the flag rather than on how long the person
  -- stayed a customer.
  LEFT JOIN outcome_events AS outcomes
    ON exposures.person_id = outcomes.person_id
    AND outcomes.event_at > exposures.first_exposed_at
    AND outcomes.event_at <= TIMESTAMP_ADD(
      exposures.first_exposed_at,
      INTERVAL {{ var.conversion_window_days }} DAY
    )
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),

segment_user_metrics AS (
  SELECT
    flag_key,
    flag_variant,
    plan,
    industry,
    LOGICAL_OR(was_enabled) AS was_enabled,
    MAX(flag_name) AS flag_name,
    LOGICAL_OR(flag_is_active) AS flag_is_active,
    DATE(MIN(first_exposed_at)) AS first_exposure_date,
    DATE(MAX(first_exposed_at)) AS last_exposure_date,
    COUNT(DISTINCT person_id) AS exposed_users,
    COUNTIF(converted) AS converted_users,
    COUNTIF(upgraded) AS upgraded_users
  FROM exposed_users
  GROUP BY 1, 2, 3, 4
),

exposed_accounts AS (
  -- Collapse to one row per exposed account first, so account-level revenue is
  -- never multiplied by the number of exposed people at that account.
  SELECT
    flag_key,
    flag_variant,
    plan,
    industry,
    company,
    MAX(account_mrr) AS account_mrr,
    LOGICAL_OR(account_is_paying) AS account_is_paying
  FROM exposed_users
  WHERE company IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5
),

segment_account_metrics AS (
  SELECT
    flag_key,
    flag_variant,
    plan,
    industry,
    COUNT(*) AS exposed_accounts,
    COUNTIF(account_is_paying) AS paying_exposed_accounts,
    AVG(account_mrr) AS avg_mrr,
    SUM(account_mrr) AS total_mrr
  FROM exposed_accounts
  GROUP BY 1, 2, 3, 4
)

SELECT
  user_metrics.flag_key,
  user_metrics.flag_variant,
  user_metrics.plan,
  user_metrics.industry,
  user_metrics.was_enabled,
  user_metrics.flag_name,
  user_metrics.flag_is_active,
  user_metrics.first_exposure_date,
  user_metrics.last_exposure_date,
  COALESCE(population.segment_active_users, 0) AS segment_active_users,
  user_metrics.exposed_users,
  COALESCE(account_metrics.exposed_accounts, 0) AS exposed_accounts,
  LEAST(
    COALESCE(
      SAFE_DIVIDE(user_metrics.exposed_users, population.segment_active_users),
      0
    ),
    1
  ) AS adoption_rate,
  user_metrics.converted_users,
  SAFE_DIVIDE(user_metrics.converted_users, user_metrics.exposed_users)
    AS conversion_rate,
  user_metrics.upgraded_users,
  SAFE_DIVIDE(user_metrics.upgraded_users, user_metrics.exposed_users)
    AS upgrade_rate,
  COALESCE(account_metrics.paying_exposed_accounts, 0)
    AS paying_exposed_accounts,
  ROUND(account_metrics.avg_mrr, 2) AS avg_mrr,
  account_metrics.total_mrr,
  user_metrics.exposed_users >= {{ var.min_segment_users }} AS is_reportable
FROM segment_user_metrics AS user_metrics
LEFT JOIN segment_population AS population
  ON user_metrics.plan = population.plan
  AND user_metrics.industry = population.industry
LEFT JOIN segment_account_metrics AS account_metrics
  ON user_metrics.flag_key = account_metrics.flag_key
  AND user_metrics.flag_variant = account_metrics.flag_variant
  AND user_metrics.plan = account_metrics.plan
  AND user_metrics.industry = account_metrics.industry;
