/* @bruin
name: posthog_stage.accounts
type: bq.sql
description: >
  One row per account, rolled up from the person profiles that carry a company.
  PostHog is person-centric and has no account entity unless `$groups` was
  instrumented at capture time, so the account is derived here instead: plan is
  the highest tier anyone at the company holds, seats and MRR are account-level
  values repeated on every person row and so are taken as a maximum, and
  industry is the account's modal value.

  Every report reads accounts from here rather than repeating the rollup, so the
  definition of "what plan is this account on" is stated once.

  Note that `posthog_stage.persons` is a current-state snapshot with no history.
  These attributes are therefore today's values, and a report that carries them
  onto a historical month is stating today's plan, not the plan in force then.

materialization:
  type: table

depends:
  - posthog_stage.persons

tags:
  - posthog_stage
  - posthog
  - product_analytics
  - accounts

columns:
  - name: company
    type: STRING
    description: Account name, taken from the `company` person property.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: plan
    type: STRING
    description: >
      Highest plan tier held by any person at the account. `unknown` when nobody
      at the account carries a recognized plan, so that accounts stay reachable
      from a plan filter instead of dropping out on NULL.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - free
          - pro
          - enterprise
          - unknown
  - name: industry
    type: STRING
    description: >
      Most common industry recorded across the account's persons. `unknown` when
      none of them carry one.
    checks:
      - name: not_null
  - name: seats
    type: INT64
    description: >
      Contracted seats, current snapshot. Null or zero on plans that do not
      track seats.
  - name: mrr
    type: NUMERIC
    description: >
      Account-level monthly recurring revenue, current snapshot. Repeated on
      every person row upstream, so it is a maximum rather than a sum, and it is
      not a historical revenue figure.
  - name: is_paying
    type: BOOL
    description: >
      Whether anyone at the account is marked as paying. True beats false at the
      account grain, on the reasoning that one paid seat makes the account a
      customer.
    checks:
      - name: not_null
  - name: known_users
    type: INT64
    description: Persons on record for the account, whether or not they were ever active.
    checks:
      - name: positive
  - name: seat_coverage
    type: FLOAT64
    description: >
      Known users over contracted seats, capped at 1. This is where the gap
      between what the account bought and who PostHog has actually seen is
      reported, instead of that gap being folded silently into an engagement
      score. A low value means either the rollout has not reached most of the
      licences or your `identify` calls do not cover everyone — both are worth
      knowing, and neither is an engagement problem. Null when the account
      carries no seat count.
  - name: licensed_seats
    type: INT64
    description: >
      Denominator for seat activation, which carries 30 of the 100 engagement
      points and 20 of the 100 PQL points.

      Contracted `seats` and the people PostHog has seen are different
      populations: PostHog only knows someone after an `identify` call. An
      account that bought 400 seats and rolled out to 40 reads as 10% activated,
      so scoring against the raw contract ranks small accounts above large ones
      and inverts the plan comparison. Which population is used is set
      by the `seat_denominator` pipeline variable: `reachable` (default) bounds
      it by known users, `contracted` scores against the `seats` property.
      Accounts with no seat count fall back to `known_users` either way.
    checks:
      - name: positive
@bruin */

SELECT
  company,
  -- Plan is ordinal, so the rollup is "highest tier anyone holds" rather than a
  -- modal or arbitrary pick: one enterprise seat makes the account enterprise.
  -- The ELSE arm matters — without it an account where nobody carries a
  -- recognized plan gets NULL, which passes the accepted_values check silently
  -- (NULL NOT IN (...) is NULL, never TRUE) and then disappears from every
  -- dashboard plan filter.
  COALESCE(
    CASE
      MAX(
        CASE plan
          WHEN 'enterprise' THEN 3
          WHEN 'pro' THEN 2
          WHEN 'free' THEN 1
          ELSE 0
        END
      )
      WHEN 3 THEN 'enterprise'
      WHEN 2 THEN 'pro'
      WHEN 1 THEN 'free'
    END,
    'unknown'
  ) AS plan,
  COALESCE(APPROX_TOP_COUNT(industry, 1)[OFFSET(0)].value, 'unknown') AS industry,
  MAX(seats) AS seats,
  MAX(mrr) AS mrr,
  LOGICAL_OR(COALESCE(is_paying, FALSE)) AS is_paying,
  COUNT(*) AS known_users,
  -- Reported separately rather than buried in the engagement denominator: how
  -- much of the contract PostHog can see at all is a coverage question, and
  -- mixing it into an activation ratio is what makes large accounts look
  -- disengaged.
  LEAST(SAFE_DIVIDE(COUNT(*), NULLIF(MAX(seats), 0)), 1) AS seat_coverage,
  -- Set by the `seat_denominator` pipeline variable. `reachable` bounds the
  -- denominator by the people PostHog has actually seen; `contracted` scores
  -- against the seats the account pays for, which penalises every account whose
  -- `identify` coverage lags its licence count. Either way an account with no
  -- seat count falls back to its known users, because dividing by zero would
  -- read as zero activation rather than as unknown.
{% if var.seat_denominator == 'reachable' %}
  LEAST(
    COALESCE(NULLIF(MAX(seats), 0), COUNT(*)),
    COUNT(*)
  ) AS licensed_seats
{% elif var.seat_denominator == 'contracted' %}
  COALESCE(NULLIF(MAX(seats), 0), COUNT(*)) AS licensed_seats
{% else %}
  -- The `enum` on this variable is documentation only: Bruin currently validates
  -- that a variable has a default and nothing else, and `--var` overrides skip
  -- schema checks entirely. So an unrecognised value has to fail here instead,
  -- loudly. Falling through to a default would silently score every account
  -- against the wrong denominator, which is the failure this whole variable
  -- exists to make visible.
  CAST(ERROR(
    'seat_denominator must be "reachable" or "contracted", got '
    || {{ sql_literal(var.seat_denominator) }}
  ) AS INT64) AS licensed_seats
{% endif %}
FROM posthog_stage.persons
WHERE company IS NOT NULL
GROUP BY company;
