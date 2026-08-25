/* @bruin
name: posthog_stage.feature_flag_exposures
type: bq.sql
description: >
  One row per `$feature_flag_called` event: the record of which flag a person
  was evaluated against and which variant they were served. Joined to
  `posthog_raw.feature_flags` for the flag's current name and enabled state.
  This is exposure, not assignment — a person appears once per evaluation, so
  count distinct people rather than rows when sizing an experiment arm.

materialization:
  type: table
  partition_by: exposed_date
  cluster_by:
    - flag_key

depends:
  - posthog_stage.events
  - posthog_stage.person_distinct_ids
  - posthog_raw.feature_flags

tags:
  - posthog_stage
  - posthog
  - product_analytics
  - feature_flags

columns:
  - name: exposure_id
    type: STRING
    description: >
      Identifier of the exposure, taken from the underlying event id, and the
      natural key of this model.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: person_id
    type: STRING
    description: >
      PostHog person resolved by looking the exposure's distinct ID up in
      `posthog_stage.persons.distinct_ids`. It is null when the distinct ID has
      not yet been merged into a person record.
  - name: distinct_id
    type: STRING
    description: Distinct ID the flag was evaluated for.
    checks:
      - name: not_null
  - name: flag_key
    type: STRING
    description: >
      Key of the evaluated flag, read from the `$feature_flag` property. It is
      the join key back to `posthog_raw.feature_flags.key`.
    checks:
      - name: not_null
  - name: flag_variant
    type: STRING
    description: >
      Variant the person was served, read from `$feature_flag_response`.
      Multivariate flags report the variant key. Boolean flags report the
      *strings* `true` and `false`, so a `false` row is a person who was
      evaluated and not given the feature -- an exposure, but not an adoption.
      Use `was_enabled` rather than testing this column when you mean "got the
      feature".
  - name: was_enabled
    type: BOOL
    description: >
      Whether the person was actually given the feature. True for a boolean flag
      answering `true`, and for any multivariate variant other than `control`.
      It exists because `flag_variant` alone does not say this: PostHog answers
      a boolean flag with the string `false` for people held out of the rollout,
      and counting those rows as exposure reads as adoption by people who never
      saw the feature.
    checks:
      - name: not_null
  - name: exposed_at
    type: TIMESTAMP
    description: When the flag was evaluated.
    checks:
      - name: not_null
  - name: exposed_date
    type: DATE
    description: UTC date of `exposed_at`. The table is partitioned on this column.
    checks:
      - name: not_null
  - name: session_id
    type: STRING
    description: >
      Session the evaluation happened in, so exposures can be tied to the
      behaviour in `posthog_stage.sessions`.
  - name: flag_name
    type: STRING
    description: >
      Human-readable description of the flag, resolved from
      `posthog_raw.feature_flags`. It is null for flags that have since been
      hard-deleted in PostHog.
  - name: flag_is_active
    type: BOOL
    description: >
      Whether the flag is enabled in PostHog right now. This is present state,
      not the state at exposure time, so a historical exposure can belong to a
      flag that is now off.
@bruin */

WITH flags AS (
  -- Flag keys are unique among live flags, but a key can be reused after a
  -- flag is soft-deleted. Prefer the live definition, then the latest update.
  SELECT
    flag_key,
    flag_name,
    flag_is_active
  FROM (
    SELECT
      key AS flag_key,
      name AS flag_name,
      active AS flag_is_active,
      ROW_NUMBER() OVER (
        PARTITION BY key
        ORDER BY COALESCE(deleted, FALSE), updated_at DESC
      ) AS flag_rank
    FROM posthog_raw.feature_flags
    WHERE key IS NOT NULL
  )
  WHERE flag_rank = 1
),

exposures AS (
  SELECT
    event_id AS exposure_id,
    distinct_id,
    JSON_VALUE(properties, '$."$feature_flag"') AS flag_key,
    -- The response is a JSON boolean for boolean flags and a string for
    -- multivariate ones; JSON_VALUE returns both as text.
    JSON_VALUE(properties, '$."$feature_flag_response"') AS flag_variant,
    event_at AS exposed_at,
    event_date AS exposed_date,
    session_id
  FROM posthog_stage.events
  WHERE event_name = '$feature_flag_called'
)

SELECT
  exposure.exposure_id,
  person.person_id,
  exposure.distinct_id,
  exposure.flag_key,
  exposure.flag_variant,
  -- PostHog answers a boolean flag with the string 'false' for anyone held out
  -- of the rollout, and 'control' is the conventional holdout arm name for a
  -- multivariate flag. Everything else means the person got the feature.
  COALESCE(exposure.flag_variant NOT IN ('false', 'control'), FALSE) AS was_enabled,
  exposure.exposed_at,
  exposure.exposed_date,
  exposure.session_id,
  flag.flag_name,
  flag.flag_is_active
FROM exposures AS exposure
LEFT JOIN posthog_stage.person_distinct_ids AS person
  ON exposure.distinct_id = person.distinct_id
LEFT JOIN flags AS flag
  ON exposure.flag_key = flag.flag_key
-- An evaluation without a flag key cannot be attributed to anything.
WHERE exposure.flag_key IS NOT NULL;
