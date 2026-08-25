/* @bruin
name: posthog_stage.persons
type: bq.sql
description: >
  Conformed PostHog persons: one row per person, with the business attributes
  set through `identify` calls lifted out of the `properties` JSON into typed
  columns. `distinct_ids` is converted from JSON to `ARRAY<STRING>` so events
  can be resolved back to a single person after PostHog merges identities.
  Persons who were never identified carry none of the business attributes and
  are flagged with `is_anonymous`, so filter on that flag rather than assuming
  every column is populated.

materialization:
  type: table

depends:
  - posthog_raw.persons

tags:
  - posthog_stage
  - posthog
  - product_analytics
  - persons

columns:
  - name: person_id
    type: STRING
    description: PostHog person identifier and natural key. A UUID string, not a number.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: primary_distinct_id
    type: STRING
    description: >
      First distinct ID PostHog lists for the person, kept as a stable display
      key. Join events on `distinct_ids` instead, because a merged person can
      own several distinct IDs and events may arrive under any of them.
  - name: distinct_ids
    type: ARRAY<STRING>
    description: >
      Every distinct ID PostHog has merged into this person, converted from the
      raw JSON array. `UNNEST` it to join `posthog_stage.events.distinct_id`
      back to a person.
  - name: email
    type: STRING
    description: Email address from the `email` property. Null for anonymous persons.
  - name: full_name
    type: STRING
    description: >
      Person's name from the `name` property. This is the identified name, not
      PostHog's derived display name, so it is null for anonymous persons.
  - name: company
    type: STRING
    description: Company name from the `company` property.
  - name: plan
    type: STRING
    description: >
      Current subscription plan from the `plan` property, one of `free`, `pro`,
      or `enterprise`.
  - name: initial_plan
    type: STRING
    description: >
      Plan the person signed up on, from the `initial_plan` property. Compare it
      with `plan` to identify upgrades and downgrades.
  - name: seats
    type: INT64
    description: Number of seats on the account, from the `seats` property.
  - name: mrr
    type: NUMERIC
    description: >
      Monthly recurring revenue attributed to the person, from the `mrr`
      property. It is the account's MRR as PostHog last saw it, not a
      time-series measure.
  - name: is_paying
    type: BOOL
    description: Whether the person is on a paid plan, from the `is_paying` property.
  - name: industry
    type: STRING
    description: Self-reported industry, from the `industry` property.
  - name: role
    type: STRING
    description: Self-reported job role, from the `role` property.
  - name: country
    type: STRING
    description: Country name from the `country` property.
  - name: country_code
    type: STRING
    description: >
      Two-letter country code from the `country_code` property, falling back to
      PostHog's GeoIP-derived `$geoip_country_code` when the property is unset.
  - name: city
    type: STRING
    description: >
      City from the `city` property, falling back to PostHog's GeoIP-derived
      `$geoip_city_name` when the property is unset.
  - name: signup_source
    type: STRING
    description: >
      Acquisition channel recorded at signup, from the `signup_source` property.
  - name: signup_date
    type: DATE
    description: Date the person signed up, from the `signup_date` property.
  - name: created_at
    type: TIMESTAMP
    description: When PostHog first created the person record.
  - name: last_seen_at
    type: TIMESTAMP
    description: When PostHog last saw activity for this person.
  - name: is_identified
    type: BOOL
    description: >
      Whether the person has been through an `$identify` call rather than being
      known only by an anonymous distinct ID.
  - name: is_anonymous
    type: BOOL
    description: >
      Whether the person is an unmerged anonymous record: never identified and
      carrying no email. These rows have null business attributes and are
      usually visitors who left before signing up, so exclude them from
      account-level reporting.
  - name: initial_referrer
    type: STRING
    description: >
      Full referrer URL of the person's first session, from
      `$initial_referrer`. PostHog writes `$direct` when there was none.
  - name: initial_referring_domain
    type: STRING
    description: >
      Domain that referred the person's first session, from
      `$initial_referring_domain`. It is the usual acquisition-channel grain.
  - name: properties
    type: JSON
    description: >
      Full person property payload, kept for attributes that are not flattened
      here.
@bruin */

WITH deduplicated_persons AS (
  -- `posthog_raw.persons` merges on `id`, so this is a no-op on a healthy load.
  -- It is kept so the unique key holds even if the raw asset is switched to an
  -- append strategy.
  SELECT
    id,
    distinct_ids,
    properties,
    is_identified,
    created_at,
    last_seen_at
  FROM (
    SELECT
      id,
      distinct_ids,
      properties,
      is_identified,
      created_at,
      last_seen_at,
      ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY _ingestr_loaded_at DESC
      ) AS load_rank
    FROM posthog_raw.persons
    WHERE id IS NOT NULL
  )
  WHERE load_rank = 1
),

flattened_persons AS (
  SELECT
    id AS person_id,
    -- `distinct_ids` arrives as a JSON array of strings; JSON_VALUE_ARRAY with
    -- the default `$` path converts the whole array to ARRAY<STRING>.
    JSON_VALUE_ARRAY(distinct_ids) AS distinct_ids,
    JSON_VALUE(properties, '$.email') AS email,
    JSON_VALUE(properties, '$.name') AS full_name,
    JSON_VALUE(properties, '$.company') AS company,
    JSON_VALUE(properties, '$.plan') AS plan,
    JSON_VALUE(properties, '$.initial_plan') AS initial_plan,
    SAFE_CAST(JSON_VALUE(properties, '$.seats') AS INT64) AS seats,
    SAFE_CAST(JSON_VALUE(properties, '$.mrr') AS NUMERIC) AS mrr,
    -- JSON booleans come back as the strings 'true'/'false'.
    SAFE_CAST(JSON_VALUE(properties, '$.is_paying') AS BOOL) AS is_paying,
    JSON_VALUE(properties, '$.industry') AS industry,
    JSON_VALUE(properties, '$.role') AS role,
    JSON_VALUE(properties, '$.country') AS country,
    -- Identified properties win; PostHog's GeoIP enrichment is the fallback so
    -- anonymous visitors still resolve to a location.
    COALESCE(
      JSON_VALUE(properties, '$.country_code'),
      JSON_VALUE(properties, '$."$geoip_country_code"')
    ) AS country_code,
    COALESCE(
      JSON_VALUE(properties, '$.city'),
      JSON_VALUE(properties, '$."$geoip_city_name"')
    ) AS city,
    JSON_VALUE(properties, '$.signup_source') AS signup_source,
    SAFE_CAST(JSON_VALUE(properties, '$.signup_date') AS DATE) AS signup_date,
    created_at,
    last_seen_at,
    COALESCE(is_identified, FALSE) AS is_identified,
    JSON_VALUE(properties, '$."$initial_referrer"') AS initial_referrer,
    JSON_VALUE(properties, '$."$initial_referring_domain"') AS initial_referring_domain,
    properties
  FROM deduplicated_persons
)

SELECT
  person_id,
  distinct_ids[SAFE_OFFSET(0)] AS primary_distinct_id,
  distinct_ids,
  email,
  full_name,
  company,
  plan,
  initial_plan,
  seats,
  mrr,
  is_paying,
  industry,
  role,
  country,
  country_code,
  city,
  signup_source,
  signup_date,
  created_at,
  last_seen_at,
  is_identified,
  -- An unmerged anonymous visitor: no `$identify` call ever landed, so none of
  -- the business attributes above are populated.
  (NOT is_identified AND email IS NULL) AS is_anonymous,
  initial_referrer,
  initial_referring_domain,
  properties
FROM flattened_persons;
