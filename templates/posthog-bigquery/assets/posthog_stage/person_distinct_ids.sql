/* @bruin
name: posthog_stage.person_distinct_ids
type: bq.sql
description: >
  The distinct-ID-to-person lookup, resolved to exactly one person per distinct
  ID. Events carry a `distinct_id`; persons carry the whole set of distinct IDs
  PostHog has merged into one identity. Every model that needs to attribute an
  event to a person or an account joins through this table rather than
  flattening `persons.distinct_ids` itself, so the resolution rule lives in one
  place and no consumer can accidentally fan an event out across two persons.

materialization:
  type: table

depends:
  - posthog_stage.persons

tags:
  - posthog_stage
  - posthog
  - product_analytics
  - identity

columns:
  - name: distinct_id
    type: STRING
    description: >
      Distinct ID as it appears on `posthog_stage.events.distinct_id`. Unique in
      this table, which is the point of the model.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: person_id
    type: STRING
    description: The person the distinct ID resolves to.
    checks:
      - name: not_null
  - name: company
    type: STRING
    description: >
      Account the person belongs to, carried here so event-to-account joins need
      only this one lookup. Null for persons with no `company` property, which
      is every anonymous visitor.
  - name: is_anonymous
    type: BOOL
    description: >
      Whether the resolved person was never identified. Carried so consumers can
      exclude anonymous traffic without a second join back to `persons`.
@bruin */

WITH flattened AS (
  SELECT
    distinct_id,
    persons.person_id,
    persons.company,
    persons.is_anonymous,
    persons.last_seen_at
  FROM posthog_stage.persons AS persons
  CROSS JOIN UNNEST(persons.distinct_ids) AS distinct_id
)

-- PostHog normally strips a distinct ID from the losing person when it merges
-- two identities, so a distinct ID usually appears once. "Usually" is not a
-- guarantee, and a duplicate here would silently fan out every event joined
-- through it, inflating counts everywhere downstream. Keeping the most recently
-- active person makes the table one row per distinct ID by construction, and
-- the `unique` check above turns any surprise into a failed run rather than a
-- wrong number.
SELECT
  distinct_id,
  ARRAY_AGG(person_id ORDER BY last_seen_at DESC, person_id LIMIT 1)[SAFE_OFFSET(0)] AS person_id,
  ARRAY_AGG(company ORDER BY last_seen_at DESC, person_id LIMIT 1)[SAFE_OFFSET(0)] AS company,
  ARRAY_AGG(is_anonymous ORDER BY last_seen_at DESC, person_id LIMIT 1)[SAFE_OFFSET(0)] AS is_anonymous
FROM flattened
GROUP BY distinct_id;
