/* @bruin
name: town.council_decisions
description: |
  Motions put to Yorkville town council and how they went. Where a vote is tied the
  chair casts the deciding vote, and the minutes record who that was.
materialization:
  type: table
depends:
  - _gen.actor_assignments
  - town.property_records
columns:
  - name: motion_id
    type: varchar
    description: Motion identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: motion
    type: varchar
    description: Motion as minuted
  - name: decided_on
    type: date
    description: Date the motion was decided
  - name: outcome
    type: varchar
    description: carried, rejected or deferred
  - name: votes_for
    type: integer
    description: Votes in favour
  - name: votes_against
    type: integer
    description: Votes against
  - name: casting_vote_citizen_id
    type: varchar
    description: Councillor who cast the deciding vote, null unless the vote was tied
  - name: affected_district
    type: varchar
    description: District the motion concerned, null for town-wide business
@bruin */

WITH a AS (SELECT * FROM _gen.actor_assignments),
spine AS (
    SELECT
        n,
        {{ pick('n', 6601, ['Highway maintenance schedule', 'Market licence renewal', 'Allotment tenancy terms', 'Street lighting replacement', 'Refuse collection contract', 'Library opening hours', 'Riverside path works', 'Parking charge review', 'School transport funding', 'Cemetery extension', 'Flood defence survey', 'Rezoning application', 'Conservation area boundary', 'Public toilet provision', 'Bus shelter programme', 'Sports pitch drainage']) }} AS subject,
        {{ pick('n', 6602, districts) }}                       AS district,
        ({{ rally_date() }} - INTERVAL ({{ rnd_int('n', 6603, 20, 2600) }}) DAY)::DATE AS decided_on,
        {{ rnd_int('n', 6604, 3, 11) }}                        AS votes_for,
        {{ rnd_int('n', 6605, 2, 10) }}                        AS votes_against
    FROM generate_series(2, 340) t(n)
)
SELECT
    'MO-' || lpad('1', 4, '0')                                AS motion_id,
    'Rezoning application: Nordheimer Vale ravine lands, from undeveloped to mixed'  AS motion,
    DATE '2026-03-11'                                         AS decided_on,
    'rejected'                                                AS outcome,
    7                                                         AS votes_for,
    7                                                         AS votes_against,
    (SELECT actor_v FROM a)                                   AS casting_vote_citizen_id,
    'Nordheimer Vale'                                            AS affected_district
UNION ALL
SELECT
    'MO-' || lpad(s.n::VARCHAR, 4, '0'),
    s.subject || ': ' || s.district,
    s.decided_on,
    CASE
        WHEN s.votes_for = s.votes_against THEN {{ weighted('s.n', 6610, [['carried', 50], ['rejected', 100]]) }}
        WHEN s.votes_for > s.votes_against THEN 'carried'
        WHEN {{ rnd('s.n', 6611) }} < 0.11 THEN 'deferred'
        ELSE 'rejected'
    END,
    s.votes_for,
    s.votes_against,
    CASE WHEN s.votes_for = s.votes_against THEN (SELECT actor_v FROM a) END,
    CASE WHEN {{ rnd('s.n', 6612) }} < 0.72 THEN s.district END
FROM spine s
