/* @bruin
name: town.range_visits
description: |
  Lane bookings at Ashmont's three shooting ranges over the last three years.
  Only the Marlpit ranges have butts beyond 300 metres.
materialization:
  type: table
depends:
  - town.firearm_licences
  - _gen.range_squad
  - _gen.actor_overrides
  - _gen.citizen_base
columns:
  - name: visit_id
    type: varchar
    description: Booking identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: citizen_id
    type: varchar
    description: Club member who booked the lane
    checks:
      - name: not_null
  - name: range_id
    type: varchar
    description: Which range the lane belongs to
  - name: check_in
    type: timestamp
    description: When the member signed in
  - name: lane_distance_m
    type: integer
    description: Lane length in metres
  - name: rounds_fired
    type: integer
    description: Rounds logged against the booking
  - name: score
    type: integer
    description: Scored result out of 100, null when the session was not scored
@bruin */

WITH rifle_holders AS (
    SELECT citizen_id, max((calibre = '7.62x51')::INTEGER) AS long_chambering
    FROM town.firearm_licences
    WHERE status = 'active' AND weapon_class LIKE '%rifle%'
    GROUP BY 1
),
adults AS (
    SELECT b.citizen_id, b.seq
    FROM _gen.citizen_base b
    WHERE b.age_years BETWEEN 16 AND 84
),
members AS (
    SELECT citizen_id, seq FROM (
        SELECT
            a.citizen_id,
            a.seq,
            row_number() OVER (
                ORDER BY {{ rnd('a.seq', 3201) }}
                         - 0.55 * (r.citizen_id IS NOT NULL)::INTEGER
                         - 0.25 * coalesce(r.long_chambering, 0),
                a.citizen_id
            ) AS rn
        FROM adults a
        LEFT JOIN rifle_holders r USING (citizen_id)
    ) WHERE rn <= 1600
),
profile AS (
    SELECT
        m.citizen_id,
        m.seq,
        q.citizen_id IS NOT NULL                                     AS squad,
        r.citizen_id IS NOT NULL                                     AS rifle,
        CASE
            WHEN q.citizen_id IS NOT NULL THEN 'dedicated'
            ELSE {{ weighted('m.seq', 3202, [['dedicated', 2], ['regular', 22], ['casual', 100]]) }}
        END                                                          AS tier,
        coalesce(o.range_skill, CASE
            WHEN q.citizen_id IS NOT NULL THEN 78 + floor({{ rnd('m.seq', 3203) }} * 15)::INTEGER
            WHEN r.citizen_id IS NOT NULL THEN 38 + floor({{ rnd('m.seq', 3203) }} * 27)::INTEGER
            ELSE 26 + floor({{ rnd('m.seq', 3203) }} * 32)::INTEGER
        END)                                                         AS base_skill,
        coalesce(o.range_long_share, CASE
            WHEN q.citizen_id IS NOT NULL THEN 0.45 + {{ rnd('m.seq', 3204) }} * 0.20
            WHEN r.citizen_id IS NOT NULL THEN 0.12
            ELSE 0.0
        END)                                                         AS long_share,
        o.range_cadence_days,
        o.range_span_days,
        o.range_last_visit
    FROM members m
    LEFT JOIN _gen.range_squad q USING (citizen_id)
    LEFT JOIN rifle_holders r USING (citizen_id)
    LEFT JOIN _gen.actor_overrides o USING (citizen_id)
),
scheduled AS (
    SELECT
        p.*,
        coalesce(p.range_cadence_days, CASE p.tier
            WHEN 'dedicated' THEN {{ rnd_int('p.seq', 3210, 7, 11) }}
            WHEN 'regular'   THEN {{ rnd_int('p.seq', 3211, 20, 45) }}
            ELSE                  {{ rnd_int('p.seq', 3212, 80, 260) }}
        END)                                                         AS cadence_days,
        coalesce(p.range_span_days, CASE p.tier
            WHEN 'dedicated' THEN {{ rnd_int('p.seq', 3213, 800, 1300) }}
            WHEN 'regular'   THEN {{ rnd_int('p.seq', 3214, 400, 1300) }}
            ELSE                  {{ rnd_int('p.seq', 3215, 200, 1300) }}
        END)                                                         AS span_days,
        -- most members were still turning up in the weeks before the rally; a
        -- few had already drifted away
        coalesce(p.range_last_visit, CASE
            WHEN {{ rnd('p.seq', 3216) }} < 0.04
                THEN ({{ rally_date() }} - INTERVAL ({{ rnd_int('p.seq', 3217, 60, 520) }}) DAY)::DATE
            ELSE      ({{ rally_date() }} - INTERVAL ({{ rnd_int('p.seq', 3218, 0, 45) }}) DAY)::DATE
        END)                                                         AS last_visit
    FROM profile p
),
sessions AS (
    SELECT
        s.citizen_id,
        s.seq,
        s.base_skill,
        s.tier,
        s.long_share,
        k,
        (s.last_visit - INTERVAL (s.span_days - k * s.cadence_days) DAY)::DATE AS visit_date
    FROM scheduled s
    CROSS JOIN generate_series(0, 200) g(k)
    WHERE k * s.cadence_days <= s.span_days
),
shaped AS (
    SELECT
        citizen_id,
        seq,
        tier,
        base_skill,
        visit_date,
        k,
        {{ rnd('seq * 1000 + k', 3220) }} < long_share                AS long_lane,
        {{ rnd_norm('seq * 1000 + k', 3222) }}                        AS skill_noise
    FROM sessions
),
laned AS (
    SELECT
        *,
        CASE
            WHEN long_lane THEN {{ weighted('seq * 1000 + k', 3230, [['600', 55], ['800', 85], ['1000', 100]]) }}::INTEGER
            ELSE                {{ weighted('seq * 1000 + k', 3231, [['25', 22], ['50', 44], ['100', 68], ['200', 88], ['300', 100]]) }}::INTEGER
        END AS lane_distance_m
    FROM shaped
)
SELECT
    'RV-' || lpad(row_number() OVER (ORDER BY citizen_id, visit_date, k)::VARCHAR, 6, '0') AS visit_id,
    citizen_id,
    CASE
        WHEN lane_distance_m >= 600 THEN 'RNG-MARLPIT'
        ELSE {{ weighted('seq * 1000 + k', 3240, [['RNG-MARLPIT', 44], ['RNG-WEIRS', 76], ['RNG-NORTHGATE', 100]]) }}
    END                                                              AS range_id,
    (visit_date + INTERVAL ({{ rnd_int('seq * 1000 + k', 3241, 8, 19) }}) HOUR
                + INTERVAL ({{ rnd_int('seq * 1000 + k', 3242, 0, 59) }}) MINUTE)::TIMESTAMP AS check_in,
    lane_distance_m,
    CASE tier
        WHEN 'dedicated' THEN {{ rnd_int('seq * 1000 + k', 3243, 20, 60) }}
        ELSE                  {{ rnd_int('seq * 1000 + k', 3244, 10, 40) }}
    END                                                              AS rounds_fired,
    CASE
        WHEN {{ rnd('seq * 1000 + k', 3245) }} < 0.08 THEN NULL
        ELSE least(100, greatest(0, round(
            base_skill + skill_noise * 3.5 - CASE WHEN lane_distance_m >= 600 THEN 4 ELSE 0 END
        )))::INTEGER
    END                                                              AS score
FROM laned
