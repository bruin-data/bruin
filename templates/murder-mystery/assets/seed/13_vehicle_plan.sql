/* @bruin
name: _gen.vehicle_plan
description: |
  Generation scaffolding. Every taxed vehicle with the registration series it was
  issued from and how it is used, before the public columns are selected out.
  Dropped with the rest of this schema at the end of the run.
materialization:
  type: table
depends:
  - _gen.citizen_base
  - _gen.actor_overrides
  - town.businesses
@bruin */

WITH keepers AS (
    SELECT
        b.citizen_id,
        b.seq,
        -- A pinned key is scaled into the band that is certain to be selected
        -- rather than pushed to the front of it, so a pinned row lands at an
        -- unremarkable position instead of at rank one.
        row_number() OVER (
            ORDER BY CASE WHEN coalesce(o.keeps_vehicle, FALSE)
                          THEN {{ rnd('b.seq', 5101) }} * 0.60
                          ELSE {{ rnd('b.seq', 5101) }} END,
            b.citizen_id
        ) AS rk
    FROM _gen.citizen_base b
    LEFT JOIN _gen.actor_overrides o USING (citizen_id)
    WHERE b.age_years >= 18
),
fleets AS (
    SELECT
        business_id,
        row_number() OVER (ORDER BY {{ rnd('business_id', 5102) }}, business_id) AS rk
    FROM town.businesses
    WHERE fleet_size > 0
),
fleet_count AS (SELECT count(*) AS n FROM fleets),
spine AS (
    SELECT
        n,
        -- roughly one vehicle in eight is held by a company
        {{ chance('n', 5110, 0.125) }}                            AS company_held,
        {{ weighted('n', 5111, [['hatchback', 43], ['saloon', 67], ['estate', 79], ['suv', 90], ['van', 96], ['coupe', 99], ['pickup', 100]]) }} AS body_drawn,
        {{ weighted('n', 5112, [['white', 18], ['black', 33], ['silver', 47], ['grey', 60], ['blue', 72], ['red', 81], ['green', 86], ['brown', 90], ['beige', 94], ['bronze', 100]]) }} AS colour_drawn,
        {{ rnd_int('n', 5113, 0, 14) }}                            AS model_idx,
        {{ rnd_int('n', 5114, 2005, 2025) }}                       AS year
    FROM generate_series(1, 7600) t(n)
),
owned AS (
    SELECT
        s.*,
        CASE WHEN s.company_held THEN NULL ELSE k.citizen_id END  AS owner_citizen_id,
        CASE WHEN s.company_held
             THEN (SELECT business_id FROM fleets WHERE rk = 1 + (s.n - 1) % (SELECT n FROM fleet_count))
        END                                                       AS owner_business_id
    FROM spine s
    LEFT JOIN keepers k ON k.rk = s.n
),
resolved AS (
    SELECT
        o.*,
        coalesce(ov.vehicle_body, o.body_drawn)                   AS body_type,
        coalesce(ov.vehicle_colour, o.colour_drawn)               AS colour
    FROM owned o
    LEFT JOIN _gen.actor_overrides ov ON ov.citizen_id = o.owner_citizen_id
),
-- The T series was issued over a decade of Ashmont registrations, so a slice of
-- the light hatchbacks on the road carry it.
series AS (
    SELECT
        r.*,
        (r.colour IN ('grey', 'silver') AND r.body_type = 'hatchback') AS light_hatch,
        row_number() OVER (
            PARTITION BY (r.colour IN ('grey', 'silver') AND r.body_type = 'hatchback')
            ORDER BY CASE WHEN coalesce(ov2.keeps_vehicle, FALSE)
                          THEN {{ rnd('r.n', 5120) }} * 0.045
                          ELSE {{ rnd('r.n', 5120) }} END,
            r.n
        ) AS band_rk,
        row_number() OVER (ORDER BY r.n) AS seq_rk
    FROM resolved r
    LEFT JOIN _gen.actor_overrides ov2 ON ov2.citizen_id = r.owner_citizen_id
),
grouped AS (
    SELECT
        *,
        CASE
            WHEN light_hatch AND band_rk <= 58 THEN 'T'
            WHEN light_hatch                   THEN 'B'
            ELSE                                    'C'
        END AS plate_group,
        band_rk::INTEGER AS group_idx
    FROM series
),
plated AS (
    SELECT
        g.*,
        CASE plate_group
            WHEN 'T' THEN 'T'
                          || chr(65 + ((group_idx - 1) // 26))
                          || chr(65 + ((group_idx - 1) % 26))
            WHEN 'B' THEN chr(65 + CASE WHEN (group_idx - 1) % 25 < 19 THEN (group_idx - 1) % 25 ELSE (group_idx - 1) % 25 + 1 END)
                          || chr(65 + 3 + (((group_idx - 59) // 25) // 26))
                          || chr(65 + (((group_idx - 59) // 25) % 26))
            ELSE          chr(65 + (group_idx - 1) % 26)
                          || chr(65 + 7 + (((group_idx - 1) // 26) // 26))
                          || chr(65 + (((group_idx - 1) // 26) % 26))
        END AS letters,
        CASE plate_group
            -- a 7 somewhere in the numeric part
            WHEN 'T' THEN CASE group_idx % 3
                WHEN 0 THEN 700 + (group_idx * 13) % 100
                WHEN 1 THEN ((group_idx * 7) % 9 + 1) * 100 + 70 + (group_idx * 3) % 10
                ELSE        ((group_idx * 5) % 9 + 1) * 100 + ((group_idx * 11) % 10) * 10 + 7
            END
            -- no 7 at all, encoded over the nine other digits
            WHEN 'B' THEN list_extract([0,1,2,3,4,5,6,8,9], 1 + (group_idx % 9)) * 100
                        + list_extract([0,1,2,3,4,5,6,8,9], 1 + ((group_idx // 9) % 9)) * 10
                        + list_extract([0,1,2,3,4,5,6,8,9], 1 + ((group_idx // 81) % 9))
            ELSE {{ rnd_int('group_idx', 5130, 0, 999) }}
        END AS digits
    FROM grouped g
)
SELECT
    letters || '-' || lpad(digits::VARCHAR, 3, '0')            AS plate,
    plate_group,
    light_hatch,
    n,
    list_extract([{% for m in vehicle_models %}'{{ m[0] }}'{% if not loop.last %}, {% endif %}{% endfor %}], model_idx + 1) AS make,
    list_extract([{% for m in vehicle_models %}'{{ m[1] }}'{% if not loop.last %}, {% endif %}{% endfor %}], model_idx + 1) AS model,
    body_type,
    colour,
    year,
    owner_citizen_id,
    owner_business_id,
    ({{ rally_date() }} - INTERVAL ({{ rnd_int('n', 5140, 20, 4200) }}) DAY)::DATE AS registered_date,
    -- how much the vehicle is on the road, which sets how often cameras see it
    {{ weighted('n', 5150, [['light', 30], ['ordinary', 82], ['heavy', 100]]) }} AS usage_class
FROM plated
