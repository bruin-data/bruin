/* @bruin
name: _gen.citizen_base
description: |
  Generation scaffolding. Every resident of Yorkville with their unadjusted
  attributes, before the override pass in town.citizens. This schema is dropped
  at the end of the run and is not part of the played database.
materialization:
  type: table
depends:
  - town.addresses
@bruin */

WITH homes AS (
    SELECT
        address_id,
        row_number() OVER (ORDER BY address_id) AS home_idx
    FROM town.addresses
    WHERE building_type IN ('house', 'terrace', 'flat_block', 'maisonette')
),
home_count AS (
    SELECT count(*) AS n_homes FROM homes
),
spine AS (
    SELECT
        n                                                       AS seq,
        'C' || lpad(n::VARCHAR, 5, '0')                          AS citizen_id,
        1 + floor({{ rnd('n', 2001) }} * (SELECT n_homes FROM home_count))::BIGINT AS home_idx,
        {{ weighted('n', 2002, [['M', 49], ['F', 100]]) }}       AS sex,
        -- an age pyramid, declared decade by decade
        {{ weighted('n', 2003, [['0', 10], ['1', 21], ['2', 34], ['3', 48], ['4', 62], ['5', 75], ['6', 86], ['7', 94], ['8', 99], ['9', 100]]) }}::INTEGER AS decade,
        {{ rnd_int('n', 2004, 0, 9) }}                           AS year_in_decade,
        {{ rnd_int('n', 2005, 0, 364) }}                         AS day_in_year,
        {{ rnd_norm('n', 2010) }}                                AS height_draw,
        {{ rnd('n', 2020) }}                                     AS service_draw,
        {{ rnd('n', 2030) }}                                     AS surname_draw,
        {{ rnd('n', 2031) }}                                     AS shoe_draw,
        {{ rnd('n', 2032) }}                                     AS licence_draw
    FROM generate_series(1, 12400) t(n)
),
attributed AS (
    SELECT
        s.citizen_id,
        s.seq,
        s.sex,
        least(s.decade * 10 + s.year_in_decade, 96)              AS age_years,
        h.address_id,
        s.height_draw,
        s.shoe_draw,
        s.licence_draw,
        s.service_draw,
        s.surname_draw,
        s.day_in_year
    FROM spine s
    JOIN homes h ON h.home_idx = s.home_idx
),
sized AS (
    SELECT
        *,
        -- adult stature, scaled down the growth curve for anyone still on it
        (CASE WHEN sex = 'M' THEN 176.2 + height_draw * 6.85 ELSE 163.5 + height_draw * 6.0 END)
            * CASE WHEN age_years >= 18 THEN 1.0
                   ELSE 0.28 + 0.72 * pow(age_years / 17.0, 0.62) END AS stature,
        -- prior service is more common among men and only among adults; the
        -- threshold is conditioned on sex rather than drawn from one flat pool
        CASE
            WHEN age_years < 24 THEN NULL
            WHEN sex = 'M' AND service_draw < 0.175 THEN {{ pick('seq', 2060, ['land forces', 'naval service', 'air service', 'engineer corps']) }}
            WHEN sex = 'F' AND service_draw < 0.054 THEN {{ pick('seq', 2061, ['land forces', 'naval service', 'air service', 'engineer corps']) }}
            ELSE NULL
        END                                                     AS prior_service
    FROM attributed
),
-- Recorded qualifications are filled to a quota rather than drawn per row, so
-- the register holds a stable number of each however the town shifts. The
-- specialist quota is split between two ranked pools, because rifle training is
-- concentrated in the taller intakes of the land branches.
served AS (
    SELECT
        citizen_id,
        seq,
        (sex = 'M' AND round(stature) BETWEEN 186 AND 194) AS in_band,
        row_number() OVER (
            PARTITION BY (sex = 'M' AND round(stature) BETWEEN 186 AND 194)
            ORDER BY {{ rnd('seq', 2062) }}, citizen_id
        ) AS pool_rank
    FROM sized
    WHERE prior_service IS NOT NULL
),
qualified AS (
    SELECT
        citizen_id,
        CASE
            WHEN in_band     AND pool_rank <= 10 THEN CASE WHEN pool_rank <= 5  THEN 'designated marksman' ELSE 'marksman' END
            WHEN NOT in_band AND pool_rank <= 34 THEN CASE WHEN pool_rank <= 17 THEN 'designated marksman' ELSE 'marksman' END
            ELSE {{ weighted('seq', 2063, [['', 54], ['logistics', 64], ['signals', 72], ['vehicle mechanic', 80], ['combat medic', 86], ['field engineer', 91], ['physical training', 96], ['small arms instructor', 100]]) }}
        END AS service_qualification_raw
    FROM served
)
SELECT
    z.citizen_id,
    CASE
        WHEN z.sex = 'M' THEN {{ pick('z.seq', 2040, male_first_names) }}
        ELSE {{ pick('z.seq', 2041, female_first_names) }}
    END                                                          AS first_name,
    -- most people in a household share a surname; lodgers and unmarried
    -- partners do not, which is what makes a shared surname worth noticing
    CASE
        WHEN z.surname_draw < 0.78 THEN {{ pick('z.address_id', 2042, surnames) }}
        ELSE {{ pick('z.seq', 2043, surnames) }}
    END                                                          AS last_name,
    ({{ rally_date() }} - INTERVAL (z.age_years * 365 + z.day_in_year) DAY)::DATE AS date_of_birth,
    z.age_years,
    z.sex,
    z.address_id,
    round(z.stature)::INTEGER                                    AS height_cm,
    {{ weighted('z.seq', 2050, [['right', 89], ['left', 99], ['ambidextrous', 100]]) }} AS handedness,
    {{ weighted('z.seq', 2051, [['brown', 45], ['blue', 67], ['hazel', 82], ['green', 94], ['grey', 100]]) }} AS eye_colour,
    {{ msisdn('z.seq') }}                                        AS phone_number,
    CASE
        WHEN z.age_years < 18 THEN 'minor'
        ELSE {{ weighted('z.seq', 2052, [['single', 34], ['married', 75], ['cohabiting', 87], ['divorced', 96], ['widowed', 100]]) }}
    END                                                          AS marital_status,
    ({{ rally_date() }} - INTERVAL ({{ rnd_int('z.seq', 2053, 30, 9000) }}) DAY)::DATE AS moved_in_date,
    CASE
        WHEN {{ rnd('z.seq', 2054) }} < 0.61 THEN 'Yorkville'
        ELSE {{ pick('z.seq', 2055, ['Deer Park', 'Summerhill', 'Moore Park', 'Rathnelly', 'Humewood', 'Cedarvale', 'Lytton Park', 'Chaplin Estates', 'Forest Hill Village', 'Oakwood Vale', 'Corso Italia', 'Regal Heights']) }}
    END                                                          AS birth_town,
    z.prior_service,
    q.service_qualification_raw,
    -- shoe size tracks stature, as it does in life
    least(50, greatest(34, round(z.stature * 0.2455 + (z.shoe_draw - 0.5) * 1.6)))::INTEGER AS shoe_size_eu,
    CASE
        WHEN z.age_years < 17 THEN NULL
        WHEN z.age_years < 25  AND z.licence_draw < 0.20 THEN NULL
        WHEN z.age_years < 65  AND z.licence_draw < 0.12 THEN NULL
        WHEN z.age_years >= 65 AND z.licence_draw < 0.34 THEN NULL
        ELSE 'D' || lpad((((z.seq * 3571) % 8000000) + 1000000)::VARCHAR, 7, '0')
    END                                                          AS licence_number,
    z.height_draw,
    z.shoe_draw,
    z.licence_draw,
    z.service_draw,
    z.seq
FROM sized z
LEFT JOIN qualified q USING (citizen_id)
