/* @bruin
name: _gen.device_base
description: |
  Generation scaffolding. Every handset on the Yorkville networks with its ordinary
  movement profile, before the scene overrides are applied. Dropped with the rest
  of this schema at the end of the run.
materialization:
  type: table
depends:
  - _gen.citizen_base
  - town.addresses
@bruin */

WITH holders AS (
    SELECT
        b.citizen_id,
        b.seq,
        b.age_years,
        a.nearest_cell_id                                   AS home_cell,
        row_number() OVER (ORDER BY b.citizen_id)            AS holder_idx
    FROM _gen.citizen_base b
    JOIN town.addresses a USING (address_id)
    WHERE b.age_years >= 13
),
holder_count AS (SELECT count(*) AS n FROM holders),
-- a second handset for a ranked slice of working-age residents
extras AS (
    SELECT citizen_id, seq, home_cell, rn FROM (
        SELECT
            h.citizen_id,
            h.seq,
            h.home_cell,
            row_number() OVER (ORDER BY {{ rnd('h.seq', 4001) }}, h.citizen_id) AS rn
        FROM holders h
        WHERE h.age_years BETWEEN 22 AND 64
    ) WHERE rn <= 12400 - (SELECT n FROM holder_count)
),
registered AS (
    SELECT
        holder_idx                          AS n,
        citizen_id,
        seq                                 AS msisdn_idx,
        home_cell,
        TRUE                                AS primary_handset
    FROM holders
    UNION ALL
    SELECT
        (SELECT n FROM holder_count) + rn,
        citizen_id,
        12400 + rn,
        home_cell,
        FALSE
    FROM extras
),
-- prepaid handsets are sold over the counter and carry no subscriber record;
-- around fourteen hundred people in Yorkville use one as their only phone
prepaid AS (
    SELECT
        12400 + k                           AS n,
        NULL::VARCHAR                       AS citizen_id,
        15900 + k                           AS msisdn_idx,
        {{ cell_id_of('1 + floor(' + rnd('k', 4002) + ' * 58)::BIGINT') }} AS home_cell,
        FALSE                               AS primary_handset
    FROM generate_series(1, 1800) g(k)
),
all_devices AS (
    SELECT * FROM registered
    UNION ALL
    SELECT * FROM prepaid
),
-- Couples who keep two addresses alternate between them, and both handsets move
-- together: on any given night they are either both at one address or both at
-- the other. Around a hundred and twenty pairs in Yorkville live this way.
pairing AS (
    SELECT
        r.n,
        r.home_cell,
        row_number() OVER (ORDER BY {{ rnd('r.n', 4060) }}, r.n) AS rk
    FROM registered r
    JOIN _gen.citizen_base b USING (citizen_id)
    WHERE r.primary_handset AND b.age_years BETWEEN 22 AND 60
),
paired AS (
    SELECT
        lo.n            AS n,
        hi.home_cell    AS other_cell,
        lo.n            AS night_key,
        round(0.30 + {{ rnd('lo.rk', 4061) }} * 0.50, 2) AS share,
        FALSE           AS invert
    FROM pairing lo
    JOIN pairing hi ON hi.rk = lo.rk + 1
    WHERE lo.rk % 2 = 1 AND lo.rk <= 239
    UNION ALL
    SELECT
        hi.n,
        lo.home_cell,
        lo.n,
        round(1.0 - (0.30 + {{ rnd('lo.rk', 4061) }} * 0.50), 2),
        TRUE
    FROM pairing lo
    JOIN pairing hi ON hi.rk = lo.rk + 1
    WHERE lo.rk % 2 = 1 AND lo.rk <= 239
)
SELECT
    'DEV-' || lpad(d.n::VARCHAR, 5, '0')                     AS device_id,
    d.n                                                      AS dev,
    {{ msisdn('d.msisdn_idx') }}                             AS msisdn,
    d.citizen_id,
    d.primary_handset,
    d.home_cell,
    -- where the handset spends the working day
    CASE
        WHEN {{ rnd('d.n', 4010) }} < 0.34 THEN d.home_cell
        ELSE {{ cell_id_of('1 + floor(' + rnd('d.n', 4011) + ' * 58)::BIGINT') }}
    END                                                      AS work_cell,
    -- a minority of handsets spend their nights somewhere other than the
    -- registered address, and how often varies from person to person
    coalesce(pr.other_cell, CASE
        WHEN {{ rnd('d.n', 4012) }} < 0.088
            THEN {{ cell_id_of('1 + floor(' + rnd('d.n', 4013) + ' * 58)::BIGINT') }}
        ELSE d.home_cell
    END)                                                     AS night_cell,
    coalesce(pr.share, CASE
        WHEN {{ rnd('d.n', 4012) }} < 0.088 THEN round({{ rnd('d.n', 4014) }} * 0.55 + 0.10, 2)
        ELSE 0.0
    END)                                                     AS night_share,
    coalesce(pr.night_key, d.n)                              AS night_key,
    coalesce(pr.invert, FALSE)                                AS night_invert,
    {{ weighted('d.n', 4020, [['static', 24], ['normal', 80], ['mobile', 95], ['very_mobile', 100]]) }} AS mobility,
    ({{ movement_end() }} - INTERVAL ({{ rnd_int('d.n', 4030, 20, 3200) }}) DAY)::DATE AS activated_date,
    {{ weighted('d.n', 4040, [['Corvid C4', 18], ['Corvid C6', 31], ['Meridian 8', 46], ['Meridian 9 Pro', 55], ['Ostler Lite', 68], ['Ostler A2', 77], ['Kestrel One', 86], ['Kestrel Mini', 92], ['Anvil Rugged', 97], ['Anvil Rugged Pro', 100]]) }} AS handset_model,
    -- a handset that is stationary re-registers on a timer rather than on
    -- movement, so it reports far more often than one being carried
    {{ weighted('d.n', 4050, [['locked', 3], ['drift', 19], ['rally', 25], ['mobile', 100]]) }} AS evening_pattern,
    -- a handset that leaves the area stops reporting until it comes back, which
    -- is why a gap in a ping record is not on its own remarkable
    CASE WHEN {{ rnd('d.n', 4070) }} < 0.045
         THEN {{ rnd_int('d.n', 4071, 0, 24) }} END               AS gap_start_day,
    CASE WHEN {{ rnd('d.n', 4070) }} < 0.045
         THEN {{ rnd_int('d.n', 4072, 1, 3) }} END                AS gap_days
FROM all_devices d
LEFT JOIN paired pr ON pr.n = d.n
