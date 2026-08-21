/* @bruin
name: town.firearm_licences
description: |
  Firearm certificates on the Yorkville register. Yorkville is a hunting town, so
  rifle certificates outnumber everything else and holding one says very little
  about a person.
materialization:
  type: table
depends:
  - _gen.citizen_base
  - _gen.actor_overrides
  - _gen.range_squad
columns:
  - name: licence_id
    type: varchar
    description: Certificate identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: citizen_id
    type: varchar
    description: Certificate holder
    checks:
      - name: not_null
  - name: weapon_class
    type: varchar
    description: Class of firearm the certificate covers
  - name: calibre
    type: varchar
    description: Chambering the certificate covers
  - name: issued_date
    type: date
    description: Date the certificate was issued
  - name: expires_date
    type: date
    description: Date the certificate lapses, five years after issue
  - name: status
    type: varchar
    description: active, expired, revoked or suspended
@bruin */

WITH adults AS (
    SELECT citizen_id, seq, sex, height_cm, prior_service, service_qualification_raw
    FROM _gen.citizen_base
    WHERE age_years >= 18
),
forced AS (
    SELECT citizen_id, firearm_calibre, firearm_status, firearm_class
    FROM _gen.actor_overrides
    WHERE firearm_calibre IS NOT NULL
),
band AS (
    SELECT * FROM adults WHERE sex = 'M' AND height_cm BETWEEN 186 AND 194
),
squad_band AS (
    SELECT citizen_id FROM _gen.range_squad
    WHERE in_band AND citizen_id NOT IN (SELECT citizen_id FROM forced)
),
-- rifle certificates in the stature band
grp_band AS (
    SELECT citizen_id FROM forced
    UNION ALL
    SELECT citizen_id FROM squad_band
    UNION ALL
    SELECT citizen_id FROM (
        SELECT b.citizen_id,
               row_number() OVER (
                   ORDER BY {{ rnd('b.seq', 3001) }}
                            - 0.20 * (b.prior_service IS NOT NULL)::INTEGER,
                   b.citizen_id
               ) AS rn
        FROM band b
        WHERE b.citizen_id NOT IN (SELECT citizen_id FROM forced)
          AND b.citizen_id NOT IN (SELECT citizen_id FROM squad_band)
    ) WHERE rn <= 61 - 1 - (SELECT count(*) FROM squad_band)
),
-- the same chambering held across the rest of the town, weighted the way
-- certificate holding actually distributes: more men, more ex-service
squad_rest AS (
    SELECT citizen_id FROM _gen.range_squad
    WHERE NOT in_band AND citizen_id NOT IN (SELECT citizen_id FROM grp_band)
),
grp_rifle AS (
    SELECT citizen_id FROM squad_rest
    UNION ALL
    SELECT citizen_id FROM (
        SELECT a.citizen_id,
               row_number() OVER (
                   ORDER BY {{ rnd('a.seq', 3002) }}
                            - 0.30 * (a.sex = 'M')::INTEGER
                            - 0.25 * (a.prior_service IS NOT NULL)::INTEGER
                            - 0.30 * (a.service_qualification_raw IN ('marksman', 'designated marksman'))::INTEGER
                            - 0.08 * (a.height_cm >= 182)::INTEGER,
                   a.citizen_id
               ) AS rn
        FROM adults a
        WHERE a.citizen_id NOT IN (SELECT citizen_id FROM grp_band)
          AND a.citizen_id NOT IN (SELECT citizen_id FROM squad_rest)
          AND NOT (a.sex = 'M' AND a.height_cm BETWEEN 186 AND 194)
    ) WHERE rn <= 279 - (SELECT count(*) FROM squad_rest)
),
grp_other AS (
    SELECT citizen_id, rn FROM (
        SELECT a.citizen_id,
               row_number() OVER (
                   ORDER BY {{ rnd('a.seq', 3003) }}
                            - 0.22 * (a.sex = 'M')::INTEGER
                            - 0.18 * (a.prior_service IS NOT NULL)::INTEGER,
                   a.citizen_id
               ) AS rn
        FROM adults a
        WHERE a.citizen_id NOT IN (SELECT citizen_id FROM grp_band)
          AND a.citizen_id NOT IN (SELECT citizen_id FROM grp_rifle)
    ) WHERE rn <= 600
),
grp_lapsed AS (
    SELECT citizen_id, rn FROM (
        SELECT a.citizen_id,
               row_number() OVER (ORDER BY {{ rnd('a.seq', 3004) }}, a.citizen_id) AS rn
        FROM adults a
        WHERE a.citizen_id NOT IN (SELECT citizen_id FROM grp_band)
          AND a.citizen_id NOT IN (SELECT citizen_id FROM grp_rifle)
          AND a.citizen_id NOT IN (SELECT citizen_id FROM grp_other)
    ) WHERE rn <= 240
),
assembled AS (
    SELECT citizen_id, '7.62x51' AS calibre, 'active' AS status, 1 AS bucket FROM grp_band
    UNION ALL
    SELECT citizen_id, '7.62x51', 'active', 2 FROM grp_rifle
    UNION ALL
    SELECT citizen_id,
           {{ weighted('rn', 3010, [['12 gauge', 26], ['.22 LR', 46], ['6.5x55', 60], ['.243 Win', 72], ['20 gauge', 82], ['9x19', 90], ['.303', 96], ['.30-06', 100]]) }},
           'active', 3
    FROM grp_other
    UNION ALL
    SELECT citizen_id,
           {{ weighted('rn', 3011, [['7.62x51', 22], ['12 gauge', 42], ['.22 LR', 60], ['6.5x55', 70], ['.243 Win', 80], ['20 gauge', 88], ['9x19', 94], ['.30-06', 100]]) }},
           {{ weighted('rn', 3012, [['expired', 74], ['revoked', 91], ['suspended', 100]]) }},
           4
    FROM grp_lapsed
),
dated AS (
    SELECT
        row_number() OVER (ORDER BY bucket, citizen_id) AS n,
        citizen_id,
        calibre,
        status,
        CASE
            WHEN status = 'expired' THEN ({{ rally_date() }} - INTERVAL ({{ rnd_int('citizen_id', 3020, 1900, 3600) }}) DAY)::DATE
            ELSE ({{ rally_date() }} - INTERVAL ({{ rnd_int('citizen_id', 3021, 40, 1790) }}) DAY)::DATE
        END AS issued_date
    FROM assembled
)
SELECT
    'FL-' || lpad(n::VARCHAR, 5, '0')                   AS licence_id,
    citizen_id,
    coalesce(o.firearm_class, CASE
        WHEN calibre IN ('12 gauge', '20 gauge')        THEN 'shotgun'
        WHEN calibre IN ('9x19')                        THEN 'pistol'
        WHEN calibre = '.22 LR' AND {{ chance('n', 3030, 0.42) }} THEN 'rimfire rifle'
        WHEN calibre = '.22 LR'                         THEN 'air rifle'
        WHEN {{ chance('n', 3031, 0.71) }}              THEN 'bolt-action rifle'
        ELSE 'semi-automatic rifle'
    END)                                                AS weapon_class,
    calibre,
    issued_date,
    (issued_date + INTERVAL 1826 DAY)::DATE             AS expires_date,
    status
FROM dated
LEFT JOIN _gen.actor_overrides o USING (citizen_id)
