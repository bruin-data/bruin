/* @bruin
name: _gen.scene_overrides
description: |
  Generation scaffolding. A second override layer for attributes that can only be
  settled once the ranked selections upstream have run, keyed by resident.
  Dropped with the rest of this schema at the end of the run.
materialization:
  type: table
depends:
  - _gen.actor_assignments
  - _gen.range_squad
  - _gen.citizen_base
  - _gen.device_base
  - town.addresses
@bruin */

WITH a AS (SELECT * FROM _gen.actor_assignments),
-- the rest of the club's in-band long-range shooters, ordered so the two of them
-- are separable
peers AS (
    SELECT
        q.citizen_id,
        row_number() OVER (ORDER BY {{ rnd('b.seq', 4101) }}, q.citizen_id) AS rk
    FROM _gen.range_squad q
    JOIN _gen.citizen_base b USING (citizen_id)
    WHERE q.in_band AND q.trained
      AND q.citizen_id NOT IN (SELECT actor_a FROM a)
),
cell_of AS (
    SELECT b.citizen_id, ad.nearest_cell_id
    FROM _gen.citizen_base b
    JOIN town.addresses ad USING (address_id)
),
handset_of AS (
    SELECT citizen_id, min(dev) AS dev
    FROM _gen.device_base
    WHERE primary_handset
    GROUP BY 1
),
slots AS (
    SELECT * FROM (VALUES
        ('a', (SELECT actor_a FROM a)),
        ('c', (SELECT actor_c FROM a)),
        ('d', (SELECT actor_d FROM a)),
        ('f', (SELECT citizen_id FROM peers WHERE rk = 1)),
        ('g', (SELECT citizen_id FROM peers WHERE rk = 2))
    ) AS t(slot, citizen_id)
),
kv AS (
    SELECT * FROM (VALUES
        ('a', 'evening_pattern', 'locked'),
        ('a', 'night_cell',      (SELECT nearest_cell_id FROM cell_of WHERE citizen_id = (SELECT actor_d FROM a))),
        ('a', 'night_share',     '0.54'),
        ('a', 'night_key',       (SELECT dev FROM handset_of WHERE citizen_id = (SELECT actor_a FROM a))::VARCHAR),
        ('a', 'night_invert',    'false'),
        ('d', 'night_cell',      (SELECT nearest_cell_id FROM cell_of WHERE citizen_id = (SELECT actor_a FROM a))),
        ('d', 'night_share',     '0.46'),
        ('d', 'night_key',       (SELECT dev FROM handset_of WHERE citizen_id = (SELECT actor_a FROM a))::VARCHAR),
        ('d', 'night_invert',    'true'),
        ('a', 'mobility',        'normal'),
        ('c', 'mobility',        'very_mobile'),
        ('c', 'evening_pattern', 'mobile'),
        ('d', 'evening_pattern', 'mobile'),
        ('f', 'evening_pattern', 'mobile'),
        ('f', 'mobility',        'mobile'),
        ('g', 'evening_pattern', 'drift'),
        ('a', 'gap_start_day',   '18'),
        ('a', 'gap_days',        '2'),
        ('d', 'gap_start_day',   '18'),
        ('d', 'gap_days',        '2')
    ) AS t(slot, k, v)
)
SELECT
    s.slot,
    s.citizen_id,
    max(CASE WHEN kv.k = 'evening_pattern' THEN kv.v END)::VARCHAR AS evening_pattern,
    max(CASE WHEN kv.k = 'night_cell'      THEN kv.v END)::VARCHAR AS night_cell,
    max(CASE WHEN kv.k = 'night_share'     THEN kv.v END)::DOUBLE  AS night_share,
    max(CASE WHEN kv.k = 'mobility'        THEN kv.v END)::VARCHAR AS mobility,
    max(CASE WHEN kv.k = 'night_key'       THEN kv.v END)::BIGINT  AS night_key,
    max(CASE WHEN kv.k = 'night_invert'    THEN kv.v END)::BOOLEAN AS night_invert,
    max(CASE WHEN kv.k = 'gap_start_day'   THEN kv.v END)::INTEGER AS gap_start_day,
    max(CASE WHEN kv.k = 'gap_days'        THEN kv.v END)::INTEGER AS gap_days
FROM slots s
LEFT JOIN kv ON kv.slot = s.slot
GROUP BY s.slot, s.citizen_id
