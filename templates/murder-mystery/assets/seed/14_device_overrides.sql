/* @bruin
name: _gen.device_overrides
description: |
  Generation scaffolding. Handset-keyed overrides for the small number of
  prepaid handsets whose behaviour is pinned rather than drawn. Dropped with the
  rest of this schema at the end of the run.
materialization:
  type: table
depends:
  - _gen.device_base
  - _gen.actor_assignments
@bruin */

WITH a AS (SELECT * FROM _gen.actor_assignments),
counter_sold AS (
    SELECT
        device_id,
        row_number() OVER (ORDER BY {{ rnd('dev', 4201) }}, device_id) AS rk
    FROM _gen.device_base
    WHERE citizen_id IS NULL
),
handset_of AS (
    SELECT citizen_id, min(device_id) AS device_id
    FROM _gen.device_base
    WHERE primary_handset
    GROUP BY 1
),
slots AS (
    SELECT * FROM (VALUES
        ('p', (SELECT device_id FROM counter_sold WHERE rk = 1)),
        ('q', (SELECT device_id FROM counter_sold WHERE rk = 2)),
        ('r', (SELECT device_id FROM counter_sold WHERE rk = 3))
    ) AS t(slot, device_id)
),
kv AS (
    SELECT * FROM (VALUES
        ('p', 'fixed_cell',        {{ corvid_cell() }}),
        ('p', 'activated_date',    '2026-04-08'),
        ('p', 'evening_pattern',   'rooftop'),
        ('p', 'dark',              'true'),
        ('q', 'follows_device_id', (SELECT device_id FROM handset_of WHERE citizen_id = (SELECT actor_b FROM a))),
        ('q', 'follow_rate',       '0.34'),
        ('q', 'activated_date',    '2026-04-11'),
        ('r', 'follows_device_id', (SELECT device_id FROM handset_of WHERE citizen_id = (SELECT actor_c FROM a))),
        ('r', 'follow_rate',       '0.55'),
        ('r', 'activated_date',    '2026-04-09')
    ) AS t(slot, k, v)
)
SELECT
    s.slot,
    s.device_id,
    max(CASE WHEN kv.k = 'follows_device_id' THEN kv.v END)::VARCHAR AS follows_device_id,
    max(CASE WHEN kv.k = 'follow_rate'       THEN kv.v END)::DOUBLE  AS follow_rate,
    coalesce(max(CASE WHEN kv.k = 'dark'     THEN kv.v END)::BOOLEAN, FALSE) AS dark,
    max(CASE WHEN kv.k = 'fixed_cell'        THEN kv.v END)::VARCHAR AS fixed_cell,
    max(CASE WHEN kv.k = 'activated_date'    THEN kv.v END)::DATE    AS activated_date,
    max(CASE WHEN kv.k = 'evening_pattern'   THEN kv.v END)::VARCHAR AS evening_pattern
FROM slots s
LEFT JOIN kv ON kv.slot = s.slot
GROUP BY s.slot, s.device_id
