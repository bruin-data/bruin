/* @bruin
name: _gen.actor_overrides
description: |
  Generation scaffolding. Per-resident attribute overrides that the downstream
  generators coalesce over their own draws. Declared as slot/key/value rows and
  pivoted into one row per resident. Dropped with the rest of this schema at the
  end of the run.
materialization:
  type: table
depends:
  - _gen.citizen_base
  - _gen.actor_assignments
@bruin */

WITH a AS (SELECT * FROM _gen.actor_assignments),
kin AS (
    SELECT c.last_name FROM _gen.citizen_base c JOIN a ON c.citizen_id = a.actor_b
),
slots AS (
    SELECT * FROM (VALUES
        ('a', (SELECT actor_a FROM a)),
        ('b', (SELECT actor_b FROM a)),
        ('c', (SELECT actor_c FROM a)),
        ('d', (SELECT actor_d FROM a)),
        ('e', (SELECT actor_e FROM a)),
        ('v', (SELECT actor_v FROM a))
    ) AS t(slot, citizen_id)
),
kv AS (
    SELECT * FROM (VALUES
        ('a', 'height_cm',              '190'),
        ('a', 'handedness',             'right'),
        ('a', 'shoe_size_eu',           '46'),
        ('a', 'prior_service',          'land forces'),
        ('a', 'service_qualification',  'designated marksman'),
        ('a', 'firearm_calibre',        '7.62x51'),
        ('a', 'firearm_status',         'active'),
        ('a', 'firearm_class',          'bolt-action rifle'),
        ('a', 'range_skill',            '88'),
        ('a', 'range_cadence_days',     '7'),
        ('a', 'range_span_days',        '1096'),
        ('a', 'range_last_visit',       '2026-04-02'),
        ('a', 'range_long_share',       '0.58'),
        ('e', 'last_name',              (SELECT last_name FROM kin)),
        ('e', 'vehicle_colour',         'grey'),
        ('e', 'vehicle_body',           'hatchback'),
        ('e', 'keeps_vehicle',          'true'),
        ('v', 'first_name',             'Adrien'),
        ('v', 'last_name',              'Volk'),
        ('v', 'marital_status',         'married')
    ) AS t(slot, k, v)
)
SELECT
    s.slot,
    s.citizen_id,
    max(CASE WHEN kv.k = 'first_name'            THEN kv.v END)::VARCHAR AS first_name,
    max(CASE WHEN kv.k = 'last_name'             THEN kv.v END)::VARCHAR AS last_name,
    max(CASE WHEN kv.k = 'height_cm'             THEN kv.v END)::INTEGER AS height_cm,
    max(CASE WHEN kv.k = 'handedness'            THEN kv.v END)::VARCHAR AS handedness,
    max(CASE WHEN kv.k = 'eye_colour'            THEN kv.v END)::VARCHAR AS eye_colour,
    max(CASE WHEN kv.k = 'shoe_size_eu'          THEN kv.v END)::INTEGER AS shoe_size_eu,
    max(CASE WHEN kv.k = 'prior_service'         THEN kv.v END)::VARCHAR AS prior_service,
    max(CASE WHEN kv.k = 'service_qualification' THEN kv.v END)::VARCHAR AS service_qualification,
    max(CASE WHEN kv.k = 'marital_status'        THEN kv.v END)::VARCHAR AS marital_status,
    max(CASE WHEN kv.k = 'licence_number'        THEN kv.v END)::VARCHAR AS licence_number,
    max(CASE WHEN kv.k = 'firearm_calibre'       THEN kv.v END)::VARCHAR AS firearm_calibre,
    max(CASE WHEN kv.k = 'firearm_status'        THEN kv.v END)::VARCHAR AS firearm_status,
    max(CASE WHEN kv.k = 'firearm_class'         THEN kv.v END)::VARCHAR AS firearm_class,
    max(CASE WHEN kv.k = 'range_skill'           THEN kv.v END)::INTEGER AS range_skill,
    max(CASE WHEN kv.k = 'range_cadence_days'    THEN kv.v END)::INTEGER AS range_cadence_days,
    max(CASE WHEN kv.k = 'range_span_days'       THEN kv.v END)::INTEGER AS range_span_days,
    max(CASE WHEN kv.k = 'range_last_visit'      THEN kv.v END)::DATE    AS range_last_visit,
    max(CASE WHEN kv.k = 'range_long_share'      THEN kv.v END)::DOUBLE  AS range_long_share,
    max(CASE WHEN kv.k = 'vehicle_colour'        THEN kv.v END)::VARCHAR AS vehicle_colour,
    max(CASE WHEN kv.k = 'vehicle_body'          THEN kv.v END)::VARCHAR AS vehicle_body,
    coalesce(max(CASE WHEN kv.k = 'keeps_vehicle' THEN kv.v END)::BOOLEAN, FALSE) AS keeps_vehicle
FROM slots s
LEFT JOIN kv ON kv.slot = s.slot
GROUP BY s.slot, s.citizen_id
