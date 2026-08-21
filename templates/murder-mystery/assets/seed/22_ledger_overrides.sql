/* @bruin
name: _gen.ledger_overrides
description: |
  Generation scaffolding. Individual account entries that are pinned to a date,
  an amount and a channel rather than sampled, because the case timeline depends
  on them. Dropped with the rest of this schema at the end of the run.
materialization:
  type: table
depends:
  - _gen.actor_assignments
  - _gen.scene_overrides
  - town.businesses
  - town.merchants
@bruin */

WITH a AS (SELECT * FROM _gen.actor_assignments),
holding AS (
    SELECT name FROM town.businesses WHERE business_id = 'B-0001'
),
who AS (
    SELECT 'a' AS slot, actor_a AS citizen_id FROM a
    UNION ALL SELECT 'b', actor_b FROM a
    UNION ALL SELECT 'd', actor_d FROM a
    UNION ALL SELECT 'f', citizen_id FROM _gen.scene_overrides WHERE slot = 'f'
),
trade AS (
    SELECT
        category,
        min(merchant_id) AS merchant_id
    FROM town.merchants
    GROUP BY 1
),
entries AS (
    SELECT * FROM (VALUES
        ('a', TIMESTAMP '2026-04-19 11:26:00',  4183.40, 'atm_deposit', NULL,            NULL),
        ('a', TIMESTAMP '2026-04-27 09:02:00',  6000.00, 'transfer_in', NULL,            (SELECT name FROM holding)),
        ('b', TIMESTAMP '2026-05-09 14:31:00',  3000.00, 'transfer_in', NULL,            (SELECT name FROM holding)),
        ('b', TIMESTAMP '2026-05-14 17:10:00',   -52.80, 'card',        'fuel',          NULL),
        ('d', TIMESTAMP '2026-03-28 15:47:00',  -742.00, 'card',        'outdoor goods', NULL),
        ('d', TIMESTAMP '2026-03-28 15:52:00',  -168.00, 'card',        'outdoor goods', NULL),
        ('f', TIMESTAMP '2026-05-14 19:14:00',   -38.50, 'card',        'hospitality',   NULL)
    ) AS t(slot, ts, amount, channel, category, counterparty_name)
)
SELECT
    w.citizen_id,
    e.ts,
    e.amount::DECIMAL(12, 2)    AS amount,
    e.channel::VARCHAR          AS channel,
    tr.merchant_id,
    e.counterparty_name::VARCHAR AS counterparty_name
FROM entries e
JOIN who w ON w.slot = e.slot
LEFT JOIN trade tr ON tr.category = e.category
