/* @bruin
name: qa.shooter_stages
description: SPOILER. Cardinality of each stage of the shooter thread.
materialization:
  type: table
depends:
  - town.citizens
  - town.firearm_licences
  - town.range_visits
  - town.device_pings
  - town.devices
  - town.card_transactions
  - town.bank_accounts
  - town.call_records
custom_checks:
  - name: S5 male 186-194 with an active 7.62x51 certificate is exactly 61
    query: SELECT count(*) FROM qa.shooter_stages WHERE in_s5
    value: 61
  - name: S6 long-range shooters among the 61 is exactly 9
    query: SELECT count(*) FROM qa.shooter_stages WHERE in_s6
    value: 9
  - name: S6 has a visible gap - no non-shooter in the 61 reaches a long-range mean of 70
    query: SELECT count(*) FROM qa.shooter_stages WHERE in_s5 AND NOT in_s6 AND long_mean >= 70
    value: 0
  - name: S6 separation - every long-range shooter has at least 20 long-range sessions
    query: SELECT count(*) FROM qa.shooter_stages WHERE in_s6 AND long_sessions < 20
    value: 0
  - name: S7 marksman-qualified among the 9 is exactly 3
    query: SELECT count(*) FROM qa.shooter_stages WHERE in_s7
    value: 3
  - name: S8 exactly one of the 3 was silent all evening on one cell
    query: SELECT count(*) FROM qa.shooter_stages WHERE in_s8
    value: 1
  - name: S8 the other two finalists are both clearable by an act on the evening
    query: SELECT count(*) FROM qa.shooter_stages WHERE in_s7 AND NOT in_s8 AND (hand_entries + answered_calls) = 0
    value: 0
  - name: the boot size cannot decide S8 on its own
    query: SELECT count(*) FROM qa.shooter_stages WHERE in_s7 AND shoe_size_eu BETWEEN 45 AND 47
    value: 3
@bruin */

WITH s5 AS (
    SELECT DISTINCT c.citizen_id, c.service_qualification, c.shoe_size_eu
    FROM town.citizens c
    JOIN town.firearm_licences f USING (citizen_id)
    WHERE c.sex = 'M' AND c.height_cm BETWEEN 186 AND 194
      AND f.status = 'active' AND f.calibre = '7.62x51'
),
long_range AS (
    SELECT v.citizen_id, avg(v.score) AS long_mean, count(*) AS long_sessions
    FROM town.range_visits v
    WHERE v.lane_distance_m >= 600 AND v.score IS NOT NULL
    GROUP BY 1
),
evening AS (
    SELECT
        d.citizen_id,
        count(DISTINCT p.cell_id) AS cells_on_the_evening,
        count(*)                  AS pings_on_the_evening
    FROM town.devices d
    JOIN town.device_pings p USING (device_id)
    WHERE d.citizen_id IS NOT NULL
      AND p.ts BETWEEN TIMESTAMP '2026-05-14 17:00:00' AND TIMESTAMP '2026-05-14 23:00:00'
    GROUP BY 1
),
spend AS (
    SELECT b.citizen_id, count(*) AS hand_entries
    FROM town.bank_accounts b
    JOIN town.card_transactions t USING (account_id)
    WHERE t.channel IN ('card', 'atm_withdrawal', 'atm_deposit')
      AND t.ts BETWEEN TIMESTAMP '2026-05-14 16:00:00' AND TIMESTAMP '2026-05-14 23:00:00'
    GROUP BY 1
),
talk AS (
    SELECT d.citizen_id, count(*) AS answered_calls
    FROM town.devices d
    JOIN town.call_records r ON d.msisdn IN (r.caller_msisdn, r.callee_msisdn)
    WHERE d.citizen_id IS NOT NULL
      AND r.duration_sec > 0
      AND r.started_at BETWEEN TIMESTAMP '2026-05-14 16:00:00' AND TIMESTAMP '2026-05-14 23:00:00'
    GROUP BY 1
)
SELECT
    s5.citizen_id,
    s5.shoe_size_eu,
    coalesce(l.long_mean, 0)                                     AS long_mean,
    coalesce(l.long_sessions, 0)                                 AS long_sessions,
    coalesce(e.cells_on_the_evening, 0)                          AS cells_on_the_evening,
    coalesce(sp.hand_entries, 0)                                 AS hand_entries,
    coalesce(tk.answered_calls, 0)                               AS answered_calls,
    TRUE                                                         AS in_s5,
    coalesce(l.long_mean, 0) >= 70                               AS in_s6,
    coalesce(l.long_mean, 0) >= 70
        AND s5.service_qualification IN ('marksman', 'designated marksman') AS in_s7,
    coalesce(l.long_mean, 0) >= 70
        AND s5.service_qualification IN ('marksman', 'designated marksman')
        AND coalesce(e.cells_on_the_evening, 0) = 1
        AND coalesce(sp.hand_entries, 0) = 0
        AND coalesce(tk.answered_calls, 0) = 0                   AS in_s8
FROM s5
LEFT JOIN long_range l USING (citizen_id)
LEFT JOIN evening    e USING (citizen_id)
LEFT JOIN spend      sp USING (citizen_id)
LEFT JOIN talk       tk USING (citizen_id)
