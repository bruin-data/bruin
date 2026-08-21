/* @bruin
name: qa.partner_stages
description: SPOILER. Cardinality of the partner thread.
materialization:
  type: table
depends:
  - qa.shooter_stages
  - qa.handler_stages
  - town.device_pings
  - town.devices
  - town.card_transactions
  - town.bank_accounts
  - town.merchants
  - town.hotel_stays
  - town.travel_records
custom_checks:
  - name: P1 exactly one handset keeps company with his at two towers on many nights each
    query: SELECT count(*) FROM qa.partner_stages WHERE in_p1
    value: 1
  - name: P1 the runner-up manages at most eight nights at its weaker tower
    query: SELECT coalesce(max(fewest_nights), 0) FROM qa.partner_stages WHERE NOT in_p1
    count: 1
  - name: P1 the separation is at least a factor of one and a half
    query: |
      SELECT count(*) FROM (
        SELECT (SELECT max(fewest_nights) FROM qa.partner_stages WHERE in_p1) AS winner,
               (SELECT coalesce(max(fewest_nights), 0) FROM qa.partner_stages WHERE NOT in_p1) AS runner_up
      ) WHERE winner < runner_up * 1.5
    value: 0
  - name: P1 her own housemates cannot produce the pattern, since it needs two towers
    query: SELECT count(*) FROM qa.partner_stages WHERE in_p1 AND towers < 2
    value: 0
  - name: P3 she bought optics and a range bag, and he bought neither
    query: |
      SELECT count(*) FROM town.card_transactions t
      JOIN town.bank_accounts b USING (account_id)
      JOIN town.merchants m USING (merchant_id)
      WHERE m.category = 'outdoor goods' AND t.ts::date = DATE '2026-03-28'
        AND b.citizen_id = (SELECT partner_citizen_id FROM qa.partner_stages WHERE in_p1)
    value: 2
  - name: P3 a two-night booking in her name in a neighbouring town
    query: |
      SELECT count(*) FROM town.hotel_stays
      WHERE booker_citizen_id = (SELECT partner_citizen_id FROM qa.partner_stages WHERE in_p1)
        AND check_in = DATE '2026-05-12' AND town_name <> 'Ashmont'
    value: 1
  - name: P3 his handset is off the network for those two nights
    query: |
      SELECT count(*) FROM town.device_pings p
      JOIN town.devices d USING (device_id)
      WHERE d.citizen_id = (SELECT citizen_id FROM qa.shooter_stages WHERE in_s8)
        AND p.ts::date IN (DATE '2026-05-12', DATE '2026-05-13')
    value: 0
  - name: P3 she left the country in the days after the rally
    query: |
      SELECT count(*) FROM town.travel_records
      WHERE citizen_id = (SELECT partner_citizen_id FROM qa.partner_stages WHERE in_p1)
        AND direction = 'departure' AND destination_type = 'international'
        AND ts::date > DATE '2026-05-14'
    value: 1
  - name: P4 exactly one call ever passed between her number and the roof handset
    query: |
      SELECT count(*) FROM town.call_records c
      WHERE (c.caller_msisdn IN (SELECT msisdn FROM town.devices WHERE citizen_id = (SELECT partner_citizen_id FROM qa.partner_stages WHERE in_p1))
             AND c.callee_msisdn IN (SELECT subject FROM qa.handler_stages WHERE stage = 'H2' AND detail_a = 'registered'))
         OR (c.callee_msisdn IN (SELECT msisdn FROM town.devices WHERE citizen_id = (SELECT partner_citizen_id FROM qa.partner_stages WHERE in_p1))
             AND c.caller_msisdn IN (SELECT msisdn FROM town.devices WHERE device_id = (SELECT subject FROM qa.handler_stages WHERE stage = 'H1')))
    count: 1
@bruin */

WITH his_device AS (
    SELECT min(d.device_id) AS device_id
    FROM town.devices d
    WHERE d.citizen_id = (SELECT citizen_id FROM qa.shooter_stages WHERE in_s8)
),
his_nights AS (
    SELECT p.ts, p.cell_id
    FROM town.device_pings p
    WHERE p.device_id = (SELECT device_id FROM his_device)
      AND (extract(hour FROM p.ts) >= 22 OR extract(hour FROM p.ts) <= 6)
),
per_tower AS (
    SELECT
        p2.device_id,
        p2.cell_id,
        count(DISTINCT p2.ts::date) AS nights
    FROM his_nights h
    JOIN town.device_pings p2 ON p2.cell_id = h.cell_id AND p2.ts = h.ts
    WHERE p2.device_id <> (SELECT device_id FROM his_device)
    GROUP BY 1, 2
)
SELECT
    pt.device_id,
    d.citizen_id                    AS partner_citizen_id,
    count(*)                        AS towers,
    min(pt.nights)                  AS fewest_nights,
    sum(pt.nights)                  AS total_nights,
    count(*) >= 2 AND min(pt.nights) >= 10 AS in_p1
FROM per_tower pt
JOIN town.devices d USING (device_id)
GROUP BY 1, 2
HAVING count(*) >= 2
