/* @bruin
name: qa.handler_stages
description: SPOILER. Cardinality of each stage of the handler thread.
materialization:
  type: table
depends:
  - town.device_pings
  - town.devices
  - town.call_records
  - town.businesses
  - town.property_records
  - town.council_decisions
  - town.card_transactions
  - town.bank_accounts
custom_checks:
  - name: H1 exactly one handset was on the roof microcell and nowhere else all window
    query: SELECT count(*) FROM qa.handler_stages WHERE stage = 'H1'
    value: 1
  - name: H1 that handset carries no subscriber record
    query: SELECT count(*) FROM qa.handler_stages WHERE stage = 'H1' AND detail_a <> 'unregistered'
    value: 0
  - name: H2 the roof handset reached exactly three numbers
    query: SELECT count(*) FROM qa.handler_stages WHERE stage = 'H2'
    value: 3
  - name: H2 two of the three are also unregistered
    query: SELECT count(*) FROM qa.handler_stages WHERE stage = 'H2' AND detail_a = 'unregistered'
    value: 2
  - name: H2 sparseness alone proves nothing - dozens of prepaid numbers look like this
    query: |
      SELECT count(*) FROM (
        SELECT d.msisdn
        FROM town.devices d
        JOIN town.call_records c ON d.msisdn IN (c.caller_msisdn, c.callee_msisdn)
        WHERE d.citizen_id IS NULL
        GROUP BY 1
        HAVING count(DISTINCT CASE WHEN c.caller_msisdn = d.msisdn THEN c.callee_msisdn ELSE c.caller_msisdn END) < 6
      )
    count: 1
  - name: H3 exactly one handset clears both co-location thresholds
    query: SELECT count(*) FROM qa.handler_stages WHERE stage = 'H3'
    value: 1
  - name: H3 the runner-up shares at most eleven towers, so breadth is the separator
    query: SELECT count(*) FROM qa.handler_stages WHERE stage = 'H3-runner-up' AND metric_a > 11
    value: 0
  - name: H3 the runner-up has high volume, so volume alone is not the separator
    query: SELECT count(*) FROM qa.handler_stages WHERE stage = 'H3-runner-up' AND metric_b < 60
    value: 0
  - name: H4 the rejected riverside motion was decided by a casting vote
    query: SELECT count(*) FROM town.council_decisions WHERE affected_district = 'Foundry Quay' AND outcome = 'rejected' AND casting_vote_citizen_id IS NOT NULL AND decided_on = DATE '2026-03-11'
    value: 1
  - name: H4 the riverside parcels are held by a company with a named principal
    query: |
      SELECT count(DISTINCT b.principal_citizen_id)
      FROM town.property_records p
      JOIN town.businesses b ON b.business_id = p.owner_business_id
      WHERE p.district = 'Foundry Quay' AND p.zoning_class = 'undeveloped'
    value: 1
  - name: H4 that principal also files a second company in the trades
    query: |
      SELECT count(*) FROM town.businesses
      WHERE principal_citizen_id = (
        SELECT DISTINCT b.principal_citizen_id FROM town.property_records p
        JOIN town.businesses b ON b.business_id = p.owner_business_id
        WHERE p.district = 'Foundry Quay' AND p.zoning_class = 'undeveloped')
    value: 2
  - name: H5 the holding company paid two personal accounts
    query: |
      SELECT count(DISTINCT b.citizen_id)
      FROM town.card_transactions t
      JOIN town.bank_accounts b USING (account_id)
      WHERE t.channel = 'transfer_in' AND b.citizen_id IS NOT NULL
        AND t.counterparty_name = (SELECT name FROM town.businesses WHERE business_id = 'B-0001')
    value: 2
@bruin */

WITH totals AS (
    SELECT device_id, count(*) AS pings, count(DISTINCT cell_id) AS cells
    FROM town.device_pings
    GROUP BY 1
),
roof AS (
    SELECT DISTINCT p.device_id
    FROM town.device_pings p
    WHERE p.cell_id = 'CELL-036'
      AND p.ts BETWEEN TIMESTAMP '2026-05-14 17:00:00' AND TIMESTAMP '2026-05-14 23:00:00'
),
h1 AS (
    SELECT r.device_id, d.msisdn, d.citizen_id
    FROM roof r
    JOIN totals t USING (device_id)
    JOIN town.devices d USING (device_id)
    WHERE t.pings <= 30
),
h2 AS (
    SELECT DISTINCT c.callee_msisdn AS msisdn
    FROM town.call_records c
    WHERE c.caller_msisdn = (SELECT msisdn FROM h1)
    UNION
    SELECT DISTINCT c.caller_msisdn
    FROM town.call_records c
    WHERE c.callee_msisdn = (SELECT msisdn FROM h1)
),
handler_burner AS (
    SELECT d.device_id
    FROM h2
    JOIN town.devices d USING (msisdn)
    WHERE d.citizen_id IS NULL
    ORDER BY (SELECT count(*) FROM town.call_records c WHERE c.caller_msisdn = d.msisdn OR c.callee_msisdn = d.msisdn) DESC,
             d.device_id
    LIMIT 1
),
burner_pings AS (
    SELECT ts, cell_id FROM town.device_pings WHERE device_id = (SELECT device_id FROM handler_burner)
),
colocated AS (
    SELECT
        p2.device_id,
        count(DISTINCT p2.cell_id) AS shared_towers,
        count(*)                   AS shared_buckets
    FROM burner_pings b
    JOIN town.device_pings p2 ON p2.cell_id = b.cell_id AND p2.ts = b.ts
    WHERE p2.device_id <> (SELECT device_id FROM handler_burner)
    GROUP BY 1
),
ranked AS (
    SELECT *, row_number() OVER (ORDER BY shared_towers DESC, shared_buckets DESC, device_id) AS rk
    FROM colocated
)
SELECT 'H1' AS stage, device_id AS subject,
       CASE WHEN citizen_id IS NULL THEN 'unregistered' ELSE 'registered' END AS detail_a,
       0 AS metric_a, 0 AS metric_b
FROM h1
UNION ALL
SELECT 'H2', h2.msisdn,
       CASE WHEN d.citizen_id IS NULL THEN 'unregistered' ELSE 'registered' END,
       0, 0
FROM h2 LEFT JOIN town.devices d USING (msisdn)
UNION ALL
SELECT 'H3', device_id, 'above threshold', shared_towers, shared_buckets
FROM colocated WHERE shared_towers >= 14 AND shared_buckets >= 120
UNION ALL
SELECT 'H3-runner-up', device_id, 'best legitimate match', shared_towers, shared_buckets
FROM ranked WHERE rk = 2
