/* @bruin
name: town.gym_checkins
description: |
  Turnstile records from Ashmont's three fitness centres. High volume and, so far
  as the case goes, entirely beside the point.
materialization:
  type: table
depends:
  - _gen.citizen_base
columns:
  - name: checkin_id
    type: varchar
    description: Turnstile record identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: citizen_id
    type: varchar
    description: Member
    checks:
      - name: not_null
  - name: site_id
    type: varchar
    description: Which centre
  - name: ts
    type: timestamp
    description: When the member came through the turnstile
  - name: minutes_on_site
    type: integer
    description: Minutes between entry and exit
@bruin */

WITH members AS (
    SELECT citizen_id, seq FROM _gen.citizen_base WHERE age_years BETWEEN 14 AND 76
),
enrolled AS (
    SELECT citizen_id, seq, {{ rnd_int('seq', 7101, 1, 5) }} AS per_week
    FROM members
    WHERE {{ rnd('seq', 7100) }} < 0.29
),
spine AS (
    SELECT e.citizen_id, e.seq, e.per_week, d.day, v.visit
    FROM enrolled e
    CROSS JOIN generate_series(0, {{ ledger_days() }} - 1) d(day)
    CROSS JOIN generate_series(1, 2) v(visit)
    WHERE {{ rnd('e.seq * 1000 + d.day * 10 + v.visit', 7110) }} < (e.per_week / 7.0) / v.visit
)
SELECT
    'GY-' || lpad(row_number() OVER (ORDER BY day, citizen_id, visit)::VARCHAR, 7, '0') AS checkin_id,
    citizen_id,
    {{ weighted('seq * 1000 + day * 10 + visit', 7120, [['GY-FOUNDRY', 44], ['GY-NORTHGATE', 75], ['GY-WEIRS', 100]]) }} AS site_id,
    ({{ ledger_start() }} + INTERVAL (day) DAY
        + INTERVAL ({{ rnd_int('seq * 1000 + day * 10 + visit', 7121, 6, 21) }}) HOUR
        + INTERVAL ({{ rnd_int('seq * 1000 + day * 10 + visit', 7122, 0, 59) }}) MINUTE)::TIMESTAMP AS ts,
    {{ rnd_int('seq * 1000 + day * 10 + visit', 7123, 22, 115) }} AS minutes_on_site
FROM spine
