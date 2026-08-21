/* @bruin
name: town.bank_accounts
description: |
  Current accounts held at Ashmont's banks, whether by a resident or a company.
materialization:
  type: table
depends:
  - _gen.citizen_base
  - town.businesses
columns:
  - name: account_id
    type: varchar
    description: Account identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: citizen_id
    type: varchar
    description: Resident holder, null for company accounts
  - name: business_id
    type: varchar
    description: Company holder, null for personal accounts
  - name: account_type
    type: varchar
    description: current, savings or business
  - name: opened
    type: date
    description: Date the account was opened
@bruin */

WITH personal AS (
    SELECT
        b.citizen_id,
        b.seq,
        row_number() OVER (ORDER BY b.citizen_id) AS rk
    FROM _gen.citizen_base b
    WHERE b.age_years >= 16
),
-- a second account, usually savings, for a ranked slice of account holders
seconds AS (
    SELECT citizen_id, seq, rn FROM (
        SELECT p.citizen_id, p.seq,
               row_number() OVER (ORDER BY {{ rnd('p.seq', 5701) }}, p.citizen_id) AS rn
        FROM personal p
    ) WHERE rn <= 2600
)
SELECT
    'AC-' || lpad(rk::VARCHAR, 5, '0')                          AS account_id,
    citizen_id,
    NULL::VARCHAR                                               AS business_id,
    'current'                                                   AS account_type,
    ({{ rally_date() }} - INTERVAL ({{ rnd_int('seq', 5710, 200, 12000) }}) DAY)::DATE AS opened
FROM personal
UNION ALL
SELECT
    'AS-' || lpad(rn::VARCHAR, 5, '0'),
    citizen_id,
    NULL,
    'savings',
    ({{ rally_date() }} - INTERVAL ({{ rnd_int('seq', 5711, 100, 9000) }}) DAY)::DATE
FROM seconds
UNION ALL
SELECT
    'AB-' || lpad(row_number() OVER (ORDER BY business_id)::VARCHAR, 5, '0'),
    NULL,
    business_id,
    'business',
    greatest(founded, ({{ rally_date() }} - INTERVAL 11000 DAY)::DATE)
FROM town.businesses
