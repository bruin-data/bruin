/* @bruin
name: town.card_transactions
description: |
  Ninety days of account entries: card payments, cash machine use, standing
  payments and transfers. Amounts are negative when money leaves the account.
materialization:
  type: table
depends:
  - town.bank_accounts
  - town.merchants
  - town.businesses
  - _gen.ledger_overrides
  - _gen.ledger_overrides_quiet
columns:
  - name: txn_id
    type: varchar
    description: Entry identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: account_id
    type: varchar
    description: Account the entry posted to
    checks:
      - name: not_null
  - name: ts
    type: timestamp
    description: When the entry posted
    checks:
      - name: not_null
  - name: amount
    type: decimal
    description: Signed amount, negative when money left the account
  - name: channel
    type: varchar
    description: card, atm_withdrawal, atm_deposit, transfer_in, transfer_out or direct_debit
  - name: merchant_id
    type: varchar
    description: Terminal the card payment was taken on, null for other channels
  - name: counterparty_name
    type: varchar
    description: Name on the other side of a transfer, null for card and cash entries
@bruin */

WITH acct AS (
    SELECT
        account_id,
        citizen_id,
        business_id,
        account_type,
        row_number() OVER (ORDER BY account_id) AS an
    FROM town.bank_accounts
),
merchant_count AS (SELECT count(*) AS n FROM town.merchants),
-- A holding company owns property and employs nobody, so it does not turn up as
-- the far side of ordinary trading payments the way a shop or a haulier does.
business_names AS (
    SELECT name, row_number() OVER (ORDER BY business_id) AS rk
    FROM town.businesses
    WHERE sector <> 'holding'
),
business_count AS (SELECT count(*) AS n FROM business_names),
-- A couple of hundred accounts take in a sum over two thousand in the window
-- that their ordinary pattern does not explain: inheritances, private sales,
-- building work paid in cash, and a handful of other reasons.
inflow_quota AS (
    SELECT account_id FROM (
        SELECT a.account_id,
               row_number() OVER (ORDER BY {{ rnd('a.an', 5801) }}, a.account_id) AS rk
        FROM acct a
        WHERE a.account_type = 'current'
    ) WHERE rk <= 230
),
spine AS (
    SELECT
        a.account_id,
        a.an,
        a.account_type,
        d.day,
        k.k
    FROM acct a
    CROSS JOIN generate_series(0, {{ ledger_days() }} - 1) d(day)
    CROSS JOIN generate_series(1, 4) k(k)
    WHERE {{ rnd('a.an * 100000 + d.day * 10 + k.k', 5810) }} <
        CASE a.account_type
            WHEN 'current'  THEN list_extract([0.42, 0.16, 0.05, 0.01], k.k)
            WHEN 'savings'  THEN list_extract([0.06, 0.01, 0.00, 0.00], k.k)
            ELSE                 list_extract([0.70, 0.50, 0.30, 0.15], k.k)
        END
),
drawn AS (
    SELECT
        s.account_id,
        ({{ ledger_start() }} + INTERVAL (s.day) DAY
            + INTERVAL ({{ rnd_int('s.an * 100000 + s.day * 10 + s.k', 5811, 7, 22) }}) HOUR
            + INTERVAL ({{ rnd_int('s.an * 100000 + s.day * 10 + s.k', 5812, 0, 59) }}) MINUTE)::TIMESTAMP AS ts,
        CASE s.account_type
            WHEN 'business' THEN {{ weighted('s.an * 100000 + s.day * 10 + s.k', 5813, [['card', 34], ['transfer_out', 58], ['transfer_in', 80], ['direct_debit', 92], ['atm_deposit', 97], ['atm_withdrawal', 100]]) }}
            ELSE                 {{ weighted('s.an * 100000 + s.day * 10 + s.k', 5814, [['card', 62], ['atm_withdrawal', 74], ['direct_debit', 84], ['transfer_out', 91], ['transfer_in', 98], ['atm_deposit', 100]]) }}
        END AS channel,
        {{ rnd('s.an * 100000 + s.day * 10 + s.k', 5815) }} AS size_draw,
        s.an,
        s.day,
        s.k,
        (q.account_id IS NOT NULL) AS inflow_flagged
    FROM spine s
    LEFT JOIN inflow_quota q ON q.account_id = s.account_id
),
priced AS (
    SELECT
        account_id,
        ts,
        channel,
        an, day, k,
        CASE channel
            WHEN 'card'           THEN -round(2.0 + pow(size_draw, 3.1) * 240.0, 2)
            WHEN 'atm_withdrawal' THEN -(10 * (1 + floor(size_draw * 30)))
            WHEN 'direct_debit'   THEN -round(6.0 + pow(size_draw, 2.2) * 420.0, 2)
            WHEN 'transfer_out'   THEN -round(10.0 + pow(size_draw, 2.6) * 1300.0, 2)
            WHEN 'atm_deposit'    THEN  (20 * (1 + floor(size_draw * 40)))
            WHEN 'transfer_in'    THEN round(40.0 + pow(size_draw, 2.4) * 1900.0, 2)
        END AS amount,
        CASE WHEN channel = 'card'
             THEN 'M-' || lpad((1 + floor({{ rnd('an * 100000 + day * 10 + k', 5820) }} * (SELECT n FROM merchant_count))::BIGINT)::VARCHAR, 4, '0')
        END AS merchant_id,
        CASE WHEN channel IN ('transfer_in', 'transfer_out')
             THEN (SELECT name FROM business_names WHERE rk = 1 + floor({{ rnd('an * 100000 + day * 10 + k', 5821) }} * (SELECT n FROM business_count))::BIGINT)
        END AS counterparty_name,
        inflow_flagged,
        CASE WHEN channel = 'transfer_in'
             THEN row_number() OVER (PARTITION BY account_id, channel ORDER BY ts)
        END AS credit_seq,
        size_draw
    FROM drawn
),
-- On a flagged account the first credit in the window is the large one, so the
-- flag always shows up somewhere in the ninety days.
adjusted AS (
    SELECT
        * REPLACE (
            CASE WHEN inflow_flagged AND credit_seq = 1
                 THEN round(2100.0 + size_draw * 5400.0, 2)
                 ELSE amount
            END AS amount
        )
    FROM priced
),
-- Accounts whose holder did not use them by hand during a stated window.
-- Standing payments still post; nobody has to be present for those.
quiet_accounts AS (
    SELECT a.account_id
    FROM _gen.ledger_overrides_quiet q
    JOIN acct a ON a.citizen_id = q.citizen_id
),
pinned AS (
    SELECT
        a.account_id,
        o.ts,
        o.amount,
        o.channel,
        o.merchant_id,
        o.counterparty_name
    FROM _gen.ledger_overrides o
    JOIN acct a ON a.citizen_id = o.citizen_id AND a.account_type = 'current'
)
SELECT
    'TX-' || lpad(row_number() OVER (ORDER BY ts, account_id, channel, amount, merchant_id NULLS LAST, counterparty_name NULLS LAST)::VARCHAR, 7, '0') AS txn_id,
    account_id,
    ts,
    amount::DECIMAL(12, 2) AS amount,
    channel,
    merchant_id,
    counterparty_name
FROM (
    SELECT account_id, ts, amount, channel, merchant_id, counterparty_name FROM adjusted
    WHERE NOT (
        channel IN ('card', 'atm_withdrawal', 'atm_deposit')
        AND account_id IN (SELECT account_id FROM quiet_accounts)
        AND ts >= {{ rally_date() }}::TIMESTAMP + INTERVAL 16 HOUR
        AND ts <  {{ rally_date() }}::TIMESTAMP + INTERVAL 23 HOUR
    )
    UNION ALL
    SELECT account_id, ts, amount, channel, merchant_id, counterparty_name FROM pinned
)
