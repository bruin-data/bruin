/* @bruin

name: bruin_payments.kpi_txn_daily
type: clickhouse.sql
description: Daily payment and risk KPIs, and the asset where the additivity trade-off becomes concrete. Counts and volume are summed up from the minute rollup because they are additive. Unique active cards, unique active merchants and P95 authorization latency cannot be summed from any pre-aggregate, so they are re-derived from the change log for the days this run touches. Days touched by the interval are recomputed in full and upserted.
owner: risk-platform@example.com

tags:
  - layer:kpi
  - domain:payments
  - grain:day
  - strategy:merge
domains:
  - payments
  - risk
  - finance
meta:
  data_classification: internal
  grain: one row per authorization date and (merchant_category, card_network, country)
  additive_measures: counts and volume, summed from bruin_payments.rollup_txn_1m
  non_additive_measures: unique_cards, unique_merchants and p95_auth_latency_ms, re-derived from bruin_payments.stg_transaction_changes
  non_additive_rationale: a distinct count and a quantile cannot be reconstructed from per-minute results; only their aggregate state can, which is what an AggregatingMergeTree would store. See docs/mode2-aggregating-mergetree.md
  refresh_strategy: full recompute of every date the run interval touches, upserted on kpi_key
  correction_window: 3 hours
  correction_mechanism: interval_modifiers, which require bruin run --apply-interval-modifiers
  freshness_sla: one minute for the current day

materialization:
  type: table
  strategy: merge

interval_modifiers:
  start: -3h

parameters:
  engine: replacing_merge_tree

depends:
  - bruin_payments.rollup_txn_1m
  - bruin_payments.stg_transaction_changes

custom_checks:
  - name: daily grain reconciles with the minute grain
    description: "Summing the daily KPI table must reproduce the minute rollup over the dates the KPI table covers."
    query: |
      SELECT abs(daily_txns - minute_txns)
      FROM (
          SELECT
              (SELECT sum(txns) FROM bruin_payments.kpi_txn_daily) AS daily_txns,
              (
                  SELECT sum(txns)
                  FROM bruin_payments.rollup_txn_1m
                  WHERE toDate(txn_minute) IN (SELECT txn_date FROM bruin_payments.kpi_txn_daily)
              ) AS minute_txns
      )
    value: 0
    blocking: true
  - name: unique cards never exceed transactions
    description: "A distinct count of cards cannot exceed the transactions it was computed from."
    query: |
      SELECT count()
      FROM bruin_payments.kpi_txn_daily
      WHERE unique_cards > txns OR unique_merchants > txns
    value: 0
    blocking: true
  - name: rates stay within bounds
    description: "Every derived rate is a share of the transaction count and must sit in [0, 1]."
    query: |
      SELECT count()
      FROM bruin_payments.kpi_txn_daily
      WHERE approval_rate < 0 OR approval_rate > 1
         OR decline_rate < 0 OR decline_rate > 1
         OR fraud_rate < 0 OR fraud_rate > 1
         OR chargeback_rate < 0 OR chargeback_rate > 1
    value: 0
    blocking: true
  - name: no future dates
    description: "The KPI table must never contain a date in the future."
    query: |
      SELECT count()
      FROM bruin_payments.kpi_txn_daily
      WHERE txn_date > toDate(now('UTC'))
    value: 0
    blocking: true

columns:
  - name: kpi_key
    type: String
    description: Date-prefixed key over the date and all three dimensions; the merge key and the ClickHouse sorting key.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: txn_date
    type: Date
    description: UTC authorization date.
    checks:
      - name: not_null
  - name: merchant_category
    type: LowCardinality(String)
    description: Merchant category dimension.
    checks:
      - name: not_null
  - name: card_network
    type: LowCardinality(String)
    description: Card scheme dimension.
    checks:
      - name: not_null
  - name: country
    type: LowCardinality(String)
    description: ISO-2 country dimension.
    checks:
      - name: not_null
  - name: txns
    type: UInt64
    description: Authorizations on the date.
    checks:
      - name: positive
  - name: approved
    type: UInt64
    description: Authorizations still in the approved state.
  - name: declined
    type: UInt64
    description: Authorizations declined by the issuer.
  - name: refunded
    type: UInt64
    description: Authorizations later refunded.
  - name: chargebacks
    type: UInt64
    description: Authorizations later charged back.
  - name: fraud_flagged
    type: UInt64
    description: Authorizations carrying the source fraud label.
  - name: declined_insufficient_funds
    type: UInt64
    description: Declines attributed to insufficient funds.
  - name: declined_do_not_honor
    type: UInt64
    description: Declines attributed to a do-not-honor response.
  - name: declined_fraud_suspected
    type: UInt64
    description: Declines attributed to suspected fraud.
  - name: declined_expired_card
    type: UInt64
    description: Declines attributed to an expired card.
  - name: approved_volume_cents
    type: Int64
    description: Approved payment volume in USD minor units.
    checks:
      - name: non_negative
  - name: approved_volume
    type: Decimal(18, 2)
    description: Approved payment volume in USD, derived exactly from the integer minor units.
    checks:
      - name: non_negative
  - name: refunded_volume
    type: Decimal(18, 2)
    description: Volume of authorizations later refunded, in USD.
    checks:
      - name: non_negative
  - name: chargeback_volume
    type: Decimal(18, 2)
    description: Volume of authorizations later charged back, in USD.
    checks:
      - name: non_negative
  - name: unique_cards
    type: UInt64
    description: Distinct cards active on the date. NOT additive across dates or dimensions; re-derived from the change log.
  - name: unique_merchants
    type: UInt64
    description: Distinct merchants active on the date. NOT additive; re-derived from the change log.
  - name: p95_auth_latency_ms
    type: UInt32
    description: 95th percentile authorization latency. NOT additive; re-derived from the change log.
  - name: avg_auth_latency_ms
    type: Float64
    description: Mean authorization latency, derived from the additive latency sum and transaction count.
    checks:
      - name: non_negative
  - name: approval_rate
    type: Float64
    description: Approved divided by total authorizations.
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: decline_rate
    type: Float64
    description: Declined divided by total authorizations.
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: fraud_rate
    type: Float64
    description: Fraud-labelled divided by total authorizations.
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: chargeback_rate
    type: Float64
    description: Charged back divided by total authorizations.
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: source_max_updated_at
    type: DateTime64(6, 'UTC')
    description: Latest source version timestamp contributing to the date.

@bruin */

/* Dates the run interval touched. Each is rebuilt from all of its data, so a
   partial interval never produces a partial day. */
WITH changed_dates AS (
    SELECT DISTINCT toDate(txn_minute) AS txn_date
    FROM bruin_payments.rollup_txn_1m
    WHERE txn_minute >= parseDateTime64BestEffort('{{ start_timestamp }}', 6, 'UTC')
      AND txn_minute <= parseDateTime64BestEffort('{{ end_timestamp }}', 6, 'UTC')
),

/* Additive half: a plain sum of the minute grain. This is the cheap path, and
   the reason the minute rollup exists. */
additive AS (
    SELECT
        toDate(txn_minute)                    AS txn_date,
        merchant_category,
        card_network,
        country,
        sum(txns)                             AS txns,
        sum(approved)                         AS approved,
        sum(declined)                         AS declined,
        sum(refunded)                         AS refunded,
        sum(chargebacks)                      AS chargebacks,
        sum(fraud_flagged)                    AS fraud_flagged,
        sum(declined_insufficient_funds)      AS declined_insufficient_funds,
        sum(declined_do_not_honor)            AS declined_do_not_honor,
        sum(declined_fraud_suspected)         AS declined_fraud_suspected,
        sum(declined_expired_card)            AS declined_expired_card,
        sum(approved_volume_cents)            AS approved_volume_cents,
        sum(refunded_volume_cents)            AS refunded_volume_cents,
        sum(chargeback_volume_cents)          AS chargeback_volume_cents,
        sum(auth_latency_ms_sum)              AS auth_latency_ms_sum,
        max(source_max_updated_at)            AS source_max_updated_at
    FROM bruin_payments.rollup_txn_1m
    WHERE toDate(txn_minute) IN (SELECT txn_date FROM changed_dates)
    GROUP BY txn_date, merchant_category, card_network, country
),

/* The exact set of minutes the additive half is able to see. Bounding the
   non-additive scan by this rather than by the calendar date keeps the two halves
   over the same population of transactions, so a partially backfilled minute
   rollup cannot produce a distinct count that exceeds its own transaction
   count. */
covered_minutes AS (
    SELECT DISTINCT txn_minute
    FROM bruin_payments.rollup_txn_1m
    WHERE toDate(txn_minute) IN (SELECT txn_date FROM changed_dates)
),

/* Non-additive half: uniques and a quantile cannot be reconstructed from the
   minute rollup, so the change log is re-read and collapsed to latest version
   again. This is the whole cost of Mode 1, and it is bounded by restricting the
   scan to the minutes this run is responsible for. */
latest_version AS (
    SELECT
        transaction_id,
        argMax(created_at, updated_at)        AS created_at,
        argMax(card_id, updated_at)           AS card_id,
        argMax(merchant_id, updated_at)       AS merchant_id,
        argMax(merchant_category, updated_at) AS merchant_category,
        argMax(card_network, updated_at)      AS card_network,
        argMax(country, updated_at)           AS country,
        argMax(auth_latency_ms, updated_at)   AS auth_latency_ms
    /* Inner scope so the date filter resolves against the source column rather
       than the argMax alias of the same name. */
    FROM (
        SELECT *
        FROM bruin_payments.stg_transaction_changes
        WHERE toDate(created_at) IN (SELECT txn_date FROM changed_dates)
          AND toDateTime64(toStartOfMinute(created_at), 3, 'UTC') IN (
              SELECT txn_minute FROM covered_minutes
          )
    )
    GROUP BY transaction_id
),
non_additive AS (
    SELECT
        toDate(created_at)                              AS txn_date,
        merchant_category,
        card_network,
        country,
        uniqExact(card_id)                              AS unique_cards,
        uniqExact(merchant_id)                          AS unique_merchants,
        toUInt32(round(quantileExact(0.95)(auth_latency_ms))) AS p95_auth_latency_ms
    FROM latest_version
    GROUP BY txn_date, merchant_category, card_network, country
)
SELECT
    concat(
        toString(a.txn_date), '|',
        a.merchant_category, '|', a.card_network, '|', a.country
    )                                                   AS kpi_key,
    a.txn_date,
    toLowCardinality(a.merchant_category)               AS merchant_category,
    toLowCardinality(a.card_network)                    AS card_network,
    toLowCardinality(a.country)                         AS country,
    toUInt64(a.txns)                                    AS txns,
    toUInt64(a.approved)                                AS approved,
    toUInt64(a.declined)                                AS declined,
    toUInt64(a.refunded)                                AS refunded,
    toUInt64(a.chargebacks)                             AS chargebacks,
    toUInt64(a.fraud_flagged)                           AS fraud_flagged,
    toUInt64(a.declined_insufficient_funds)             AS declined_insufficient_funds,
    toUInt64(a.declined_do_not_honor)                   AS declined_do_not_honor,
    toUInt64(a.declined_fraud_suspected)                AS declined_fraud_suspected,
    toUInt64(a.declined_expired_card)                   AS declined_expired_card,
    toInt64(a.approved_volume_cents)                    AS approved_volume_cents,
    toDecimal64(a.approved_volume_cents, 2) / 100       AS approved_volume,
    toDecimal64(a.refunded_volume_cents, 2) / 100       AS refunded_volume,
    toDecimal64(a.chargeback_volume_cents, 2) / 100     AS chargeback_volume,
    toUInt64(ifNull(n.unique_cards, 0))                 AS unique_cards,
    toUInt64(ifNull(n.unique_merchants, 0))             AS unique_merchants,
    toUInt32(ifNull(n.p95_auth_latency_ms, 0))          AS p95_auth_latency_ms,
    round(a.auth_latency_ms_sum / a.txns, 2)            AS avg_auth_latency_ms,
    round(a.approved / a.txns, 6)                       AS approval_rate,
    round(a.declined / a.txns, 6)                       AS decline_rate,
    round(a.fraud_flagged / a.txns, 6)                  AS fraud_rate,
    round(a.chargebacks / a.txns, 6)                    AS chargeback_rate,
    a.source_max_updated_at
FROM additive AS a
LEFT JOIN non_additive AS n
    ON a.txn_date = n.txn_date
    AND a.merchant_category = n.merchant_category
    AND a.card_network = n.card_network
    AND a.country = n.country
