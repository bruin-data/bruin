/* @bruin

name: bruin_payments.rollup_txn_1h
type: clickhouse.sql
description: Hourly grain of the same payment KPIs, aggregated from the minute rollup rather than from the change log. Because every measure at the minute grain is additive, the coarser grain is a plain sum, which is what makes a rollup cascade cheap. Hours touched by the run interval are recomputed in full and upserted, so the asset stays correct even when the interval starts mid-hour.
owner: risk-platform@example.com

tags:
  - layer:rollup
  - domain:payments
  - grain:hour
  - strategy:merge
domains:
  - payments
  - risk
meta:
  data_classification: internal
  grain: one row per authorization hour and (merchant_category, card_network, country)
  derivation: summed from bruin_payments.rollup_txn_1m; never re-reads the change log
  refresh_strategy: full recompute of every hour the run interval touches, upserted on rollup_key
  strategy_rationale: merge rather than time_interval because a lookback that lands mid-hour would rebuild a partial hour under a time_interval delete
  correction_window: 3 hours, wide enough to absorb any correction the minute grain makes
  correction_mechanism: interval_modifiers, which require bruin run --apply-interval-modifiers
  additivity: additive-only measures, inherited from the minute grain
  freshness_sla: one minute

materialization:
  type: table
  strategy: merge

interval_modifiers:
  start: -3h

parameters:
  engine: replacing_merge_tree

depends:
  - bruin_payments.rollup_txn_1m

custom_checks:
  - name: hourly grain reconciles with the minute grain
    description: "Summing the hourly rollup must reproduce the minute rollup over the hours the hourly grain covers."
    query: |
      SELECT abs(hourly_txns - minute_txns)
      FROM (
          SELECT
              (SELECT sum(txns) FROM bruin_payments.rollup_txn_1h) AS hourly_txns,
              (
                  SELECT sum(txns)
                  FROM bruin_payments.rollup_txn_1m
                  WHERE toStartOfHour(txn_minute) IN (
                      SELECT txn_hour FROM bruin_payments.rollup_txn_1h
                  )
              ) AS minute_txns
      )
    value: 0
    blocking: true
  - name: no future hours
    description: "The rollup must never contain an hour bucket in the future."
    query: |
      SELECT count()
      FROM bruin_payments.rollup_txn_1h
      WHERE txn_hour > toStartOfHour(now('UTC'))
    value: 0
    blocking: true

columns:
  - name: rollup_key
    type: String
    description: Hour-prefixed key over the hour and all three dimensions; the merge key and the ClickHouse sorting key.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: txn_hour
    type: DateTime64(3, 'UTC')
    description: Start of the authorization hour.
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
    description: Authorizations in the hour.
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
  - name: approved_volume_cents
    type: Int64
    description: Approved payment volume in USD minor units.
    checks:
      - name: non_negative
  - name: refunded_volume_cents
    type: Int64
    description: Volume of authorizations later refunded, in USD minor units.
    checks:
      - name: non_negative
  - name: chargeback_volume_cents
    type: Int64
    description: Volume of authorizations later charged back, in USD minor units.
    checks:
      - name: non_negative
  - name: auth_latency_ms_sum
    type: Int64
    description: Sum of authorization latencies across the hour.
    checks:
      - name: non_negative
  - name: source_max_updated_at
    type: DateTime64(6, 'UTC')
    description: Latest source version timestamp contributing to the hour.

@bruin */

/* Hours the run interval touched. Only these are rebuilt, and each is rebuilt
   from all of its minutes rather than only the minutes inside the interval, so a
   partial interval never produces a partial hour. */
WITH changed_hours AS (
    SELECT DISTINCT toStartOfHour(txn_minute) AS txn_hour
    FROM bruin_payments.rollup_txn_1m
    WHERE txn_minute >= parseDateTime64BestEffort('{{ start_timestamp }}', 6, 'UTC')
      AND txn_minute <= parseDateTime64BestEffort('{{ end_timestamp }}', 6, 'UTC')
),
hourly AS (
    SELECT
        toStartOfHour(m.txn_minute)             AS txn_hour,
        m.merchant_category                     AS merchant_category,
        m.card_network                          AS card_network,
        m.country                               AS country,
        toUInt64(sum(m.txns))                   AS txns,
        toUInt64(sum(m.approved))               AS approved,
        toUInt64(sum(m.declined))               AS declined,
        toUInt64(sum(m.refunded))               AS refunded,
        toUInt64(sum(m.chargebacks))            AS chargebacks,
        toUInt64(sum(m.fraud_flagged))          AS fraud_flagged,
        toInt64(sum(m.approved_volume_cents))   AS approved_volume_cents,
        toInt64(sum(m.refunded_volume_cents))   AS refunded_volume_cents,
        toInt64(sum(m.chargeback_volume_cents)) AS chargeback_volume_cents,
        toInt64(sum(m.auth_latency_ms_sum))     AS auth_latency_ms_sum,
        max(m.source_max_updated_at)            AS source_max_updated_at
    FROM bruin_payments.rollup_txn_1m AS m
    INNER JOIN changed_hours AS c
        ON toStartOfHour(m.txn_minute) = c.txn_hour
    GROUP BY
        txn_hour,
        merchant_category,
        card_network,
        country
)
SELECT
    concat(
        formatDateTime(txn_hour, '%Y-%m-%dT%H', 'UTC'), '|',
        merchant_category, '|', card_network, '|', country
    )                                     AS rollup_key,
    toDateTime64(txn_hour, 3, 'UTC')      AS txn_hour,
    toLowCardinality(merchant_category)   AS merchant_category,
    toLowCardinality(card_network)        AS card_network,
    toLowCardinality(country)             AS country,
    txns,
    approved,
    declined,
    refunded,
    chargebacks,
    fraud_flagged,
    approved_volume_cents,
    refunded_volume_cents,
    chargeback_volume_cents,
    auth_latency_ms_sum,
    source_max_updated_at
FROM hourly
