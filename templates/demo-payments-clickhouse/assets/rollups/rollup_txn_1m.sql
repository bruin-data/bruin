/* @bruin

name: bruin_payments.rollup_txn_1m
type: clickhouse.sql
description: Per-minute pre-aggregated payment KPIs, the table the live operational dashboard reads. Collapses the append-only change log to each transaction's latest version with argMax on updated_at, then buckets on the authorization time. Every run reprocesses a lookback window so restatements and late arrivals are folded back into minutes that were already sealed.
owner: risk-platform@example.com

tags:
  - layer:rollup
  - domain:payments
  - grain:minute
  - strategy:time-interval
domains:
  - payments
  - risk
meta:
  data_classification: internal
  grain: one row per authorization minute and (merchant_category, card_network, country)
  event_time: created_at, the authorization time; never updated_at
  correction_window: 15 minutes; a restatement arriving later than this is not reflected at the minute grain
  correction_mechanism: interval_modifiers, which require bruin run --apply-interval-modifiers
  additivity: every measure here is additive across time and dimensions; unique cards and P95 latency are deliberately absent
  money_units: USD minor units, carried as integers so sums are exact
  freshness_sla: one minute

materialization:
  type: table
  strategy: time_interval
  incremental_key: txn_minute
  time_granularity: timestamp

interval_modifiers:
  start: -15m

parameters:
  engine: replacing_merge_tree

depends:
  - bruin_payments.stg_transaction_changes

custom_checks:
  - name: rollup reconciles with the change log
    description: "Transactions counted at the minute grain must equal the distinct transactions in the change log over the range the rollup covers."
    query: |
      SELECT abs(rollup_txns - source_txns)
      FROM (
          SELECT
              (SELECT sum(txns) FROM bruin_payments.rollup_txn_1m) AS rollup_txns,
              (
                  SELECT uniqExact(transaction_id)
                  FROM bruin_payments.stg_transaction_changes
                  WHERE created_at >= (SELECT min(txn_minute) FROM bruin_payments.rollup_txn_1m)
                    AND created_at < (SELECT max(txn_minute) + INTERVAL 1 MINUTE FROM bruin_payments.rollup_txn_1m)
              ) AS source_txns
      )
    value: 0
    blocking: true
  - name: one row per minute and dimension set
    description: "The time_interval delete is bounded by the run interval, so a run that started off a minute boundary would leave a partial duplicate of the boundary minute. This check catches that."
    query: |
      SELECT count()
      FROM (
          SELECT txn_minute, merchant_category, card_network, country
          FROM bruin_payments.rollup_txn_1m
          GROUP BY txn_minute, merchant_category, card_network, country
          HAVING count() > 1
      )
    value: 0
    blocking: true
  - name: no future minutes
    description: "The rollup must never contain a minute bucket in the future."
    query: |
      SELECT count()
      FROM bruin_payments.rollup_txn_1m
      WHERE txn_minute > toStartOfMinute(now('UTC'))
    value: 0
    blocking: true
  - name: status counts partition the transaction count
    description: "Every transaction sits in exactly one terminal state, so the four status counts must sum to txns."
    query: |
      SELECT count()
      FROM bruin_payments.rollup_txn_1m
      WHERE approved + declined + refunded + chargebacks != txns
    value: 0
    blocking: true
  - name: decline reasons partition the decline count
    description: "Every declined transaction carries exactly one issuer reason."
    query: |
      SELECT count()
      FROM bruin_payments.rollup_txn_1m
      WHERE declined_insufficient_funds
          + declined_do_not_honor
          + declined_fraud_suspected
          + declined_expired_card != declined
    value: 0
    blocking: true

columns:
  - name: txn_minute
    type: DateTime64(3, 'UTC')
    description: Start of the authorization minute and the incremental key the lookback window replaces.
    primary_key: true
    checks:
      - name: not_null
  - name: merchant_category
    type: LowCardinality(String)
    description: Merchant category dimension.
    primary_key: true
    checks:
      - name: not_null
  - name: card_network
    type: LowCardinality(String)
    description: Card scheme dimension.
    primary_key: true
    checks:
      - name: not_null
  - name: country
    type: LowCardinality(String)
    description: ISO-2 country dimension.
    primary_key: true
    checks:
      - name: not_null
  - name: txns
    type: UInt64
    description: Authorizations in the minute. Serves as the authorizations-per-minute throughput metric.
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
    description: Approved payment volume in USD minor units. The headline volume metric.
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
    description: Sum of authorization latencies. Additive, so mean latency can be derived at any grain; P95 cannot, which is the point.
    checks:
      - name: non_negative
  - name: source_max_updated_at
    type: DateTime64(6, 'UTC')
    description: Latest source version timestamp contributing to the row; the freshness signal for the minute.

@bruin */

/* Collapse the change log to each transaction's latest version.

   The window is bounded on created_at, not updated_at: a transaction's versions
   all share the same authorization time, so bounding on created_at keeps every
   version of the transactions this run is responsible for. The lookback declared
   in interval_modifiers is what widens that bound far enough to pick up
   restatements of minutes that were already written. */
WITH latest_version AS (
    SELECT
        transaction_id,
        argMax(created_at, updated_at)        AS created_at,
        argMax(status, updated_at)            AS status,
        argMax(decline_reason, updated_at)    AS decline_reason,
        argMax(card_network, updated_at)      AS card_network,
        argMax(merchant_category, updated_at) AS merchant_category,
        argMax(country, updated_at)           AS country,
        argMax(is_fraud, updated_at)          AS is_fraud,
        argMax(amount_cents, updated_at)      AS amount_cents,
        argMax(auth_latency_ms, updated_at)   AS auth_latency_ms,
        max(updated_at)                       AS source_max_updated_at
    /* The window filter sits in an inner scope on purpose. Each argMax above is
       aliased to the column it selects, and ClickHouse would resolve a WHERE at
       this level against those aliases rather than the source columns. */
    FROM (
        SELECT *
        FROM bruin_payments.stg_transaction_changes
        WHERE created_at >= parseDateTime64BestEffort('{{ start_timestamp }}', 6, 'UTC')
          AND created_at <= parseDateTime64BestEffort('{{ end_timestamp }}', 6, 'UTC')
    )
    GROUP BY transaction_id
)
SELECT
    toDateTime64(toStartOfMinute(created_at), 3, 'UTC')      AS txn_minute,
    toLowCardinality(merchant_category)                      AS merchant_category,
    toLowCardinality(card_network)                           AS card_network,
    toLowCardinality(country)                                AS country,
    toUInt64(count())                                        AS txns,
    toUInt64(countIf(status = 'approved'))                   AS approved,
    toUInt64(countIf(status = 'declined'))                   AS declined,
    toUInt64(countIf(status = 'refunded'))                   AS refunded,
    toUInt64(countIf(status = 'chargeback'))                 AS chargebacks,
    toUInt64(countIf(is_fraud))                              AS fraud_flagged,
    toUInt64(countIf(decline_reason = 'insufficient_funds')) AS declined_insufficient_funds,
    toUInt64(countIf(decline_reason = 'do_not_honor'))       AS declined_do_not_honor,
    toUInt64(countIf(decline_reason = 'fraud_suspected'))    AS declined_fraud_suspected,
    toUInt64(countIf(decline_reason = 'expired_card'))       AS declined_expired_card,
    toInt64(sumIf(amount_cents, status = 'approved'))        AS approved_volume_cents,
    toInt64(sumIf(amount_cents, status = 'refunded'))        AS refunded_volume_cents,
    toInt64(sumIf(amount_cents, status = 'chargeback'))      AS chargeback_volume_cents,
    toInt64(sum(auth_latency_ms))                            AS auth_latency_ms_sum,
    max(source_max_updated_at)                               AS source_max_updated_at
FROM latest_version
GROUP BY
    txn_minute,
    merchant_category,
    card_network,
    country
