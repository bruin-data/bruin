/* @bruin

name: bruin_payments.serving_realtime_risk
type: clickhouse.sql
description: One dashboard-ready object over the whole reporting history. The current day is served live from the minute rollup so it reflects the last completed minute, and every earlier day is served from the sealed daily KPI table. Rates are derived here rather than stored twice. The two non-additive KPIs are null for the current day by design, because they cannot be summed out of the minute rollup; that gap is the argument for Mode 2.
owner: risk-platform@example.com

tags:
  - layer:serving
  - domain:payments
  - consumer:dashboard
domains:
  - payments
  - risk
  - finance
meta:
  data_classification: internal
  grain: one row per authorization date and (merchant_category, card_network, country)
  composition: today from bruin_payments.rollup_txn_1m, earlier dates from bruin_payments.kpi_txn_daily
  known_gap: unique_cards and p95_auth_latency_ms are null on the current day; see docs/mode2-aggregating-mergetree.md
  consumer: dashboards/payments_risk.yml via semantic/payments_risk.yml
  freshness_sla: one minute

materialization:
  type: view

depends:
  - bruin_payments.rollup_txn_1m
  - bruin_payments.kpi_txn_daily

columns:
  - name: txn_date
    type: Date
    description: UTC authorization date.
  - name: is_today
    type: UInt8
    description: 1 when the row is served live from the minute rollup, 0 when it comes from the sealed daily KPI table.
  - name: merchant_category
    type: LowCardinality(String)
    description: Merchant category dimension.
  - name: card_network
    type: LowCardinality(String)
    description: Card scheme dimension.
  - name: country
    type: LowCardinality(String)
    description: ISO-2 country dimension.
  - name: txns
    type: UInt64
    description: Authorizations on the date.
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
  - name: approved_volume
    type: Decimal(18, 2)
    description: Approved payment volume in USD.
  - name: refunded_volume
    type: Decimal(18, 2)
    description: Volume of authorizations later refunded, in USD.
  - name: chargeback_volume
    type: Decimal(18, 2)
    description: Volume of authorizations later charged back, in USD.
  - name: unique_cards
    type: Nullable(UInt64)
    description: Distinct active cards. Null for the current day because the measure is not additive from the minute rollup.
  - name: p95_auth_latency_ms
    type: Nullable(UInt32)
    description: 95th percentile authorization latency. Null for the current day for the same reason as unique_cards.
  - name: avg_auth_latency_ms
    type: Float64
    description: Mean authorization latency; additive inputs, so available on every date.
  - name: approval_rate
    type: Float64
    description: Approved divided by total authorizations.
  - name: decline_rate
    type: Float64
    description: Declined divided by total authorizations.
  - name: fraud_rate
    type: Float64
    description: Fraud-labelled divided by total authorizations.
  - name: chargeback_rate
    type: Float64
    description: Charged back divided by total authorizations.
  - name: source_max_updated_at
    type: DateTime64(6, 'UTC')
    description: Latest source version timestamp contributing to the row.

@bruin */

/* Today so far, straight off the minute rollup. */
SELECT
    txn_date,
    toUInt8(1)                                         AS is_today,
    merchant_category,
    card_network,
    country,
    txns,
    approved,
    declined,
    refunded,
    chargebacks,
    fraud_flagged,
    declined_insufficient_funds,
    declined_do_not_honor,
    declined_fraud_suspected,
    declined_expired_card,
    toDecimal64(approved_volume_cents, 2) / 100        AS approved_volume,
    toDecimal64(refunded_volume_cents, 2) / 100        AS refunded_volume,
    toDecimal64(chargeback_volume_cents, 2) / 100      AS chargeback_volume,
    /* Not additive out of a per-minute pre-aggregate, so deliberately absent
       rather than silently wrong. */
    CAST(NULL AS Nullable(UInt64))                     AS unique_cards,
    CAST(NULL AS Nullable(UInt32))                     AS p95_auth_latency_ms,
    round(auth_latency_ms_sum / txns, 2)               AS avg_auth_latency_ms,
    round(approved / txns, 6)                          AS approval_rate,
    round(declined / txns, 6)                          AS decline_rate,
    round(fraud_flagged / txns, 6)                     AS fraud_rate,
    round(chargebacks / txns, 6)                       AS chargeback_rate,
    source_max_updated_at
/* Aggregated in a subquery so the derived rates below read plain columns. Naming
   an aggregate after the column it sums would make a later reference to that name
   resolve to the alias, nesting one aggregate inside another. */
FROM (
    SELECT
        toDate(txn_minute)                         AS txn_date,
        merchant_category,
        card_network,
        country,
        toUInt64(sum(txns))                        AS txns,
        toUInt64(sum(approved))                    AS approved,
        toUInt64(sum(declined))                    AS declined,
        toUInt64(sum(refunded))                    AS refunded,
        toUInt64(sum(chargebacks))                 AS chargebacks,
        toUInt64(sum(fraud_flagged))               AS fraud_flagged,
        toUInt64(sum(declined_insufficient_funds)) AS declined_insufficient_funds,
        toUInt64(sum(declined_do_not_honor))       AS declined_do_not_honor,
        toUInt64(sum(declined_fraud_suspected))    AS declined_fraud_suspected,
        toUInt64(sum(declined_expired_card))       AS declined_expired_card,
        toInt64(sum(approved_volume_cents))        AS approved_volume_cents,
        toInt64(sum(refunded_volume_cents))        AS refunded_volume_cents,
        toInt64(sum(chargeback_volume_cents))      AS chargeback_volume_cents,
        toInt64(sum(auth_latency_ms_sum))          AS auth_latency_ms_sum,
        max(source_max_updated_at)                 AS source_max_updated_at
    FROM bruin_payments.rollup_txn_1m
    WHERE toDate(txn_minute) = toDate(now('UTC'))
    GROUP BY
        txn_date,
        merchant_category,
        card_network,
        country
)

UNION ALL

/* Sealed history, straight off the daily KPI table. */
SELECT
    txn_date,
    toUInt8(0)                                      AS is_today,
    merchant_category,
    card_network,
    country,
    txns,
    approved,
    declined,
    refunded,
    chargebacks,
    fraud_flagged,
    declined_insufficient_funds,
    declined_do_not_honor,
    declined_fraud_suspected,
    declined_expired_card,
    approved_volume,
    refunded_volume,
    chargeback_volume,
    CAST(unique_cards AS Nullable(UInt64))          AS unique_cards,
    CAST(p95_auth_latency_ms AS Nullable(UInt32))   AS p95_auth_latency_ms,
    avg_auth_latency_ms,
    approval_rate,
    decline_rate,
    fraud_rate,
    chargeback_rate,
    source_max_updated_at
FROM bruin_payments.kpi_txn_daily
WHERE txn_date < toDate(now('UTC'))
