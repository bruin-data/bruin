/* @bruin

name: bruin_payments.stg_transaction_changes
type: clickhouse.sql
description: Typed projection of the raw change log. Ingestion infers every column as Nullable, which cannot be used in a ClickHouse sorting key, so conformance to non-null dimension types and LowCardinality encoding happens once here instead of being repeated in each rollup. This is a logical view, so a created_at predicate from a consumer still reaches the underlying table and no data is copied.
owner: risk-platform@example.com

tags:
  - layer:staging
  - domain:payments
domains:
  - payments
meta:
  data_classification: confidential
  grain: one row per captured version of a transaction, unchanged from the raw change log
  purpose: null-conformance and dimension encoding only; no filtering, deduplication or aggregation
  encoding: dimensions cast to LowCardinality(String) so downstream sorting keys are valid
  null_policy: rows without an authorization or version timestamp are dropped; a missing dimension becomes 'unknown'
  read_pattern: consumers bound created_at themselves, then collapse versions with argMax on updated_at

materialization:
  type: view

depends:
  - bruin_payments.raw_transaction_changes

columns:
  - name: transaction_id
    type: Int64
    description: Authorization identifier. Repeats across versions.
  - name: created_at
    type: DateTime64(6, 'UTC')
    description: Authorization time; the event time for every rollup.
  - name: updated_at
    type: DateTime64(6, 'UTC')
    description: Source version cursor, used to pick the latest version.
  - name: merchant_id
    type: Int64
    description: Merchant that submitted the authorization.
  - name: card_id
    type: Int64
    description: Tokenized card reference.
  - name: amount_cents
    type: Int64
    description: Authorization amount in USD minor units.
  - name: status
    type: LowCardinality(String)
    description: Authorization state as of this version.
  - name: decline_reason
    type: LowCardinality(Nullable(String))
    description: Issuer decline reason; genuinely null unless declined, so it stays nullable.
  - name: card_network
    type: LowCardinality(String)
    description: Card scheme dimension.
  - name: merchant_category
    type: LowCardinality(String)
    description: Merchant category dimension.
  - name: country
    type: LowCardinality(String)
    description: ISO-2 country dimension.
  - name: is_fraud
    type: Bool
    description: Fraud label from the source.
  - name: auth_latency_ms
    type: Int64
    description: Authorization latency in milliseconds.

@bruin */

SELECT
    transaction_id,
    assumeNotNull(created_at)                             AS created_at,
    assumeNotNull(updated_at)                             AS updated_at,
    toInt64(ifNull(merchant_id, 0))                       AS merchant_id,
    toInt64(ifNull(card_id, 0))                           AS card_id,
    toInt64(ifNull(amount_cents, 0))                      AS amount_cents,
    toLowCardinality(ifNull(status, 'unknown'))           AS status,
    toLowCardinality(decline_reason)                      AS decline_reason,
    toLowCardinality(ifNull(card_network, 'unknown'))     AS card_network,
    toLowCardinality(ifNull(merchant_category, 'unknown')) AS merchant_category,
    toLowCardinality(ifNull(country, 'unknown'))          AS country,
    ifNull(is_fraud, false)                               AS is_fraud,
    toInt64(ifNull(auth_latency_ms, 0))                   AS auth_latency_ms
FROM bruin_payments.raw_transaction_changes
WHERE created_at IS NOT NULL
  AND updated_at IS NOT NULL
