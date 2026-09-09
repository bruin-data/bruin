# Mode 2: making unique cards and P95 latency additive

Everything in this pipeline is Mode 1: Bruin orchestrates incremental rollups, and every
stored measure is additive. This page documents Mode 2 — native ClickHouse materialized
views over an `AggregatingMergeTree` — which is what would make the two non-additive KPIs
behave like the rest. It is not built here, and the last section explains why.

## The problem, with real numbers from this pipeline

After 29 minutes of demo traffic (1,045 authorizations across 190 dimension groups):

| Measure | Naive rollup of the per-group values | Truth, from the change log | Error |
|---|---|---|---|
| Unique active cards | `sum(unique_cards)` = 1,043 | 932 | **+11.9%** |
| P95 auth latency | `max(p95_auth_latency_ms)` = 397 ms | 121 ms | **+228%** |

Reproduce it:

```sql
SELECT
    (SELECT sum(unique_cards) FROM bruin_payments.kpi_txn_daily)            AS naive_sum,
    (SELECT uniqExact(card_id) FROM bruin_payments.stg_transaction_changes) AS truth,
    (SELECT max(p95_auth_latency_ms) FROM bruin_payments.kpi_txn_daily)     AS naive_p95,
    (SELECT toUInt32(quantileExact(0.95)(auth_latency_ms))
     FROM bruin_payments.stg_transaction_changes)                           AS true_p95
```

A card used at both a grocer and an airline is counted twice by the sum. A quantile is
worse: no arithmetic over per-group quantiles recovers the overall quantile, and taking
the maximum reports the worst small group rather than the 95th percentile of anything.

Mode 1 handles this by refusing to fake it. `kpi_txn_daily` sums the additive measures
out of `rollup_txn_1m` and **re-reads the change log** for the two that are not additive.
`serving_realtime_risk` returns `NULL` for both on the current day, because the minute
rollup genuinely does not contain the information. The dashboard's P95 tile pays the
cost explicitly: it re-derives the quantile from `stg_transaction_changes` on every
render.

## Why re-reading is the expensive path

The additive path reads 837 pre-aggregated rows. The non-additive path reads every
version of every transaction for the dates in scope. That ratio only gets worse: rollup
rows grow with (minutes × dimension combinations), which plateaus, while change-log rows
grow with traffic forever. At production volume, a dashboard tile that re-derives a
quantile from raw on each render is not viable.

## What Mode 2 would do

The insight is that these measures are not additive as *values* but they are mergeable as
*aggregate state*. ClickHouse's `-State` combinators store that intermediate state —
`uniqState` keeps a HyperLogLog sketch, `quantileState` keeps a reservoir — and `-Merge`
combines them correctly across any grouping.

```sql
-- 1. The state-bearing target, at minute grain.
CREATE TABLE bruin_payments.rollup_txn_1m_state
(
    txn_minute        DateTime64(3, 'UTC'),
    merchant_category LowCardinality(String),
    card_network      LowCardinality(String),
    country           LowCardinality(String),
    txns              SimpleAggregateFunction(sum, UInt64),
    approved          SimpleAggregateFunction(sum, UInt64),
    approved_volume_cents SimpleAggregateFunction(sum, Int64),
    -- the two that Mode 1 cannot pre-aggregate
    unique_cards_state    AggregateFunction(uniq, Int64),
    auth_latency_state    AggregateFunction(quantile(0.95), Int64)
)
ENGINE = AggregatingMergeTree
ORDER BY (txn_minute, merchant_category, card_network, country);

-- 2. An incremental materialized view: a trigger on inserts into the change log.
CREATE MATERIALIZED VIEW bruin_payments.rollup_txn_1m_state_mv
TO bruin_payments.rollup_txn_1m_state
AS
SELECT
    toDateTime64(toStartOfMinute(created_at), 3, 'UTC') AS txn_minute,
    merchant_category,
    card_network,
    country,
    count()                                AS txns,
    countIf(status = 'approved')            AS approved,
    sumIf(amount_cents, status='approved')  AS approved_volume_cents,
    uniqState(card_id)                      AS unique_cards_state,
    quantileState(0.95)(auth_latency_ms)    AS auth_latency_state
FROM bruin_payments.stg_transaction_changes
GROUP BY txn_minute, merchant_category, card_network, country;

-- 3. Read at ANY grain, correctly, with no re-read of the change log.
SELECT
    toStartOfDay(txn_minute)                    AS day,
    sum(txns)                                   AS txns,
    uniqMerge(unique_cards_state)               AS unique_cards,     -- exact-ish, mergeable
    quantileMerge(0.95)(auth_latency_state)     AS p95_auth_latency  -- correct at day grain
FROM bruin_payments.rollup_txn_1m_state
GROUP BY day;
```

`uniqMerge` and `quantileMerge` are the whole point: the day-grain P95 is computed from
merged minute-level reservoirs, never from the source rows, so it stays correct while
costing the same as the additive path.

The trade-offs Mode 2 brings with it:

- **Approximation.** `uniq` is a HyperLogLog estimate (~0.8% typical error). `uniqExact`
  has a mergeable state too, at proportionally larger storage. `quantile` is a sampled
  reservoir; `quantileTDigest` gives tighter tails per byte.
- **Cascade rules.** Chaining materialized views means each level must consume the level
  below with `-MergeState`, not `-Merge`. See ClickHouse's
  [cascading materialized views](https://clickhouse.com/docs/guides/developer/cascading-materialized-views).
- **Restatements are the hard part.** A materialized view is an insert trigger. It sees
  the new `chargeback` version and adds it, but it cannot retract the `approved` version
  it already folded into that minute's state, because that state is a sketch with no
  per-row identity. The append-only change log this pipeline uses would double-count
  every restated transaction. Making Mode 2 correct here needs either a
  `CollapsingMergeTree` carrying sign-corrected rows, or a source that emits explicit
  before/after images — which is exactly the log-based CDC that
  [is unavailable for ClickHouse](ingestion-boundaries.md).
- **No backfill.** A materialized view only fires on inserts made after it exists;
  history has to be populated by a separate `INSERT INTO ... SELECT`.

That last point deserves emphasis: Mode 2 is not simply a better Mode 1. It buys additive
uniques and quantiles, and it pays with a much harder correctness story under
restatements — which is the defining feature of this workload.

## Why it is not built here

Bruin materializes tables (`create+replace`, `append`, `delete+insert`, `time_interval`,
`truncate+insert`, `merge`, `ddl`) and logical views (`CREATE OR REPLACE VIEW`). Its
`parameters.engine` covers `merge_tree`, `replacing_merge_tree`, `shared_merge_tree` and
`replicated_merge_tree`.

It does not expose, as first-class strategies:

- `CREATE MATERIALIZED VIEW` — a trigger-driven object with no Bruin-managed refresh, so
  it does not fit the "asset Bruin runs on a schedule" model;
- `AggregatingMergeTree` / `SummingMergeTree` with `AggregateFunction` state columns.

### The escape hatch, if you want it

Bruin's asset-level `hooks` run arbitrary SQL around a SQL asset's main query, which is
enough to create and maintain both objects:

```yaml
hooks:
  pre:
    - query: |
        CREATE TABLE IF NOT EXISTS bruin_payments.rollup_txn_1m_state (...)
        ENGINE = AggregatingMergeTree
        ORDER BY (txn_minute, merchant_category, card_network, country)
    - query: |
        CREATE MATERIALIZED VIEW IF NOT EXISTS bruin_payments.rollup_txn_1m_state_mv
        TO bruin_payments.rollup_txn_1m_state AS SELECT ...
```

This works, and it is worth knowing about. It is not used here because the objects it
creates are invisible to Bruin: they carry no column metadata, no lineage, no quality
checks and no schedule, so the governance this repository is meant to demonstrate would
stop at the hook boundary. A showcase that hid its most interesting table inside a
string literal would be making the wrong point.

## Feature request

First-class `parameters.engine: aggregating_merge_tree` with `AggregateFunction` column
types, plus a `materialization.type: materialized_view` strategy, would let Bruin own
Mode 2 with full lineage, column documentation and checks — and would let a pipeline like
this one serve additive uniques and quantiles from a single pre-aggregate instead of
re-reading raw or returning `NULL`.

## References

- [Incremental materialized views](https://clickhouse.com/docs/materialized-view/incremental-materialized-view)
- [Cascading materialized views](https://clickhouse.com/docs/guides/developer/cascading-materialized-views)
- [AggregatingMergeTree](https://clickhouse.com/docs/engines/table-engines/mergetree-family/aggregatingmergetree)
- [Aggregate function combinators](https://clickhouse.com/docs/sql-reference/aggregate-functions/combinators)
