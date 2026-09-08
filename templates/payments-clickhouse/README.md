# Payments ClickHouse

A self-contained demo template. Near-real-time payments authorization and fraud
monitoring, built around the pattern ClickHouse is best at: an operational PostgreSQL
database is the system of record, its changes are captured into ClickHouse, and Bruin
folds them into pre-aggregated serving tables that a Dashboard-as-Code dashboard reads.
The pipeline is scheduled every minute, so the dashboard trails the source by about a
minute.

It runs end to end from a fresh `bruin init` against two Docker containers. No cloud
account, no credentials to fill in. The template uses the `bruin_payments` ClickHouse
database and the connection names `clickhouse-default` and `postgres-default`; rename them
consistently if they do not fit your project. This README assumes the generated folder is
named `payments-clickhouse` (the `bruin init` default); adjust the paths if you named it
something else.

| | |
|---|---|
| Source | PostgreSQL `payments.transactions` (operational OLTP) |
| Destination | ClickHouse `bruin_payments` |
| Cadence | every minute (`schedule: "* * * * *"`) |
| Currency | single-currency USD; money carried as integer minor units |
| Timezone | UTC throughout |
| Fraud labels | supplied by the source and monitored, never modelled |

---

## Quick start

One command. It starts the containers, waits for them, replays 30 minutes of traffic
through the pipeline, and opens the dashboard.

```bash
./payments-clickhouse/demo.sh
```

`bruin init` does not set the executable bit, so if that reports "permission denied", run
`chmod +x payments-clickhouse/demo.sh` first, or call it as `bash payments-clickhouse/demo.sh`.

About three minutes, most of it the replay. Pass a different number of minutes as an
argument: `./payments-clickhouse/demo.sh up 60`.

Before the dashboard opens it prints what you are about to look at:

```text
==> What the dashboard will show
┌────────────────────────────────────────────┬───────────────────────────────┬────────────────┬───────────────┬─────────────────────┬───────────────────────────────┐
│ SOURCE                                     │ FROM_DATE                     │ AUTHORIZATIONS │ APPROVAL_RATE │ APPROVED_VOLUME_USD │ UNIQUE_CARDS                  │
├────────────────────────────────────────────┼───────────────────────────────┼────────────────┼───────────────┼─────────────────────┼───────────────────────────────┤
│ earlier (sealed, from the daily KPI table) │ 2026-08-24 00:00:00 +0000 UTC │ 442            │ 0.7873        │ 57003.31            │ available                     │
│ today (live, from the minute rollup)       │ 2026-08-25 00:00:00 +0000 UTC │ 802            │ 0.7855        │ 105294.98           │ not additive - null by design │
└────────────────────────────────────────────┴───────────────────────────────┴────────────────┴───────────────┴─────────────────────┴───────────────────────────────┘
```

Those two rows are the whole architecture in miniature: today is served live from the
per-minute rollup, earlier days from the sealed daily table, and the non-additive KPIs
exist only on the sealed side. Exact numbers vary with the traffic you generate.

The dashboard comes up on <http://localhost:8321>. If that port is busy, DAC moves to the
next free one, so check the URL it prints.

### The other subcommands

| Command | What it does |
|---|---|
| `demo.sh` or `demo.sh up [minutes]` | Everything: start containers, replay, serve |
| `demo.sh replay [minutes]` | Replay only — containers already running, no dashboard |
| `demo.sh serve` | Dashboard only |
| `demo.sh status` | What is running, and what is in the warehouse |
| `demo.sh down` | Stop and delete the containers and their data |
| `demo.sh --help` | The same list |

It needs `docker`, `bruin` and `python3` on `PATH`, plus `dac`
([bruin-data/dac](https://github.com/bruin-data/dac)) for the dashboard. The full run and
`serve` check for `dac` up front; `replay`, `status` and `down` do not need it.

---

## Running it by hand

`demo.sh replay` is a loop around one command. To drive it yourself:

```bash
CFG=payments-clickhouse/docker/bruin-local.yml

bruin run payments-clickhouse/pipeline.yml \
  --config-file $CFG \
  --apply-interval-modifiers \
  --start-date "2026-08-24 11:00:00" \
  --end-date   "2026-08-24 11:00:59.999999"
```

Four things matter:

1. **The first run of a fresh warehouse needs `--full-refresh`.** `rollup_txn_1m` uses the
   `time_interval` strategy, whose delete statement runs before the table exists.
2. **Windows must be whole UTC minutes**, matching the schedule. A window that starts
   mid-minute would leave a partial duplicate of the boundary minute — there is a blocking
   check for exactly that.
3. **Run windows in chronological order.** Restatements reference the three preceding
   windows, so replaying out of order turns them into late-arriving inserts instead of
   updates.
4. **`--apply-interval-modifiers` is not optional.** Without it Bruin uses the interval as
   given and the `interval_modifiers` on the rollup and KPI assets are ignored
   **silently** — the run still succeeds, and late restatements are simply never folded
   in. On a schedule, Bruin Cloud computes the interval and applies the modifiers itself,
   so a scheduled run needs no flag.

All commands pass `--config-file payments-clickhouse/docker/bruin-local.yml`, a config
bundled with the template that points at the containers. Its credentials are local-only
demo values, not secrets. To run against ClickHouse Cloud instead, fill in the
`.bruin.yml` that `bruin init` wrote for you (its `clickhouse-default` and
`postgres-default` placeholders) and drop the `--config-file` flag.

> The Bruin CLI writes a `.gitignore` next to any config file it loads, so a
> `docker/.gitignore` listing `bruin-local.yml` appears after the first command. Keep
> `docker/bruin-local.yml` tracked deliberately; `git add -f payments-clickhouse/docker/bruin-local.yml`
> re-adds it if it ever gets ignored.

### Other useful commands

```bash
CFG=payments-clickhouse/docker/bruin-local.yml

bruin validate payments-clickhouse --config-file $CFG
bruin run payments-clickhouse/pipeline.yml --config-file $CFG --only checks
bruin lineage payments-clickhouse/assets/serving/serving_realtime_risk.sql --full

# validate the dashboard, and execute every widget query without a browser
dac validate --dir payments-clickhouse --config $CFG
dac check    --dir payments-clickhouse --config $CFG
```

---

## Architecture

```text
PostgreSQL · payments.transactions          ← system of record
     ▲                                        (Python seed asset writes demo traffic here,
     │                                         including approved → refunded → chargeback
     │                                         restatements as real UPDATEs)
     │  ingestr, cursor on updated_at, append-only
     ▼
bruin_payments.raw_transaction_changes      ← append-only change log, MergeTree
     │                                        one row per captured version
     │  view: null-conformance + LowCardinality encoding
     ▼
bruin_payments.stg_transaction_changes
     │  argMax(…, updated_at) GROUP BY transaction_id, then bucket on created_at
     │  time_interval + 15m lookback
     ▼
bruin_payments.rollup_txn_1m                ← per-minute pre-aggregate, all measures additive
     │                        ╲
     │  merge, 3h lookback     ╲  merge, 3h lookback
     ▼                          ▼
rollup_txn_1h                  kpi_txn_daily  ← additive measures summed from the minute grain;
                                │                uniques and P95 re-derived from the change log
     ┌──────────────────────────┘
     ▼
bruin_payments.serving_realtime_risk        ← view: today live ∪ sealed history + derived rates
     │
     ▼
dashboards/payments_risk.yml + semantic/payments_risk.yml  →  dac serve
```

| Asset | Type | Strategy | What it is for |
|---|---|---|---|
| `payments.transactions` | Python | `merge` into PostgreSQL | Generates demo traffic and restatements into the system of record |
| `bruin_payments.raw_transaction_changes` | ingestr | `append` | Captures changed rows into an append-only change log |
| `bruin_payments.stg_transaction_changes` | SQL view | — | Non-null, `LowCardinality`-encoded projection of the change log |
| `bruin_payments.rollup_txn_1m` | SQL | `time_interval` + 15m lookback | Per-minute pre-aggregate; the live dashboard's throughput source |
| `bruin_payments.rollup_txn_1h` | SQL | `merge` + 3h lookback | Coarser grain, summed from the minute grain |
| `bruin_payments.kpi_txn_daily` | SQL | `merge` + 3h lookback | Daily KPIs; where additivity stops being free |
| `bruin_payments.serving_realtime_risk` | SQL view | — | One dashboard-ready object over all history |

### How it compares to the other ClickHouse templates

| | `clickhouse` | `shopify-clickhouse` | **this template** |
|---|---|---|---|
| Source | Postgres + seeds | Shopify API | **PostgreSQL OLTP, change capture** |
| Cadence | daily | daily | **every minute** |
| Pattern | feature tour | medallion warehouse | **change log → rollup cascade → serving → dashboard** |
| Consumer | learners | analysts | **operations and risk, live** |
| Vertical | generic | commerce | **fintech payments** |

---

## Tuning the demo traffic

`pipeline.yml` exposes four variables, overridable with `--var`:

| Variable | Default | Effect |
|---|---|---|
| `txns_per_minute` | 40 | Authorizations generated per minute of the run window |
| `restatement_rate` | 0.06 | Share of earlier authorizations that get restated |
| `restatement_windows` | 3 | How many windows back restatements reach |
| `max_seed_rows` | 20000 | Hard cap per run, so a wide backfill window stays quick |

```bash
bruin run payments-clickhouse/pipeline.yml \
  --config-file payments-clickhouse/docker/bruin-local.yml \
  --apply-interval-modifiers --var txns_per_minute=200 \
  --start-date "2026-08-24 11:00:00" --end-date "2026-08-24 11:00:59.999999"
```

Every generated row is a pure function of `(window_start_epoch, row_index)`, so the seed
is idempotent: rerunning a window reproduces byte-identical transactions rather than
inventing new ones.

---

## The two ideas worth taking away

### 1. Late-arriving restatements, and what a lookback window buys

A payment is not immutable. An authorization is approved, then refunded, then charged
back. Each transition bumps `updated_at` in PostgreSQL, so capture picks it up in a
*later* run than the one that first recorded the transaction — but the metric has to
change in the minute the authorization *happened*, not the minute the chargeback arrived.

That is why every rollup buckets on `created_at`, never on `updated_at`, and why
`rollup_txn_1m` declares:

```yaml
interval_modifiers:
  start: -15m
```

Each run reprocesses the previous 15 minutes, so a restatement rewrites the minute it
belongs to. To watch it happen, find a transaction that went the whole way:

```bash
bruin query --config-file payments-clickhouse/docker/bruin-local.yml \
  --connection clickhouse-default --query "
WITH lifecycle AS (
    SELECT transaction_id, arrayStringConcat(groupArray(status), ' -> ') AS states
    FROM (SELECT DISTINCT transaction_id, status, updated_at
          FROM bruin_payments.stg_transaction_changes
          ORDER BY transaction_id, updated_at)
    GROUP BY transaction_id
    HAVING count() = 3
    LIMIT 1
),
authorization AS (
    SELECT DISTINCT c.transaction_id, c.created_at, c.merchant_category, c.card_network, c.country
    FROM bruin_payments.stg_transaction_changes AS c
    INNER JOIN lifecycle AS l ON c.transaction_id = l.transaction_id
    LIMIT 1
)
SELECT a.transaction_id, l.states, a.created_at AS authorized_at,
       r.txn_minute, r.txns, r.approved, r.refunded, r.chargebacks
FROM authorization AS a
CROSS JOIN lifecycle AS l
INNER JOIN bruin_payments.rollup_txn_1m AS r
    ON r.txn_minute = toDateTime64(toStartOfMinute(a.created_at), 3, 'UTC')
   AND r.merchant_category = a.merchant_category
   AND r.card_network = a.card_network
   AND r.country = a.country"
```

In one verified run this returned:

```text
┌──────────────────┬────────────────────────────────────┬───────────────────────────────────┬───────────────────────────────┬──────┬──────────┬──────────┬─────────────┐
│ A.TRANSACTION_ID │ STATES                             │ AUTHORIZED_AT                     │ TXN_MINUTE                    │ TXNS │ APPROVED │ REFUNDED │ CHARGEBACKS │
├──────────────────┼────────────────────────────────────┼───────────────────────────────────┼───────────────────────────────┼──────┼──────────┼──────────┼─────────────┤
│ 178756968000009  │ approved -> refunded -> chargeback │ 2026-08-24 11:08:20.411 +0000 UTC │ 2026-08-24 11:08:00 +0000 UTC │ 1    │ 0        │ 0        │ 1           │
└──────────────────┴────────────────────────────────────┴───────────────────────────────────┴───────────────────────────────┴──────┴──────────┴──────────┴─────────────┘
```

Minute `11:08` was written to the rollup while that payment was still `approved`. Two runs
later the chargeback arrived, the lookback reprocessed the minute, and it now reads
`approved = 0, chargebacks = 1`. The sealed minute was corrected in place, and the
chargeback is attributed to the minute the payment was authorized rather than the minute
the chargeback landed.

The lookback is the entire correctness budget, and it is a real trade-off. 15 minutes at
minute grain means a restatement arriving 20 minutes late is **never** reflected. Widen
`interval_modifiers.start` to buy a longer correction window and pay for it in recompute
on every single run.

### 2. Additive and non-additive measures are not the same kind of thing

`rollup_txn_1m` stores only measures that survive being summed: counts, volumes, and a
latency *sum*. `rollup_txn_1h` and the additive half of `kpi_txn_daily` are therefore
plain `sum()` queries over the minute grain — which is the whole reason a rollup cascade
is cheap.

Unique cards and P95 latency do not survive it. Compare a naive rollup against the truth
for the sealed day:

```bash
bruin query --config-file payments-clickhouse/docker/bruin-local.yml \
  --connection clickhouse-default --query "
SELECT
    (SELECT sum(unique_cards) FROM bruin_payments.kpi_txn_daily
     WHERE txn_date = toDate(now('UTC')) - 1)                       AS naive_sum_of_groups,
    (SELECT uniqExact(card_id) FROM bruin_payments.stg_transaction_changes
     WHERE toDate(created_at) = toDate(now('UTC')) - 1)             AS true_unique_cards,
    (SELECT max(p95_auth_latency_ms) FROM bruin_payments.kpi_txn_daily
     WHERE txn_date = toDate(now('UTC')) - 1)                       AS naive_max_of_group_p95,
    (SELECT toUInt32(quantileExact(0.95)(auth_latency_ms))
     FROM bruin_payments.stg_transaction_changes
     WHERE toDate(created_at) = toDate(now('UTC')) - 1)             AS true_p95"
```

One run of the 10-minute history block returned:

```text
┌─────────────────────┬───────────────────┬────────────────────────┬──────────┐
│ NAIVE_SUM_OF_GROUPS │ TRUE_UNIQUE_CARDS │ NAIVE_MAX_OF_GROUP_P95 │ TRUE_P95 │
├─────────────────────┼───────────────────┼────────────────────────┼──────────┤
│ 447                 │ 427               │ 355                    │ 131      │
└─────────────────────┴───────────────────┴────────────────────────┴──────────┘
```

Cards overstated by 4.7%, P95 by 171%. The gap widens with the number of dimension groups,
because a card used in two groups is counted twice, and a maximum-of-quantiles reports the
worst small group rather than the 95th percentile of anything.

So `kpi_txn_daily` re-reads the change log for exactly those two columns, and
`serving_realtime_risk` returns `NULL` for both on the current day rather than a plausible
wrong number. The dashboard's P95 tile pays the cost in the open, re-deriving the quantile
on every render.

Making them additive requires storing aggregate *state* rather than values —
`uniqState`, `quantileState` on an `AggregatingMergeTree` — which Bruin does not expose as
a first-class strategy today, and which brings a genuinely harder correctness problem
under restatements. That analysis is in
[docs/mode2-aggregating-mergetree.md](docs/mode2-aggregating-mergetree.md).

---

## Reading the change log correctly

`raw_transaction_changes` is append-only and holds one row per captured version, so
`transaction_id` is **not** unique in it. Always collapse before aggregating:

```sql
SELECT transaction_id, argMax(status, updated_at) AS status
FROM bruin_payments.stg_transaction_changes
WHERE created_at >= now('UTC') - INTERVAL 1 HOUR   -- always bound created_at
GROUP BY transaction_id
```

Bound `created_at`, not `updated_at`: all versions of a transaction share one
`created_at`, so a `created_at` bound keeps every version of the transactions in scope,
whereas an `updated_at` bound would slice a transaction's history in half.

`FINAL` is not an option here. The table is a `MergeTree`, not a `ReplacingMergeTree`,
deliberately — ingestr derives the sorting key from the primary key alone, so a
`ReplacingMergeTree` would deduplicate on `transaction_id` and silently discard genuine
earlier versions.

Replaying a completed interval appends rows the table already holds. Results stay correct
because every consumer collapses by `transaction_id`, but it costs storage:

```sql
SELECT count() - uniqExact((transaction_id, updated_at)) AS duplicate_rows
FROM bruin_payments.raw_transaction_changes
```

A `--full-refresh` over the full history clears them.

---

## Governance

Every asset declares an owner, layer and domain tags, grain and freshness metadata, and a
description for every column. 81 checks run across the pipeline. Blocking ones cover:

- primary-key nullability and uniqueness on both merge-keyed tables;
- each captured version being internally self-consistent;
- no future minutes, hours, dates or authorizations;
- restatements never predating the authorization they restate;
- status counts partitioning the transaction count, and decline reasons partitioning the
  decline count;
- reconciliation between the change log, the minute grain, the hourly grain and the daily
  grain;
- distinct counts never exceeding the transactions they were computed from;
- every derived rate within `[0, 1]`, and every monetary measure non-negative;
- one row per minute and dimension set, which catches a run interval that did not start on
  a minute boundary.

```bash
bruin run payments-clickhouse/pipeline.yml \
  --config-file payments-clickhouse/docker/bruin-local.yml --only checks
```

Verify the grains agree:

```bash
bruin query --config-file payments-clickhouse/docker/bruin-local.yml \
  --connection clickhouse-default --query "
SELECT 'changelog' AS grain, uniqExact(transaction_id) AS n FROM bruin_payments.stg_transaction_changes
UNION ALL SELECT 'minute',  sum(txns) FROM bruin_payments.rollup_txn_1m
UNION ALL SELECT 'hour',    sum(txns) FROM bruin_payments.rollup_txn_1h
UNION ALL SELECT 'day',     sum(txns) FROM bruin_payments.kpi_txn_daily
UNION ALL SELECT 'serving', sum(txns) FROM bruin_payments.serving_realtime_risk"
```

All five numbers should be identical.

---

## Deviations from the original requirements

Four things in the requirements document turned out not to be buildable as specified. Each
was verified against the running toolchain rather than assumed;
[docs/ingestion-boundaries.md](docs/ingestion-boundaries.md) has the reproductions.

| Specified | Built | Why |
|---|---|---|
| Log-based CDC (`postgres+cdc://`, batch mode) | Cursor-based capture on `updated_at` | ingestr refuses ClickHouse as a managed-CDC destination: *"destination scheme "clickhouse" cannot safely run managed CDC"*. The same source works into DuckDB. Hard `DELETE`s are consequently not captured. |
| `merge` into a `ReplacingMergeTree`, deduplicated on `transaction_id` | `append` into a `MergeTree`, collapsed with `argMax` at read time | ingestr's ClickHouse merge reported 927 rows loaded and persisted 1. Read-time collapse is also the canonical ClickHouse pattern for mutable sources. |
| `amount numeric(18,2)` | `amount_cents bigint` | Schema evolution fails on every incremental run after the first: *"decimal widening requires precision 40"*. Integer minor units are also how card processors actually store money, so every rollup sum is exact. |
| `--exclude-tag requires-postgres-cdc` runs fully offline | The whole pipeline needs PostgreSQL; Docker makes it local | The seed writes to PostgreSQL as the system of record, so change capture is the only path into ClickHouse. "Offline" here means no cloud, not no database. Live-source assets are tagged `requires-postgres`; the generator is tagged `demo-seed`, so `--exclude-tag demo-seed` points the pipeline at a real source. |

One asset was added that the requirements did not call for:
`stg_transaction_changes`. Ingestion infers every column as `Nullable`, and ClickHouse
refuses a nullable sorting key, so the conformance has to happen somewhere. Doing it once
in a view beats repeating `ifNull` in every rollup.

A note for real-source mode (`--exclude-tag demo-seed`): the merge keys of
`rollup_txn_1h` and `kpi_txn_daily` include the three reporting dimensions. The demo seed
never changes a transaction's dimensions across restatements, so a version only ever
updates its own key and the grains always reconcile. Against a real source where a
correction can *move* a transaction to a different merchant category, network or country,
the recompute would insert the new key without deleting the old one, leaving a stale
aggregate. If your source can do that, widen the merge to delete the affected
hour/date partitions before reinserting, rather than merging on the dimensioned key.

There are no unit tests. `bruin unit-test` casts only the first mocked input row and emits
later rows as untyped string literals, so a multi-row fixture with timestamp columns —
exactly what testing the version-collapse needs — cannot be expressed. Shipping a failing
test seemed worse than saying so here.

## Known cosmetic quirks

- **Dips at the start of each replayed block.** The seed restates transactions from the
  three windows *before* the one it is running, so the first few minutes of a block contain
  only late arrivals — a handful of transactions, or none at all. On a real one-minute
  schedule this does not happen, because there is always a preceding window. The x-axis is
  a category of minute labels, so a minute with no rows closes up rather than leaving a
  hole, and the line dips to zero and recovers. `demo.sh` replays two disjoint blocks,
  so there are two such dips.

  To see it: the leading minutes carry 1 and 3 authorizations before the rate settles at
  `txns_per_minute`.

  ```sql
  SELECT txn_minute, sum(txns) AS txns
  FROM bruin_payments.rollup_txn_1m
  GROUP BY txn_minute ORDER BY txn_minute LIMIT 8
  ```
- **The P95 tile is a tall, mostly empty card.** A metric widget stretches to the height of
  the chart beside it.
